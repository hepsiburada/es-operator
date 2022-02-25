package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	zv1 "github.com/zalando-incubator/es-operator/pkg/apis/zalando.org/v1"
	"github.com/zalando-incubator/es-operator/pkg/clientset"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	kube_record "k8s.io/client-go/tools/record"
)

const (
	esDataSetLabelKey                       = "es-operator-dataset"
	esOperatorAnnotationKey                 = "es-operator.zalando.org/operator"
	esScalingOperationKey                   = "es-operator.zalando.org/current-scaling-operation"
	defaultElasticsearchDataSetEndpointPort = 9200
)

type ElasticsearchOperator struct {
	logger                *log.Entry
	kube                  *clientset.Clientset
	interval              time.Duration
	autoscalerInterval    time.Duration
	metricsInterval       time.Duration
	priorityNodeSelectors labels.Set
	operatorID            string
	namespace             string
	clusterDNSZone        string
	elasticsearchEndpoint *url.URL
	operating             map[types.UID]operatingEntry
	sync.Mutex
	recorder kube_record.EventRecorder
}

type operatingEntry struct {
	cancel context.CancelFunc
	doneCh <-chan struct{}
	logger *log.Entry
}

// NewElasticsearchOperator initializes a new ElasticsearchDataSet operator instance.
func NewElasticsearchOperator(
	client *clientset.Clientset,
	priorityNodeSelectors map[string]string,
	interval,
	autoscalerInterval time.Duration,
	operatorID,
	namespace,
	clusterDNSZone string,
	elasticsearchEndpoint *url.URL,
) *ElasticsearchOperator {
	return &ElasticsearchOperator{
		logger: log.WithFields(
			log.Fields{
				"operator": "elasticsearch",
			},
		),
		kube:                  client,
		interval:              interval,
		autoscalerInterval:    autoscalerInterval,
		metricsInterval:       60 * time.Second,
		priorityNodeSelectors: labels.Set(priorityNodeSelectors),
		operatorID:            operatorID,
		namespace:             namespace,
		clusterDNSZone:        clusterDNSZone,
		elasticsearchEndpoint: elasticsearchEndpoint,
		operating:             make(map[types.UID]operatingEntry),
		recorder:              createEventRecorder(client),
	}
}

// Run runs the main loop of the operator.
func (o *ElasticsearchOperator) Run(ctx context.Context) {
	go o.collectMetrics(ctx)
	go o.runAutoscaler(ctx)

	// run EDS watcher
	o.runWatch(ctx)
	<-ctx.Done()
	o.logger.Info("Terminating main operator loop.")
}

// collectMetrics collects metrics for all the managed EDS resources.
// The metrics are stored in the coresponding ElasticsearchMetricSet and used
// by the autoscaler for scaling EDS.
func (o *ElasticsearchOperator) collectMetrics(ctx context.Context) {
	nextCheck := time.Now().Add(-o.metricsInterval)

	for {
		o.logger.Debug("Collecting metrics")
		select {
		case <-time.After(time.Until(nextCheck)):
			nextCheck = time.Now().Add(o.metricsInterval)

			resources, err := o.collectResources(ctx)
			if err != nil {
				o.logger.Error(err)
				continue
			}

			for _, es := range resources {
				if es.ElasticsearchDataSet.Spec.Scaling != nil && es.ElasticsearchDataSet.Spec.Scaling.Enabled {
					metrics := &ElasticsearchMetricsCollector{
						kube:   o.kube,
						logger: log.WithFields(log.Fields{"collector": "metrics"}),
						es:     *es,
					}
					err := metrics.collectMetrics(ctx)
					if err != nil {
						o.logger.Error(err)
						continue
					}
				}
			}
		case <-ctx.Done():
			o.logger.Info("Terminating metrics collector loop.")
			return
		}
	}
}

// runAutoscaler runs the EDS autoscaler which checks at an interval if any
// EDS resources needs to be autoscaled. If autoscaling is needed this will be
// indicated on the EDS with the
// 'es-operator.zalando.org/current-scaling-operation' annotation. The
// annotation indicates the desired scaling which will be reconciled by the
// operator.
func (o *ElasticsearchOperator) runAutoscaler(ctx context.Context) {
	nextCheck := time.Now().Add(-o.autoscalerInterval)

	for {
		o.logger.Debug("Checking autoscaling")
		select {
		case <-time.After(time.Until(nextCheck)):
			nextCheck = time.Now().Add(o.autoscalerInterval)

			resources, err := o.collectResources(ctx)
			if err != nil {
				o.logger.Error(err)
				continue
			}

			for _, es := range resources {
				if es.ElasticsearchDataSet.Spec.Scaling != nil && es.ElasticsearchDataSet.Spec.Scaling.Enabled {
					endpoint := o.getElasticsearchEndpoint(es.ElasticsearchDataSet)

					client := &ESClient{
						Endpoint:             endpoint,
						excludeSystemIndices: es.ElasticsearchDataSet.Spec.ExcludeSystemIndices,
					}

					err := o.scaleEDS(ctx, es.ElasticsearchDataSet, es, client)
					if err != nil {
						o.logger.Error(err)
						continue
					}
				}
			}
		case <-ctx.Done():
			o.logger.Info("Terminating autoscaler loop.")
			return
		}
	}
}

// Run setups up a shared informer for listing and watching changes to pods and
// starts listening for events.
func (o *ElasticsearchOperator) runWatch(ctx context.Context) {
	informer := cache.NewSharedIndexInformer(
		cache.NewListWatchFromClient(
			o.kube.ZalandoV1().RESTClient(),
			"elasticsearchdatasets",
			o.namespace, fields.Everything(),
		),
		&zv1.ElasticsearchDataSet{},
		0, // skip resync
		cache.Indexers{},
	)

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    o.add,
		UpdateFunc: o.update,
		DeleteFunc: o.del,
	})

	go informer.Run(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		log.Errorf("Timed out waiting for caches to sync")
		return
	}

	log.Info("Synced ElasticsearchDataSet watcher")
}

// add is the handler for an EDS getting added.
func (o *ElasticsearchOperator) add(obj interface{}) {
	eds, ok := obj.(*zv1.ElasticsearchDataSet)
	if !ok {
		log.Errorf("Failed to get EDS object")
		return
	}

	err := o.operateEDS(eds, false)
	if err != nil {
		log.Errorf("Add failed, this is bad!: %v", err)
	}
}

// update is the handler for an EDS getting updated.
func (o *ElasticsearchOperator) update(oldObj, newObj interface{}) {
	newEDS, ok := newObj.(*zv1.ElasticsearchDataSet)
	if !ok {
		log.Errorf("Failed to get EDS object")
		return
	}

	err := o.operateEDS(newEDS, false)
	if err != nil {
		log.Errorf("Add failed, this is bad!: %v", err)
	}
}

// del is the handler for an EDS getting deleted.
func (o *ElasticsearchOperator) del(obj interface{}) {
	eds, ok := obj.(*zv1.ElasticsearchDataSet)
	if !ok {
		log.Errorf("Failed to get EDS object")
		return
	}

	err := o.operateEDS(eds, true)
	if err != nil {
		log.Errorf("Add failed, this is bad!: %v", err)
	}
}

// collectResources collects all the ElasticsearchDataSet resources and there
// corresponding StatefulSets if they exist.
func (o *ElasticsearchOperator) collectResources(ctx context.Context) (map[types.UID]*ESResource, error) {
	resources := make(map[types.UID]*ESResource)

	edss, err := o.kube.ZalandoV1().ElasticsearchDataSets(o.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	// create a map of ElasticsearchDataSet clusters to later map the matching
	// StatefulSet.
	for _, eds := range edss.Items {
		eds := eds
		if !o.hasOwnership(&eds) {
			continue
		}

		// set TypeMeta manually because of this bug:
		// https://github.com/kubernetes/client-go/issues/308
		eds.APIVersion = "zalando.org/v1"
		eds.Kind = "ElasticsearchDataSet"

		resources[eds.UID] = &ESResource{
			ElasticsearchDataSet: &eds,
		}
	}

	metricSets, err := o.kube.ZalandoV1().ElasticsearchMetricSets(o.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	// Map metricSets to the owning ElasticsearchDataSet resource.
	for _, ms := range metricSets.Items {
		ms := ms
		if uid, ok := getOwnerUID(ms.ObjectMeta); ok {
			if er, ok := resources[uid]; ok {
				er.MetricSet = &ms
			}
		}
	}

	// TODO: label filter
	pods, err := o.kube.CoreV1().Pods(o.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	for _, pod := range pods.Items {
		for _, es := range resources {
			es := es
			// TODO: leaky abstraction
			if v, ok := pod.Labels[esDataSetLabelKey]; ok && v == es.ElasticsearchDataSet.Name {
				es.Pods = append(es.Pods, pod)
				break
			}
		}
	}

	return resources, nil
}

func (o *ElasticsearchOperator) scaleEDS(ctx context.Context, eds *zv1.ElasticsearchDataSet, es *ESResource, client *ESClient) error {
	err := validateScalingSettings(eds.Spec.Scaling)
	if err != nil {
		o.recorder.Event(eds, v1.EventTypeWarning, "ScalingInvalid", fmt.Sprintf(
			"Scaling settings are invalid: %v", err),
		)
		return err
	}

	// first, try to find an existing annotation and return it
	scalingOperation, err := edsScalingOperation(eds)
	if err != nil {
		return err
	}

	// exit early if the scaling operation is already defined
	if scalingOperation != nil && scalingOperation.ScalingDirection != NONE {
		o.logger.Info(fmt.Sprintf("%s scaling operation continues", scalingOperation.ScalingDirection))
		return nil
	}

	// second, calculate a new EDS scaling operation
	scaling := eds.Spec.Scaling
	name := eds.Name
	namespace := eds.Namespace

	currentReplicas := edsReplicas(eds)
	eds.Spec.Replicas = &currentReplicas
	as := NewAutoScaler(es, o.metricsInterval, client)

	if scaling != nil && scaling.Enabled {
		var scalingOperation *ScalingOperation

		if len(scaling.MainIndexAlias) > 0 {
			scalingOperation, err = as.GetScalingOperationByIndexAliasNew(scaling.MainIndexAlias)
		} else {
			scalingOperation, err = as.GetScalingOperation()
		}

		if err != nil {
			return err
		}

		// update EDS definition.
		if scalingOperation.NodeReplicas != nil && *scalingOperation.NodeReplicas != currentReplicas {
			now := metav1.Now()
			if *scalingOperation.NodeReplicas > currentReplicas {
				eds.Status.LastScaleUpStarted = &now
			} else {
				eds.Status.LastScaleDownStarted = &now
			}
			log.Infof("Updating last scaling event in EDS '%s/%s'", namespace, name)

			// update status
			eds, err = o.kube.ZalandoV1().ElasticsearchDataSets(eds.Namespace).UpdateStatus(ctx, eds, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
			eds.Spec.Replicas = scalingOperation.NodeReplicas
		}

		// TODO: move to a function
		jsonBytes, err := json.Marshal(scalingOperation)
		if err != nil {
			return err
		}

		if scalingOperation.ScalingDirection != NONE {
			eds.Annotations[esScalingOperationKey] = string(jsonBytes)

			// persist changes of EDS
			log.Infof("Updating desired scaling for EDS '%s/%s'. New desired replicas: %d. %s", namespace, name, *eds.Spec.Replicas, scalingOperation.Description)
			_, err = o.kube.ZalandoV1().ElasticsearchDataSets(eds.Namespace).Update(ctx, eds, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
		}

	}

	return nil
}

func (o *ElasticsearchOperator) operateEDS(eds *zv1.ElasticsearchDataSet, deleted bool) error {
	// set TypeMeta manually because of this bug:
	// https://github.com/kubernetes/client-go/issues/308
	eds.APIVersion = "zalando.org/v1"
	eds.Kind = "ElasticsearchDataSet"

	// insert into operating
	o.Lock()
	defer o.Unlock()

	// restart if already being operated on.
	if entry, ok := o.operating[eds.UID]; ok {
		entry.cancel()
		// wait for previous operation to terminate
		entry.logger.Infof("Waiting for operation to stop")
		<-entry.doneCh
	}

	if deleted {
		return nil
	}

	if !o.hasOwnership(eds) {
		o.logger.Infof("Skipping EDS %s/%s, not owned by the operator", eds.Namespace, eds.Name)
		return nil
	}

	doneCh := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	logger := log.WithFields(log.Fields{
		"eds":       eds.Name,
		"namespace": eds.Namespace,
	})
	// add to operating table
	o.operating[eds.UID] = operatingEntry{
		cancel: cancel,
		doneCh: doneCh,
		logger: logger,
	}

	endpoint := o.getElasticsearchEndpoint(eds)

	// TODO: abstract this
	client := &ESClient{
		Endpoint: endpoint,
	}

	operator := &Operator{
		esClient:              client,
		kube:                  o.kube,
		priorityNodeSelectors: o.priorityNodeSelectors,
		interval:              o.interval,
		logger:                logger,
		recorder:              o.recorder,
	}

	rs := &EDSResource{
		eds:      eds,
		kube:     o.kube,
		esClient: client, // TODO: think about not setting this twice
		recorder: o.recorder,
	}

	go operator.Run(ctx, doneCh, rs)

	return nil
}

// hasOwnership returns true if the operator is the "owner" of the EDS.
// Whether it's owner is determined by the value of the
// 'es-operator.zalando.org/operator' annotation. If the value
// matches the operatorID then it owns it, or if the operatorID is
// "" and there's no annotation set.
func (o *ElasticsearchOperator) hasOwnership(eds *zv1.ElasticsearchDataSet) bool {
	if eds.Annotations != nil {
		if owner, ok := eds.Annotations[esOperatorAnnotationKey]; ok {
			return owner == o.operatorID
		}
	}
	return o.operatorID == ""
}

func (o *ElasticsearchOperator) getElasticsearchEndpoint(eds *zv1.ElasticsearchDataSet) *url.URL {
	if o.elasticsearchEndpoint != nil {
		return o.elasticsearchEndpoint
	}

	// TODO: discover port from EDS
	return &url.URL{
		Scheme: "http",
		Host: fmt.Sprintf(
			"%s.%s.svc.%s:%d",
			eds.Name,
			eds.Namespace,
			o.clusterDNSZone,
			defaultElasticsearchDataSetEndpointPort,
		),
	}
}

func getOwnerUID(objectMeta metav1.ObjectMeta) (types.UID, bool) {
	if len(objectMeta.OwnerReferences) == 1 {
		return objectMeta.OwnerReferences[0].UID, true
	}
	return "", false
}

// templateInjectLabels injects labels into a pod template spec.
func templateInjectLabels(template v1.PodTemplateSpec, labels map[string]string) v1.PodTemplateSpec {
	if template.ObjectMeta.Labels == nil {
		template.ObjectMeta.Labels = map[string]string{}
	}

	for key, value := range labels {
		if _, ok := template.ObjectMeta.Labels[key]; !ok {
			template.ObjectMeta.Labels[key] = value
		}
	}
	return template
}

// edsScalingOperation returns the scaling operation read from the
// scaling-operation annotation on the EDS. If no operation is defined it will
// return nil.
func edsScalingOperation(eds *zv1.ElasticsearchDataSet) (*ScalingOperation, error) {
	if op, ok := eds.Annotations[esScalingOperationKey]; ok {
		scalingOperation := &ScalingOperation{}
		err := json.Unmarshal([]byte(op), scalingOperation)
		if err != nil {
			return nil, err
		}
		return scalingOperation, nil
	}
	return nil, nil
}

// validateScalingSettings checks that the scaling settings are valid.
//
// * min values can not be > than the corresponding max values.
// * min can not be less than 0.
// * 'replicas', 'indexReplicas' and 'shardsPerNode' settings should not
//   conflict.
func validateScalingSettings(scaling *zv1.ElasticsearchDataSetScaling) error {
	// don't validate if scaling is not enabled
	if scaling == nil || !scaling.Enabled {
		return nil
	}

	// check that min is not greater than max values
	if scaling.MinReplicas > scaling.MaxReplicas {
		return fmt.Errorf(
			"minReplicas(%d) can't be greater than maxReplicas(%d)",
			scaling.MinReplicas,
			scaling.MaxReplicas,
		)
	}

	if scaling.MinIndexReplicas > scaling.MaxIndexReplicas {
		return fmt.Errorf(
			"minIndexReplicas(%d) can't be greater than maxIndexReplicas(%d)",
			scaling.MinIndexReplicas,
			scaling.MaxIndexReplicas,
		)
	}

	if scaling.MinShardsPerNode > scaling.MaxShardsPerNode {
		return fmt.Errorf(
			"minShardsPerNode(%d) can't be greater than maxShardsPerNode(%d)",
			scaling.MinShardsPerNode,
			scaling.MaxShardsPerNode,
		)
	}

	// ensure that relation between replicas and indexReplicas is valid
	if scaling.MinReplicas < (scaling.MinIndexReplicas + 1) {
		return fmt.Errorf(
			"minReplicas(%d) can not be less than minIndexReplicas(%d)+1",
			scaling.MinReplicas,
			scaling.MinIndexReplicas,
		)
	}

	if scaling.MaxReplicas < (scaling.MaxIndexReplicas + 1) {
		return fmt.Errorf(
			"maxReplicas(%d) can not be less than maxIndexReplicas(%d)+1",
			scaling.MaxReplicas,
			scaling.MaxIndexReplicas,
		)
	}

	return nil
}
