package operator

import (
	"context"
	"fmt"
	zv1 "github.com/zalando-incubator/es-operator/pkg/apis/zalando.org/v1"
	"github.com/zalando-incubator/es-operator/pkg/clientset"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	pv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	kube_record "k8s.io/client-go/tools/record"
	"math"
)

type StatefulResource interface {
	// Name returns the name of the resource.
	Name() string
	// Namespace returns the namespace where the resource is located.
	Namespace() string
	// APIVersion returns the APIVersion of the resource.
	APIVersion() string
	// Kind returns the kind of the resource.
	Kind() string
	// Generation returns the generation of the resource.
	Generation() int64
	// UID returns the uid of the resource.
	UID() types.UID
	// Labels returns the labels of the resource.
	Labels() map[string]string
	// LabelSelector returns a set of labels to be used for label selecting.
	LabelSelector() map[string]string

	// Replicas returns the desired replicas of the resource.
	Replicas() int32
	// PodTemplateSpec returns the pod template spec of the resource. This
	// is added to the underlying StatefulSet.
	PodTemplateSpec() *v1.PodTemplateSpec
	// VolumeClaimTemplates returns the volume claim templates of the
	// resource. This is added to the underlying StatefulSet.
	VolumeClaimTemplates() []v1.PersistentVolumeClaim
	Self() runtime.Object

	// EnsureResources
	EnsureResources(ctx context.Context) error

	// UpdateStatus updates the status of the StatefulResource. The
	// statefulset is parsed to provide additional information like
	// replicas to the status.
	UpdateStatus(ctx context.Context, sts *appsv1.StatefulSet) error

	// PreScaleDownHook is triggered when a scaledown is to be performed.
	// It's ensured that the hook will be triggered at least once, but it
	// may trigger multiple times e.g. if the scaledown fails at a later
	// stage and has to be retried.
	PreScaleDownHook(ctx context.Context) error

	// OnStableReplicasHook is triggered when the statefulSet is observed
	// to be stable meaning readyReplicas == desiredReplicas.
	// This hook can for instance be used to perform cleanup tasks.
	OnStableReplicasHook(ctx context.Context) error

	// Drain drains a pod for data. It's expected that the method only
	// returns after the pod has been drained.
	Drain(ctx context.Context, pod *v1.Pod) error

	DrainPods(ctx context.Context, pods []v1.Pod) error
}

type EDSResource struct {
	eds      *zv1.ElasticsearchDataSet
	kube     *clientset.Clientset
	esClient *ESClient
	recorder kube_record.EventRecorder
}

func (r *EDSResource) Name() string {
	return r.eds.Name
}

func (r *EDSResource) Namespace() string {
	return r.eds.Namespace
}

func (r *EDSResource) APIVersion() string {
	return r.eds.APIVersion
}

func (r *EDSResource) Kind() string {
	return r.eds.Kind
}

func (r *EDSResource) Labels() map[string]string {
	return r.eds.Labels
}

func (r *EDSResource) LabelSelector() map[string]string {
	return map[string]string{esDataSetLabelKey: r.Name()}
}

func (r *EDSResource) Generation() int64 {
	return r.eds.Generation
}

func (r *EDSResource) UID() types.UID {
	return r.eds.UID
}

func (r *EDSResource) Replicas() int32 {
	return edsReplicas(r.eds)
}

func (r *EDSResource) PodTemplateSpec() *v1.PodTemplateSpec {
	template := r.eds.Spec.Template.DeepCopy()
	return &v1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: template.Annotations,
			Labels:      template.Labels,
		},
		Spec: template.Spec,
	}
}

func (r *EDSResource) VolumeClaimTemplates() []v1.PersistentVolumeClaim {
	claims := make([]v1.PersistentVolumeClaim, 0, len(r.eds.Spec.VolumeClaimTemplates))
	for _, template := range r.eds.Spec.VolumeClaimTemplates {
		claims = append(claims, v1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        template.Name,
				Labels:      template.Labels,
				Annotations: template.Annotations,
			},
			Spec: template.Spec,
		})
	}
	return claims
}

func (r *EDSResource) EnsureResources(ctx context.Context) error {
	// ensure PDB
	err := r.ensurePodDisruptionBudget(ctx)
	if err != nil {
		return err
	}

	// ensure service
	err = r.ensureService(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (r *EDSResource) Self() runtime.Object {
	return r.eds
}

// ensurePodDisruptionBudget creates a PodDisruptionBudget for the
// ElasticsearchDataSet if it doesn't already exist.
func (r *EDSResource) ensurePodDisruptionBudget(ctx context.Context) error {
	var pdb *pv1.PodDisruptionBudget
	var err error

	pdb, err = r.kube.PolicyV1().PodDisruptionBudgets(r.eds.Namespace).Get(ctx, r.eds.Name, metav1.GetOptions{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf(
				"failed to get PodDisruptionBudget for %s %s/%s: %v",
				r.eds.Kind,
				r.eds.Namespace, r.eds.Name,
				err,
			)
		}
		pdb = nil
	}

	// check if owner
	if pdb != nil && !isOwnedReference(r, pdb.ObjectMeta) {
		return fmt.Errorf(
			"PodDisruptionBudget %s/%s is not owned by the %s %s/%s",
			pdb.Namespace, pdb.Name,
			r.eds.Kind,
			r.eds.Namespace, r.eds.Name,
		)
	}

	matchLabels := r.LabelSelector()

	createPDB := false

	if pdb == nil {
		createPDB = true
		maxUnavailable := intstr.FromInt(0)
		pdb = &pv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Name:      r.eds.Name,
				Namespace: r.eds.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: r.eds.APIVersion,
						Kind:       r.eds.Kind,
						Name:       r.eds.Name,
						UID:        r.eds.UID,
					},
				},
			},
			Spec: pv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &maxUnavailable,
			},
		}
	}

	pdb.Labels = r.eds.Labels
	pdb.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: matchLabels,
	}

	if createPDB {
		var err error
		_, err = r.kube.PolicyV1().PodDisruptionBudgets(pdb.Namespace).Create(ctx, pdb, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf(
				"failed to create PodDisruptionBudget for %s %s/%s: %v",
				r.eds.Kind,
				r.eds.Namespace, r.eds.Name,
				err,
			)
		}
		r.recorder.Event(r.eds, v1.EventTypeNormal, "CreatedPDB", fmt.Sprintf(
			"Created PodDisruptionBudget '%s/%s' for %s",
			pdb.Namespace, pdb.Name, r.eds.Kind,
		))
	}

	return nil
}

// ensureService creates a Service for the ElasticsearchDataSet if it doesn't
// already exist.
func (r *EDSResource) ensureService(ctx context.Context) error {
	var svc *v1.Service
	var err error

	svc, err = r.kube.CoreV1().Services(r.eds.Namespace).Get(ctx, r.eds.Name, metav1.GetOptions{})
	if err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf(
				"failed to get Service for %s %s/%s: %v",
				r.eds.Kind,
				r.eds.Namespace, r.eds.Name,
				err,
			)
		}
		svc = nil
	}

	// check if owner
	if svc != nil && !isOwnedReference(r, svc.ObjectMeta) {
		return fmt.Errorf(
			"the Service '%s/%s' is not owned by the %s '%s/%s'",
			svc.Namespace, svc.Name,
			r.eds.Kind,
			r.eds.Namespace, r.eds.Name,
		)
	}

	matchLabels := r.LabelSelector()

	createService := false

	if svc == nil {
		createService = true
		svc = &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      r.eds.Name,
				Namespace: r.eds.Namespace,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: r.eds.APIVersion,
						Kind:       r.eds.Kind,
						Name:       r.eds.Name,
						UID:        r.eds.UID,
					},
				},
			},
			Spec: v1.ServiceSpec{
				Type: v1.ServiceTypeClusterIP,
			},
		}
	}

	svc.Labels = r.eds.Labels
	svc.Spec.Selector = matchLabels
	// TODO: derive port from EDS
	svc.Spec.Ports = []v1.ServicePort{
		{
			Name:       "elasticsearch",
			Protocol:   v1.ProtocolTCP,
			Port:       defaultElasticsearchDataSetEndpointPort,
			TargetPort: intstr.FromInt(defaultElasticsearchDataSetEndpointPort),
		},
	}

	if createService {
		var err error
		_, err = r.kube.CoreV1().Services(svc.Namespace).Create(ctx, svc, metav1.CreateOptions{})
		if err != nil {
			return fmt.Errorf(
				"failed to create Service for %s %s/%s: %v",
				r.eds.Kind,
				r.eds.Namespace, r.eds.Name,
				err,
			)
		}
		r.recorder.Event(r.eds, v1.EventTypeNormal, "CreatedService", fmt.Sprintf(
			"Created Service '%s/%s' for %s",
			svc.Namespace, svc.Name, r.eds.Kind,
		))
	}

	return nil
}

// Drain drains a pod for Elasticsearch data.
func (r *EDSResource) Drain(ctx context.Context, pod *v1.Pod) error {
	return r.esClient.Drain(ctx, pod)
}

func (r *EDSResource) DrainPods(ctx context.Context, pods []v1.Pod) error {
	return r.esClient.DrainPods(ctx, pods)
}

// PreScaleDownHook ensures that the IndexReplicas is set as defined in the EDS
// 'scaling-operation' annotation prior to scaling down the internal
// StatefulSet.
func (r *EDSResource) PreScaleDownHook(ctx context.Context) error {
	return r.applyScalingOperation(ctx)
}

// OnStableReplicasHook ensures that the indexReplicas is set as defined in the
// EDS scaling-operation annotation.
func (r *EDSResource) OnStableReplicasHook(ctx context.Context) error {
	err := r.applyScalingOperation(ctx)
	if err != nil {
		return err
	}

	// cleanup state in ES
	return r.esClient.Cleanup(ctx)
}

// UpdateStatus updates the status of the EDS to set the current replicas from
// StatefulSet and updating the observedGeneration.
func (r *EDSResource) UpdateStatus(ctx context.Context, sts *appsv1.StatefulSet) error {
	observedGeneration := int64(0)
	if r.eds.Status.ObservedGeneration != nil {
		observedGeneration = *r.eds.Status.ObservedGeneration
	}

	replicas := int32(0)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}

	if r.eds.Generation != observedGeneration ||
		r.eds.Status.Replicas != replicas {
		r.eds.Status.Replicas = replicas
		r.eds.Status.ObservedGeneration = &r.eds.Generation
		eds, err := r.kube.ZalandoV1().ElasticsearchDataSets(r.eds.Namespace).UpdateStatus(ctx, r.eds, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		// set TypeMeta manually because of this bug:
		// https://github.com/kubernetes/client-go/issues/308
		eds.APIVersion = "zalando.org/v1"
		eds.Kind = "ElasticsearchDataSet"
		r.eds = eds
	}

	return nil
}

func (r *EDSResource) applyScalingOperation(ctx context.Context) error {
	operation, err := edsScalingOperation(r.eds)
	if err != nil {
		return err
	}

	if operation != nil && operation.ScalingDirection != NONE {
		err = r.esClient.UpdateIndexSettings(operation.IndexReplicas)
		if err != nil {
			return err
		}
		err = r.removeScalingOperationAnnotation(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// removeScalingOperationAnnotation removes the 'scaling-operation' annotation
// from the EDS.
// If the annotation is already gone, this is a no-op.
func (r *EDSResource) removeScalingOperationAnnotation(ctx context.Context) error {
	if _, ok := r.eds.Annotations[esScalingOperationKey]; ok {
		delete(r.eds.Annotations, esScalingOperationKey)
		_, err := r.kube.ZalandoV1().
			ElasticsearchDataSets(r.eds.Namespace).
			Update(ctx, r.eds, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("failed to remove 'scaling-operating' annotation from EDS: %v", err)
		}
	}
	return nil
}

// edsReplicas returns the desired node replicas of an ElasticsearchDataSet
// In case it was not specified, it will return '1'.
func edsReplicas(eds *zv1.ElasticsearchDataSet) int32 {
	scaling := eds.Spec.Scaling
	if scaling == nil || !scaling.Enabled {
		if eds.Spec.Replicas == nil {
			return 1
		}
		return *eds.Spec.Replicas
	}
	// initialize with minReplicas
	minReplicas := eds.Spec.Scaling.MinReplicas
	if eds.Spec.Replicas == nil {
		return minReplicas
	}
	currentReplicas := *eds.Spec.Replicas
	return int32(math.Max(float64(currentReplicas), float64(scaling.MinReplicas)))
}