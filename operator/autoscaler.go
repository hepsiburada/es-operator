package operator

import (
	"errors"
	"fmt"
	"math"

	"time"

	log "github.com/sirupsen/logrus"
	zv1 "github.com/zalando-incubator/es-operator/pkg/apis/zalando.org/v1"
	v1 "k8s.io/api/core/v1"
)

// 1. check if we have enough data
// 2. decide if we need to scale up or down
// 3. retrieve elasticsearch data (indices, shards, replicas)
// 4. decide if we only need to increase/decrease nodes
//    -> if not, don't do anything for now...

type ScalingDirection int

func (d ScalingDirection) String() string {
	switch d {
	case DOWN:
		return "DOWN"
	case UP:
		return "UP"
	case NONE:
		return "NONE"
	}
	return ""
}

const (
	DOWN ScalingDirection = iota
	NONE
	UP
)

type ScalingOperation struct {
	ScalingDirection ScalingDirection
	NodeReplicas     *int32
	IndexReplicas    []ESIndex
	Description      string
}

func noopScalingOperation(description string) *ScalingOperation {
	return &ScalingOperation{
		ScalingDirection: NONE,
		Description:      description,
	}
}

type AutoScaler struct {
	logger          *log.Entry
	eds             *zv1.ElasticsearchDataSet
	esMSet          *zv1.ElasticsearchMetricSet
	metricsInterval time.Duration
	pods            []v1.Pod
	esClient        *ESClient
}

func NewAutoScaler(es *ESResource, metricsInterval time.Duration, esClient *ESClient) *AutoScaler {
	return &AutoScaler{
		logger: log.WithFields(log.Fields{
			"eds":       es.ElasticsearchDataSet.Name,
			"namespace": es.ElasticsearchDataSet.Namespace,
		}),
		eds:             es.ElasticsearchDataSet,
		esMSet:          es.MetricSet,
		metricsInterval: metricsInterval,
		pods:            es.Pods,
		esClient:        esClient,
	}
}

func (as *AutoScaler) scalingHint() ScalingDirection {
	scaling := as.eds.Spec.Scaling

	// no metrics yet
	if as.esMSet == nil {
		return NONE
	}

	status := as.eds.Status

	// TODO: only consider metric samples that are not too old.
	sampleSize := len(as.esMSet.Metrics)

	// check for enough data points
	requiredScaledownSamples := int(math.Ceil(float64(scaling.ScaleDownThresholdDurationSeconds) / as.metricsInterval.Seconds()))
	if sampleSize >= requiredScaledownSamples {
		// check if CPU is below threshold for the last n samples
		scaleDownRequired := true
		for _, currentItem := range as.esMSet.Metrics[sampleSize-requiredScaledownSamples:] {
			if currentItem.Value >= scaling.ScaleDownCPUBoundary {
				scaleDownRequired = false
				break
			}
		}
		if scaleDownRequired {
			if status.LastScaleDownStarted == nil || status.LastScaleDownStarted.Time.Before(time.Now().Add(-time.Duration(scaling.ScaleDownCooldownSeconds)*time.Second)) {
				as.logger.Infof("Scaling hint: %s", DOWN)
				return DOWN
			}
			as.logger.Info("Not scaling down, currently in cool-down period.")
		}
	}

	requiredScaleUpSamples := int(math.Ceil(float64(scaling.ScaleUpThresholdDurationSeconds) / as.metricsInterval.Seconds()))
	if sampleSize >= requiredScaleUpSamples {
		// check if CPU is above threshold for the last n samples
		scaleUpRequired := true
		for _, currentItem := range as.esMSet.Metrics[sampleSize-requiredScaleUpSamples:] {
			if currentItem.Value <= scaling.ScaleUpCPUBoundary {
				scaleUpRequired = false
				break
			}
		}
		if scaleUpRequired {
			if status.LastScaleUpStarted == nil || status.LastScaleUpStarted.Time.Before(time.Now().Add(-time.Duration(scaling.ScaleUpCooldownSeconds)*time.Second)) {
				as.logger.Infof("Scaling hint: %s", UP)
				return UP
			}
			as.logger.Info("Not scaling up, currently in cool-down period.")
		}
	}
	return NONE
}

// TODO: check alternative approach by configuring the tags used for `index.routing.allocation`
// and deriving the indices from there.
func (as *AutoScaler) GetScalingOperationByIndexAliasNew(indexAlias string) (*ScalingOperation, error) {
	direction := as.scalingHint()

	esIndices, err := as.esClient.GetIndicesWithAlias()
	if err != nil {
		return nil, err
	}

	mainIndex, err := as.esClient.GetIndexByIndexAlias(indexAlias)
	if err != nil || mainIndex == nil {
		as.logger.Info("Main Index operation error")
		return nil, err
	}

	esShards, err := as.esClient.GetShards()
	if err != nil {
		return nil, err
	}

	esNodes, err := as.esClient.GetNodes()
	if err != nil {
		return nil, err
	}

	managedIndices := as.getManagedIndices(esIndices, esShards)
	managedNodes := as.getManagedNodes(as.pods, esNodes)
	return as.calculateScalingOperationNew(managedIndices, *mainIndex, managedNodes, direction), nil
}

// TODO: check alternative approach by configuring the tags used for `index.routing.allocation`
// and deriving the indices from there.
func (as *AutoScaler) GetScalingOperation() (*ScalingOperation, error) {
	direction := as.scalingHint()

	esIndices, err := as.esClient.GetIndicesWithAlias()
	if err != nil {
		return nil, err
	}

	esShards, err := as.esClient.GetShards()
	if err != nil {
		return nil, err
	}

	esNodes, err := as.esClient.GetNodes()
	if err != nil {
		return nil, err
	}

	managedIndices := as.getManagedIndices(esIndices, esShards)
	managedNodes := as.getManagedNodes(as.pods, esNodes)
	return as.calculateScalingOperation(managedIndices, managedNodes, direction), nil
}

func (as *AutoScaler) getManagedNodes(pods []v1.Pod, esNodes []ESNode) []ESNode {
	podIPs := make(map[string]struct{})
	for _, pod := range pods {
		if pod.Status.PodIP != "" {
			podIPs[pod.Status.PodIP] = struct{}{}
		}
	}
	managedNodes := make([]ESNode, 0, len(pods))
	for _, node := range esNodes {
		if _, ok := podIPs[node.IP]; ok {
			managedNodes = append(managedNodes, node)
		}
	}
	return managedNodes
}

func (as *AutoScaler) getManagedIndices(esIndices []ESIndex, esShards []ESShard) map[string]ESIndex {
	podIPs := make(map[string]struct{})
	for _, pod := range as.pods {
		if pod.Status.PodIP != "" {
			podIPs[pod.Status.PodIP] = struct{}{}
		}
	}
	managedIndices := make(map[string]ESIndex)
	for _, shard := range esShards {
		if _, ok := podIPs[shard.IP]; ok {
			for _, index := range esIndices {
				if shard.Index == index.Index {
					managedIndices[shard.Index] = index
					break
				}
			}
		}
	}
	return managedIndices
}

func (as *AutoScaler) calculateScalingOperationNew(managedIndices map[string]ESIndex, mainIndex ESIndex, managedNodes []ESNode, scalingHint ScalingDirection) *ScalingOperation {
	scalingSpec := as.eds.Spec.Scaling

	currentDesiredNodeReplicas := as.eds.Spec.Replicas
	if currentDesiredNodeReplicas == nil {
		return noopScalingOperation("DesiredReplicas is not set yet.")
	}

	if len(managedIndices) == 0 {
		return noopScalingOperation("No indices allocated yet.")
	}

	scalingOperation := as.scaleUpOrDownNew(managedIndices, mainIndex, scalingHint, *currentDesiredNodeReplicas)

	// safety check: ensure we don't scale below minIndexReplicas+1
	if scalingOperation.NodeReplicas != nil && *scalingOperation.NodeReplicas < scalingSpec.MinIndexReplicas+1 {
		return noopScalingOperation(fmt.Sprintf("Scaling would violate the minimum required nodes to hold %d index replicas.", scalingSpec.MinIndexReplicas))
	}

	// safety check: ensure we don't scale-down if disk usage is already above threshold
	if scalingOperation.ScalingDirection == DOWN && scalingSpec.DiskUsagePercentScaledownWatermark > 0 && as.getMaxDiskUsage(managedNodes) > float64(scalingSpec.DiskUsagePercentScaledownWatermark) {
		return noopScalingOperation(fmt.Sprintf("Scaling would violate the minimum required disk free percent: %.2f", 75.0))
	}

	return scalingOperation
}

func (as *AutoScaler) calculateScalingOperation(managedIndices map[string]ESIndex, managedNodes []ESNode, scalingHint ScalingDirection) *ScalingOperation {
	scalingSpec := as.eds.Spec.Scaling

	currentDesiredNodeReplicas := as.eds.Spec.Replicas
	if currentDesiredNodeReplicas == nil {
		return noopScalingOperation("DesiredReplicas is not set yet.")
	}

	if len(managedIndices) == 0 {
		return noopScalingOperation("No indices allocated yet.")
	}

	scalingOperation := as.scaleUpOrDown(managedIndices, scalingHint, *currentDesiredNodeReplicas)

	// safety check: ensure we don't scale below minIndexReplicas+1
	if scalingOperation.NodeReplicas != nil && *scalingOperation.NodeReplicas < scalingSpec.MinIndexReplicas+1 {
		return noopScalingOperation(fmt.Sprintf("Scaling would violate the minimum required nodes to hold %d index replicas.", scalingSpec.MinIndexReplicas))
	}

	// safety check: ensure we don't scale-down if disk usage is already above threshold
	if scalingOperation.ScalingDirection == DOWN && scalingSpec.DiskUsagePercentScaledownWatermark > 0 && as.getMaxDiskUsage(managedNodes) > float64(scalingSpec.DiskUsagePercentScaledownWatermark) {
		return noopScalingOperation(fmt.Sprintf("Scaling would violate the minimum required disk free percent: %.2f", 75.0))
	}

	return scalingOperation
}

func (as *AutoScaler) getMaxDiskUsage(managedNodes []ESNode) float64 {
	maxDisk := 0.0
	for _, node := range managedNodes {
		maxDisk = math.Max(maxDisk, node.DiskUsedPercent)
	}
	return maxDisk
}

func (as *AutoScaler) ensureBoundsNodeReplicas(newDesiredNodeReplicas int32) int32 {
	scalingSpec := as.eds.Spec.Scaling
	if scalingSpec.MaxReplicas > 0 && scalingSpec.MaxReplicas < newDesiredNodeReplicas {
		as.logger.Warnf("Requested to scale up to %d, which is beyond the defined maxReplicas of %d.", newDesiredNodeReplicas, scalingSpec.MaxReplicas)
		return scalingSpec.MaxReplicas
	}
	if scalingSpec.MinReplicas > 0 && scalingSpec.MinReplicas > newDesiredNodeReplicas {
		return scalingSpec.MinReplicas
	}
	return newDesiredNodeReplicas
}

func (as *AutoScaler) getCoordinatorIndexSettingsForScaleDown(esIndices map[string]ESIndex, coordinatorIndex ESIndex, currentTotalShards int32) (*CoordinatorSettings, error) {
	scalingSpec := as.eds.Spec.Scaling
	newTotalShards := currentTotalShards

	for _, index := range esIndices {
		if index.Index == coordinatorIndex.Index {
			if index.Replicas > scalingSpec.MinIndexReplicas {
				newTotalShards -= index.Primaries

				newCoordinatorIndex := ESIndex{
					Index:     index.Index,
					Primaries: index.Primaries,
					Replicas:  index.Replicas - 1,
				}

				coordinatorSettings := &CoordinatorSettings{
					DesiredIndexReplica:     newCoordinatorIndex,
					CalculatedNewTotalShard: newTotalShards,
				}

				return coordinatorSettings, nil
			} else {
				return nil, errors.New(fmt.Sprintf("Not allowed to scale down due to minIndexReplicas (%d) reached for index %s.",
					scalingSpec.MinIndexReplicas, index.Index))
			}
		}
	}
	return nil, errors.New(fmt.Sprintf("%s not found in esIndices", coordinatorIndex.Index))
}

func (as *AutoScaler) getCoordinatorIndexSettingsForScaleUp(esIndices map[string]ESIndex, coordinatorIndex ESIndex, currentTotalShards int32) (*CoordinatorSettings, error) {
	scalingSpec := as.eds.Spec.Scaling
	newTotalShards := currentTotalShards

	for _, index := range esIndices {
		if index.Index == coordinatorIndex.Index {
			if index.Replicas < scalingSpec.MaxIndexReplicas {
				newTotalShards += index.Primaries

				newCoordinatorIndex := ESIndex{
					Index:     index.Index,
					Primaries: index.Primaries,
					Replicas:  index.Replicas + 1,
				}

				coordinatorSettings := &CoordinatorSettings{
					DesiredIndexReplica:     newCoordinatorIndex,
					CalculatedNewTotalShard: newTotalShards,
				}

				return coordinatorSettings, nil
			} else {
				return nil, errors.New(fmt.Sprintf("Not allowed to scale up due to maxIndexReplicas (%d) reached for index %s.",
					scalingSpec.MaxIndexReplicas, index.Index))
			}
		}
	}
	return nil, errors.New(fmt.Sprintf("%s not found in esIndices", coordinatorIndex.Index))
}

func (as *AutoScaler) scaleUpOrDownNew(esIndices map[string]ESIndex, mainIndex ESIndex, scalingHint ScalingDirection, currentDesiredNodeReplicas int32) *ScalingOperation {
	scalingSpec := as.eds.Spec.Scaling

	newDesiredIndexReplicas := make([]ESIndex, 0, len(esIndices))

	currentTotalShards := int32(0)
	for _, index := range esIndices {
		as.logger.Debugf("Index: %s, primaries: %d, replicas: %d", index.Index, index.Primaries, index.Replicas)
		if index.Index == mainIndex.Index {
			currentTotalShards += index.Primaries * (index.Replicas + 1)

			if index.Replicas < scalingSpec.MinIndexReplicas {
				newDesiredIndexReplicas = append(newDesiredIndexReplicas, ESIndex{
					Index:     index.Index,
					Primaries: index.Primaries,
					Replicas:  scalingSpec.MinIndexReplicas,
				})
			}
			break
		}
	}

	currentShardToNodeRatio := shardToNodeRatio(currentTotalShards, currentDesiredNodeReplicas)

	// independent of the scaling direction: in case the scaling setting MaxShardsPerNode has changed, we might need to scale up.
	if currentShardToNodeRatio > float64(scalingSpec.MaxShardsPerNode) {
		newDesiredNodeReplicas := as.ensureBoundsNodeReplicas(int32(math.Ceil(shardToNodeRatio(currentTotalShards, scalingSpec.MaxShardsPerNode))))
		return &ScalingOperation{
			ScalingDirection: as.calculateScalingDirection(currentDesiredNodeReplicas, newDesiredNodeReplicas),
			NodeReplicas:     &newDesiredNodeReplicas,
			Description:      fmt.Sprintf("Current shard-to-node ratio (%.2f) exceeding the desired limit of (%d).", currentShardToNodeRatio, scalingSpec.MaxShardsPerNode),
		}
	}

	// independent of the scaling direction: in case there are indices with < MinIndexReplicas, we try to scale these indices.
	if len(newDesiredIndexReplicas) > 0 {
		return &ScalingOperation{
			ScalingDirection: UP,
			IndexReplicas:    newDesiredIndexReplicas,
			Description:      "Scale indices replicas to fit MinIndexReplicas requirement",
		}
	}

	switch scalingHint {
	case UP:
		if currentShardToNodeRatio <= float64(scalingSpec.MinShardsPerNode) {
			coordinatorIndexSettings, err := as.getCoordinatorIndexSettingsForScaleUp(esIndices, mainIndex, currentTotalShards)

			if err != nil {
				as.logger.Info(err)
				return noopScalingOperation(err.Error())
			}

			newDesiredIndexReplicas = append(newDesiredIndexReplicas, coordinatorIndexSettings.DesiredIndexReplica)

			newTotalShards := coordinatorIndexSettings.CalculatedNewTotalShard
			for _, index := range esIndices {
				if index.Index != mainIndex.Index {
					newReplicas := int32(math.Floor(float64((newTotalShards / index.Primaries) - 1)))

					newDesiredIndexReplicas = append(newDesiredIndexReplicas, ESIndex{
						Index:     index.Index,
						Primaries: index.Primaries,
						Replicas:  newReplicas,
					})
				}
			}
			if newTotalShards > currentTotalShards {
				newDesiredNodeReplicas := currentDesiredNodeReplicas

				scalingMsg := "Increasing index replicas."

				// Evaluate new number of nodes only if we above MinShardsPerNode parameter
				if shardToNodeRatio(newTotalShards, currentDesiredNodeReplicas) >= float64(scalingSpec.MinShardsPerNode) {
					newDesiredNodeReplicas = as.ensureBoundsNodeReplicas(
						calculateNodesWithSameShardToNodeRatio(currentDesiredNodeReplicas, currentTotalShards, newTotalShards))
					if newDesiredNodeReplicas != currentDesiredNodeReplicas {
						scalingMsg = fmt.Sprintf("Trying to keep shard-to-node ratio (%.2f), and increasing index replicas.", shardToNodeRatio(newTotalShards, newDesiredNodeReplicas))
					}
				}

				return &ScalingOperation{
					Description:   scalingMsg,
					NodeReplicas:  &newDesiredNodeReplicas,
					IndexReplicas: newDesiredIndexReplicas,
					// we don't use "as.calculateScalingDirection" because the func "calculateNodesWithSameShardToNodeRatio" can produce the same number of nodes
					// but we still need to scale up shards
					ScalingDirection: UP,
				}
			}
		}

		// round down to the next non-fractioned shard-to-node ratio
		newDesiredNodeReplicas := as.ensureBoundsNodeReplicas(calculateIncreasedNodes(currentDesiredNodeReplicas, currentTotalShards))

		return &ScalingOperation{
			ScalingDirection: as.calculateScalingDirection(currentDesiredNodeReplicas, newDesiredNodeReplicas),
			NodeReplicas:     &newDesiredNodeReplicas,
			Description:      fmt.Sprintf("Increasing node replicas to %d.", newDesiredNodeReplicas),
		}
	case DOWN:
		coordinatorIndexSettings, err := as.getCoordinatorIndexSettingsForScaleDown(esIndices, mainIndex, currentTotalShards)

		if err != nil {
			as.logger.Info(err)
			return noopScalingOperation(err.Error())
		}

		newDesiredIndexReplicas = append(newDesiredIndexReplicas, coordinatorIndexSettings.DesiredIndexReplica)
		newTotalShards := coordinatorIndexSettings.CalculatedNewTotalShard

		for _, index := range esIndices {
			if index.Index != mainIndex.Index && index.Replicas > scalingSpec.MinIndexReplicas {
				newReplicas := int32(math.Floor(float64((newTotalShards / index.Primaries) - 1)))
				newDesiredIndexReplicas = append(newDesiredIndexReplicas, ESIndex{
					Index:     index.Index,
					Primaries: index.Primaries,
					Replicas:  newReplicas,
				})
			}
		}
		if newTotalShards != currentTotalShards {
			newDesiredNodeReplicas := as.ensureBoundsNodeReplicas(calculateNodesWithSameShardToNodeRatio(currentDesiredNodeReplicas, currentTotalShards, newTotalShards))
			return &ScalingOperation{
				ScalingDirection: as.calculateScalingDirection(currentDesiredNodeReplicas, newDesiredNodeReplicas),
				NodeReplicas:     &newDesiredNodeReplicas,
				IndexReplicas:    newDesiredIndexReplicas,
				Description:      fmt.Sprintf("Keeping shard-to-node ratio (%.2f), and decreasing index replicas.", currentShardToNodeRatio),
			}
		}
		// increase shard-to-node ratio, and scale down by at least one
		newDesiredNodeReplicas := as.ensureBoundsNodeReplicas(calculateDecreasedNodes(currentDesiredNodeReplicas, currentTotalShards))
		ratio := shardToNodeRatio(newTotalShards, newDesiredNodeReplicas)
		if ratio > float64(scalingSpec.MaxShardsPerNode) {
			return noopScalingOperation(fmt.Sprintf("Scaling would violate the shard-to-node maximum (%.2f/%d).", ratio, scalingSpec.MaxShardsPerNode))
		}

		return &ScalingOperation{
			ScalingDirection: as.calculateScalingDirection(currentDesiredNodeReplicas, newDesiredNodeReplicas),
			NodeReplicas:     &newDesiredNodeReplicas,
			Description:      fmt.Sprintf("Decreasing node replicas to %d.", newDesiredNodeReplicas),
		}
	}
	return noopScalingOperation("Nothing to do")
}

func (as *AutoScaler) scaleUpOrDown(esIndices map[string]ESIndex, scalingHint ScalingDirection, currentDesiredNodeReplicas int32) *ScalingOperation {
	scalingSpec := as.eds.Spec.Scaling

	newDesiredIndexReplicas := make([]ESIndex, 0, len(esIndices))

	currentTotalShards := int32(0)
	for _, index := range esIndices {
		as.logger.Debugf("Index: %s, primaries: %d, replicas: %d", index.Index, index.Primaries, index.Replicas)
		currentTotalShards += index.Primaries * (index.Replicas + 1)

		if index.Replicas < scalingSpec.MinIndexReplicas {
			newDesiredIndexReplicas = append(newDesiredIndexReplicas, ESIndex{
				Index:     index.Index,
				Primaries: index.Primaries,
				Replicas:  scalingSpec.MinIndexReplicas,
			})
		}
	}

	currentShardToNodeRatio := shardToNodeRatio(currentTotalShards, currentDesiredNodeReplicas)

	// independent of the scaling direction: in case the scaling setting MaxShardsPerNode has changed, we might need to scale up.
	if currentShardToNodeRatio > float64(scalingSpec.MaxShardsPerNode) {
		newDesiredNodeReplicas := as.ensureBoundsNodeReplicas(int32(math.Ceil(shardToNodeRatio(currentTotalShards, scalingSpec.MaxShardsPerNode))))
		return &ScalingOperation{
			ScalingDirection: as.calculateScalingDirection(currentDesiredNodeReplicas, newDesiredNodeReplicas),
			NodeReplicas:     &newDesiredNodeReplicas,
			Description:      fmt.Sprintf("Current shard-to-node ratio (%.2f) exceeding the desired limit of (%d).", currentShardToNodeRatio, scalingSpec.MaxShardsPerNode),
		}
	}

	// independent of the scaling direction: in case there are indices with < MinIndexReplicas, we try to scale these indices.
	if len(newDesiredIndexReplicas) > 0 {
		return &ScalingOperation{
			ScalingDirection: UP,
			IndexReplicas:    newDesiredIndexReplicas,
			Description:      "Scale indices replicas to fit MinIndexReplicas requirement",
		}
	}

	switch scalingHint {
	case UP:
		if currentShardToNodeRatio <= float64(scalingSpec.MinShardsPerNode) {
			newTotalShards := currentTotalShards
			for _, index := range esIndices {
				if index.Replicas >= scalingSpec.MaxIndexReplicas {
					return noopScalingOperation(fmt.Sprintf("Not allowed to scale up due to maxIndexReplicas (%d) reached for index %s.",
						scalingSpec.MaxIndexReplicas, index.Index))
				}
				newTotalShards += index.Primaries
				newDesiredIndexReplicas = append(newDesiredIndexReplicas, ESIndex{
					Index:     index.Index,
					Primaries: index.Primaries,
					Replicas:  index.Replicas + 1,
				})
			}
			if newTotalShards > currentTotalShards {
				newDesiredNodeReplicas := currentDesiredNodeReplicas

				scalingMsg := "Increasing index replicas."

				// Evaluate new number of nodes only if we above MinShardsPerNode parameter
				if shardToNodeRatio(newTotalShards, currentDesiredNodeReplicas) >= float64(scalingSpec.MinShardsPerNode) {
					newDesiredNodeReplicas = as.ensureBoundsNodeReplicas(
						calculateNodesWithSameShardToNodeRatio(currentDesiredNodeReplicas, currentTotalShards, newTotalShards))
					if newDesiredNodeReplicas != currentDesiredNodeReplicas {
						scalingMsg = fmt.Sprintf("Trying to keep shard-to-node ratio (%.2f), and increasing index replicas.", shardToNodeRatio(newTotalShards, newDesiredNodeReplicas))
					}
				}

				return &ScalingOperation{
					Description:   scalingMsg,
					NodeReplicas:  &newDesiredNodeReplicas,
					IndexReplicas: newDesiredIndexReplicas,
					// we don't use "as.calculateScalingDirection" because the func "calculateNodesWithSameShardToNodeRatio" can produce the same number of nodes
					// but we still need to scale up shards
					ScalingDirection: UP,
				}
			}
		}

		// round down to the next non-fractioned shard-to-node ratio
		newDesiredNodeReplicas := as.ensureBoundsNodeReplicas(calculateIncreasedNodes(currentDesiredNodeReplicas, currentTotalShards))

		return &ScalingOperation{
			ScalingDirection: as.calculateScalingDirection(currentDesiredNodeReplicas, newDesiredNodeReplicas),
			NodeReplicas:     &newDesiredNodeReplicas,
			Description:      fmt.Sprintf("Increasing node replicas to %d.", newDesiredNodeReplicas),
		}
	case DOWN:
		newTotalShards := currentTotalShards
		for _, index := range esIndices {
			if index.Replicas > scalingSpec.MinIndexReplicas {
				newTotalShards -= index.Primaries
				newDesiredIndexReplicas = append(newDesiredIndexReplicas, ESIndex{
					Index:     index.Index,
					Primaries: index.Primaries,
					Replicas:  index.Replicas - 1,
				})
			}
		}
		if newTotalShards != currentTotalShards {
			newDesiredNodeReplicas := as.ensureBoundsNodeReplicas(calculateNodesWithSameShardToNodeRatio(currentDesiredNodeReplicas, currentTotalShards, newTotalShards))
			return &ScalingOperation{
				ScalingDirection: as.calculateScalingDirection(currentDesiredNodeReplicas, newDesiredNodeReplicas),
				NodeReplicas:     &newDesiredNodeReplicas,
				IndexReplicas:    newDesiredIndexReplicas,
				Description:      fmt.Sprintf("Keeping shard-to-node ratio (%.2f), and decreasing index replicas.", currentShardToNodeRatio),
			}
		}
		// increase shard-to-node ratio, and scale down by at least one
		newDesiredNodeReplicas := as.ensureBoundsNodeReplicas(calculateDecreasedNodes(currentDesiredNodeReplicas, currentTotalShards))
		ratio := shardToNodeRatio(newTotalShards, newDesiredNodeReplicas)
		if ratio > float64(scalingSpec.MaxShardsPerNode) {
			return noopScalingOperation(fmt.Sprintf("Scaling would violate the shard-to-node maximum (%.2f/%d).", ratio, scalingSpec.MaxShardsPerNode))
		}

		return &ScalingOperation{
			ScalingDirection: as.calculateScalingDirection(currentDesiredNodeReplicas, newDesiredNodeReplicas),
			NodeReplicas:     &newDesiredNodeReplicas,
			Description:      fmt.Sprintf("Decreasing node replicas to %d.", newDesiredNodeReplicas),
		}
	}
	return noopScalingOperation("Nothing to do")
}

func (as *AutoScaler) calculateScalingDirection(oldNodeReplicas, newNodeReplicas int32) ScalingDirection {
	if newNodeReplicas > oldNodeReplicas {
		return UP
	}
	if newNodeReplicas < oldNodeReplicas {
		return DOWN
	}
	return NONE
}

func shardToNodeRatio(shards, nodes int32) float64 {
	return float64(shards) / float64(nodes)
}

func calculateNodesWithSameShardToNodeRatio(currentDesiredNodeReplicas, currentTotalShards, newTotalShards int32) int32 {
	currentShardToNodeRatio := shardToNodeRatio(currentTotalShards, currentDesiredNodeReplicas)
	/*
		if currentShardToNodeRatio < 1 {
			return currentDesiredNodeReplicas
		}
	*/
	return int32(math.Ceil(float64(newTotalShards) / float64(currentShardToNodeRatio)))
}

func calculateDecreasedNodes(currentDesiredNodeReplicas, currentTotalShards int32) int32 {
	currentShardToNodeRatio := shardToNodeRatio(currentTotalShards, currentDesiredNodeReplicas)
	newDesiredNodes := int32(math.Min(float64(currentDesiredNodeReplicas)-float64(1), math.Ceil(float64(currentTotalShards)/math.Ceil(currentShardToNodeRatio+0.00001))))
	if newDesiredNodes <= 1 {
		return 1
	}
	return newDesiredNodes
}

func calculateIncreasedNodes(currentDesiredNodeReplicas, currentTotalShards int32) int32 {
	currentShardToNodeRatio := shardToNodeRatio(currentTotalShards, currentDesiredNodeReplicas)
	if currentShardToNodeRatio <= 1 {
		return currentTotalShards
	}
	return int32(math.Ceil(float64(currentTotalShards) / math.Floor(currentShardToNodeRatio-0.00001)))
}
