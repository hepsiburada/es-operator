package operator

import (
	zv1 "github.com/zalando-incubator/es-operator/pkg/apis/zalando.org/v1"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
)

type ESResource struct {
	ElasticsearchDataSet *zv1.ElasticsearchDataSet
	StatefulSet          *appsv1.StatefulSet
	MetricSet            *zv1.ElasticsearchMetricSet
	Pods                 []v1.Pod
}

// Replicas returns the desired node replicas of an ElasticsearchDataSet
// In case it was not specified, it will return '1'.
func (es *ESResource) Replicas() int32 {
	return edsReplicas(es.ElasticsearchDataSet)
}
