package operator

import (
	v1 "k8s.io/api/core/v1"
)

type updatePriority struct {
	Pod      v1.Pod
	Priority int
	Number   int
}

const (
	podDrainingPriority       = 16
	unschedulableNodePriority = 8
	nodeSelectorPriority      = 4
	podOldRevisionPriority    = 2
	stsReplicaDiffPriority    = 1
	// priorityNames
	podDrainingPriorityName       = "PodDraining"
	unschedulableNodePriorityName = "UnschedulableNode"
	nodeSelectorPriorityName      = "NodeSelector"
	podOldRevisionPriorityName    = "PodOldRevision"
	stsReplicaDiffPriorityName    = "STSReplicaDiff"
)


