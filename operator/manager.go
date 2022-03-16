package operator

import (
	"sync"
)

var (
	scalingManagerOnce sync.Once
	scalingManager     *ScalingManager
)

type ScalingManager struct {
	ActiveScalingContinues bool
	ScalingDisabled        bool
}

func init() {
	scalingManagerOnce.Do(func() {
		scalingManager = &ScalingManager{}
	})
}
