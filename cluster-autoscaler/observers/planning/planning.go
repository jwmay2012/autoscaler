/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package planning

import (
	"time"

	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
)

// Observer receives provider templates and incoming-capacity accounting before
// speculative scheduling changes the loop snapshot. Implementations must copy
// anything they retain and must not mutate the autoscaling context.
type Observer interface {
	Observe(*context.AutoscalingContext, Snapshot)
}

// Snapshot contains native scale-up safety and resource accounting from one loop.
// A failed limit observation permits diagnostics, but never new capacity plans.
type Snapshot struct {
	ObservedAt    time.Time
	Templates     map[string]*framework.NodeInfo
	Upcoming      map[string]int
	Registered    map[string][]string
	SafeToScale   map[string]bool
	ResourcesLeft map[string]int64
	ResourceDelta map[string]map[string]int64
	LimitsError   string
	PDBs          []*policyv1.PodDisruptionBudget
}
