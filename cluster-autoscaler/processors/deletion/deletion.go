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

package deletion

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/context"
)

// Batch is a native, post-budget selection for one node group.
type Batch struct {
	NodeGroupID string
	Nodes       []*corev1.Node
	Empty       bool
	Atomic      bool
}

// Authorizer can defer the complete final batch before any taint or eviction.
// It cannot add nodes, bypass native checks, or authorize a subset.
type Authorizer interface {
	AuthorizeDeletion(*context.AutoscalingContext, []Batch, bool) (bool, string)
}
