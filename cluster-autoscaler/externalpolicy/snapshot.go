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

package externalpolicy

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"sort"
)

// destinationSnapshot keeps unavailable Nodes visible to topology predicates,
// but never allows placement on them, even for Pods with blanket tolerations.
// The guard also applies to the native estimator's internal scheduling calls.
type destinationSnapshot struct {
	clustersnapshot.ClusterSnapshot
	unavailable map[string]bool
}

func (s *destinationSnapshot) CheckPredicates(pod *corev1.Pod, name string) clustersnapshot.SchedulingError {
	if s.unavailable[name] {
		return clustersnapshot.NewFailingPredicateError(pod, "PlanningDestination", []string{"node is unavailable"}, "", "")
	}
	return s.ClusterSnapshot.CheckPredicates(pod, name)
}

func (s *destinationSnapshot) SchedulePod(pod *corev1.Pod, name string) clustersnapshot.SchedulingError {
	if s.unavailable[name] {
		return clustersnapshot.NewFailingPredicateError(pod, "PlanningDestination", []string{"node is unavailable"}, "", "")
	}
	return s.ClusterSnapshot.SchedulePod(pod, name)
}

func (s *destinationSnapshot) SchedulePodOnAnyNodeMatching(pod *corev1.Pod, match func(*framework.NodeInfo) bool) (string, clustersnapshot.SchedulingError) {
	nodes, err := s.ListNodeInfos()
	if err != nil {
		return "", clustersnapshot.NewSchedulingInternalError(pod, err.Error())
	}
	// Native store iteration and the plugin runner's rotating starting index
	// are useful in the live loop, but would make identical read-only retries
	// reserve different destinations. Keep this planner's first-fit order stable.
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Node().Name < nodes[j].Node().Name })
	for _, node := range nodes {
		if s.unavailable[node.Node().Name] || !match(node) {
			continue
		}
		if err := s.SchedulePod(pod, node.Node().Name); err == nil {
			return node.Node().Name, nil
		} else if err.Type() == clustersnapshot.SchedulingInternalError {
			return "", err
		}
	}
	return "", clustersnapshot.NewNoNodesPassingPredicatesFoundError(pod)
}
