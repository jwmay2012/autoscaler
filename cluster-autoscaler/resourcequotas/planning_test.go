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

package resourcequotas

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/autoscaler/cluster-autoscaler/utils/test"
)

func TestPlanningSnapshotPreservesOverlappingQuotas(t *testing.T) {
	tracker := newTracker([]*quotaStatus{
		{quota: &FakeQuota{Name: "global", AppliesToFn: MatchEveryNode}, limitsLeft: resourceList{"nodes": 3}},
		{quota: &FakeQuota{Name: "zone-a", AppliesToFn: func(n *corev1.Node) bool { return n.Labels["zone"] == "a" }}, limitsLeft: resourceList{"nodes": 1}},
		{quota: &FakeQuota{Name: "zone-b", AppliesToFn: func(n *corev1.Node) bool { return n.Labels["zone"] == "b" }}, limitsLeft: resourceList{"nodes": 2}},
	}, newNodeResourcesCache(&fakeCustomResourcesProcessor{}))
	remaining := tracker.Remaining()
	for _, zone := range []string{"a", "b"} {
		node := test.BuildTestNode(zone, 1000, 1000)
		node.Labels["zone"] = zone
		delta, err := tracker.DeltaForNode(nil, nil, node)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]int64{`"global"/"nodes"`: 1, `"zone-` + zone + `"/"nodes"`: 1}
		if diff := cmp.Diff(want, delta); diff != "" {
			t.Fatalf("quota charges (-want +got): %s", diff)
		}
		for key, amount := range delta {
			remaining[key] -= amount
		}
	}
	if remaining[`"global"/"nodes"`] != 1 || remaining[`"zone-a"/"nodes"`] != 0 || remaining[`"zone-b"/"nodes"`] != 1 {
		t.Fatalf("cross-group quota accounting: %v", remaining)
	}
	if tracker.Remaining()[`"global"/"nodes"`] != 3 {
		t.Fatal("planning changed the native tracker")
	}
	first := test.BuildTestNode("first", 1000, 1000)
	first.Labels["zone"] = "a"
	result, err := tracker.CheckDelta(nil, nil, first, 2)
	if err != nil || result.AllowedDelta != 1 {
		t.Fatalf("native limit diverged: %v, %v", result, err)
	}
}
