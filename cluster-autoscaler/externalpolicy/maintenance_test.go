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
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	api "k8s.io/autoscaler/cluster-autoscaler/externalpolicy/protos"
	utiltest "k8s.io/autoscaler/cluster-autoscaler/utils/test"
)

func TestMaintenanceExcludesOldExistingAndIncomingDestinations(t *testing.T) {
	source, old := plannerNode("source"), plannerNode("other-old")
	pod := plannerPod("work", source.Name)
	pod.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
	planner, _ := plannerFixture(t, []*corev1.Node{source, old}, []*corev1.Pod{pod})
	publishGroups(t, planner, map[string]*corev1.Node{"preferred": plannerNode("old-template"), "fallback": plannerNode("new-template")}, map[string]int{"preferred": 1}, nil)
	request := &api.ReplacementRequest{Sources: []*api.NodeIdentity{identity(source, "source")}, PlannedMaintenance: true, AllowedNodeGroupIds: []string{"fallback"}, MaximumAdditionalNodes: 1}
	result, err := planner.PlanReplacement(context.Background(), request)
	if err != nil || !result.Covered || len(result.Additions) != 1 || result.Additions[0].NodeGroupId != "fallback" || result.LatestDrainStartUnixMillis != 0 {
		t.Fatalf("old room satisfied planned replacement: %+v %v", result, err)
	}
	drain, err := planner.PlanDrain(context.Background(), &api.DrainRequest{Sources: request.Sources, PlannedMaintenance: true})
	if err != nil || !drain.WaitForCapacity || drain.DeadlineReached || len(drain.Evictions) != 0 {
		t.Fatalf("excluded existing room authorized maintenance: %+v %v", drain, err)
	}
}

func TestMaintenanceUsesOnlyReadyIdentifiedDestinations(t *testing.T) {
	for _, ready := range []bool{false, true} {
		t.Run(map[bool]string{false: "unready", true: "ready"}[ready], func(t *testing.T) {
			source, destination := plannerNode("source"), plannerNode("replacement")
			utiltest.SetNodeReadyState(destination, ready, time.Now())
			planner, client := plannerFixture(t, []*corev1.Node{source, destination}, []*corev1.Pod{plannerPod("work", source.Name)})
			request := &api.DrainRequest{Sources: []*api.NodeIdentity{identity(source, "source")}, PlannedMaintenance: true, Destinations: []*api.NodeIdentity{identity(destination, "alternative")}}
			result, err := planner.PlanDrain(context.Background(), request)
			if err != nil || result.WaitForCapacity == ready || result.DeadlineReached || result.LatestDrainStartUnixMillis != 0 || len(result.Nodes) != 1 || result.Nodes[0].Waiting == ready {
				t.Fatalf("incorrect maintenance gate: %+v %v", result, err)
			}
			if ready && len(result.Evictions) != 1 || !ready && len(result.Evictions) != 0 {
				t.Fatalf("incorrect maintenance eviction estimate: %+v", result)
			}
			request.Destinations[0].Uid = "reused-name-different-node"
			if _, err := planner.PlanDrain(context.Background(), request); err == nil {
				t.Fatal("stale destination identity accepted")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "list" && action.GetVerb() != "watch" {
					t.Fatalf("maintenance planner performed a write: %v", action)
				}
			}
		})
	}
}

func TestMaintenanceRejectsEveryLossDeadline(t *testing.T) {
	source := plannerNode("source")
	planner, _ := plannerFixture(t, []*corev1.Node{source}, nil)
	for _, deadline := range []int64{1, time.Now().Add(24 * time.Hour).UnixMilli()} {
		_, err := planner.PlanReplacement(context.Background(), &api.ReplacementRequest{Sources: []*api.NodeIdentity{identity(source, "source")}, PlannedMaintenance: true, DeadlineUnixMillis: deadline})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("maintenance replacement accepted deadline: %v", err)
		}
		_, err = planner.PlanDrain(context.Background(), &api.DrainRequest{Sources: []*api.NodeIdentity{identity(source, "source")}, PlannedMaintenance: true, DeadlineUnixMillis: deadline})
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("maintenance drain accepted deadline: %v", err)
		}
	}
	_, err := planner.PlanDrain(context.Background(), &api.DrainRequest{PlannedMaintenance: true, SourceDeadlinesUnixMillis: map[string]int64{string(source.UID): 1}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("maintenance accepted a per-source deadline: %v", err)
	}
}

func TestIdleMaintenancePreservesMinimumAndCreditsAcceptedCapacity(t *testing.T) {
	source := plannerNode("source")
	planner, _ := plannerFixture(t, []*corev1.Node{source}, nil)
	publishGroups(t, planner, map[string]*corev1.Node{"fallback": plannerNode("template")}, nil, nil)
	request := &api.ReplacementRequest{Sources: []*api.NodeIdentity{identity(source, "source")}, PlannedMaintenance: true, AllowedNodeGroupIds: []string{"fallback"}, MaximumAdditionalNodes: 1, MinimumDestinationNodes: 1}
	result, err := planner.PlanReplacement(context.Background(), request)
	if err != nil || !result.Covered || len(result.Additions) != 1 || result.Additions[0].Count != 1 {
		t.Fatalf("idle maintenance did not preserve the declared minimum: %+v %v", result, err)
	}
	request.PendingTargetSizes = map[string]int32{"fallback": 1}
	result, err = planner.PlanReplacement(context.Background(), request)
	if err != nil || !result.Covered || len(result.Additions) != 0 {
		t.Fatalf("retry duplicated the accepted minimum-capacity request: %+v %v", result, err)
	}
	request.PendingTargetSizes = nil
	planner.options.MaxNodesTotal = 1
	result, err = planner.PlanReplacement(context.Background(), request)
	if err != nil || result.Covered || len(result.Additions) != 0 {
		t.Fatalf("minimum overrode the cluster node limit: %+v %v", result, err)
	}
}

func TestMaintenanceRespectsVoluntaryDisruptionAndJointPDB(t *testing.T) {
	for _, mode := range []string{"pdb", "joint-pdb", "safe-to-evict"} {
		t.Run(mode, func(t *testing.T) {
			source, destination := plannerNode("source"), plannerNode("replacement")
			pod := plannerPod("work", source.Name)
			pods := []*corev1.Pod{pod}
			extra := []runtime.Object{}
			if mode == "safe-to-evict" {
				pod.Annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"] = "false"
			} else {
				allowed := int32(0)
				if mode == "joint-pdb" {
					other := plannerPod("other", source.Name)
					other.Labels = pod.Labels
					pods, allowed = append(pods, other), 1
				}
				extra = append(extra, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "protect", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: pod.Labels}}, Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: allowed}})
			}
			planner, _ := plannerFixture(t, []*corev1.Node{source, destination}, pods, extra...)
			publishGroups(t, planner, map[string]*corev1.Node{"fallback": plannerNode("template")}, nil, nil)
			sources := []*api.NodeIdentity{identity(source, "source")}
			destinations := []*api.NodeIdentity{identity(destination, "alternative")}
			result, err := planner.PlanDrain(context.Background(), &api.DrainRequest{Sources: sources, PlannedMaintenance: true, Destinations: destinations})
			if mode == "joint-pdb" {
				if err != nil || result.WaitForCapacity || result.PdbBlocked || len(result.Evictions) != 1 || result.DeadlineReached {
					t.Fatalf("native sequential drain did not preserve its PDB budget: %+v %v", result, err)
				}
				return
			}
			if err != nil || !result.WaitForCapacity || len(result.Evictions) != 0 || result.DeadlineReached || (mode != "safe-to-evict" && !result.PdbBlocked) {
				t.Fatalf("voluntary disruption restriction bypassed: %+v %v", result, err)
			}
			replacement, err := planner.PlanReplacement(context.Background(), &api.ReplacementRequest{Sources: sources, PlannedMaintenance: true, Destinations: destinations, AllowedNodeGroupIds: []string{"fallback"}, MaximumAdditionalNodes: 1})
			if err != nil || replacement.Covered || len(replacement.Additions) != 0 {
				t.Fatalf("unneeded maintenance capacity was purchased for blocked work: %+v %v", replacement, err)
			}
		})
	}
}
