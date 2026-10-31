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
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	api "k8s.io/autoscaler/cluster-autoscaler/externalpolicy/protos"
	"k8s.io/client-go/kubernetes/fake"
)

const storageClassName = "zonal"

// zonalVolume returns a bound claim whose volume is restricted to one zone,
// either by volume node affinity or by the older topology label.
func zonalVolume(name, zone string, legacyLabel bool) []runtime.Object {
	mode := storagev1.VolumeBindingWaitForFirstConsumer
	class := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: storageClassName}, Provisioner: "example.com/csi", VolumeBindingMode: &mode}
	size := resource.MustParse("10Gi")
	claimUID := types.UID(name + "-claim-uid")
	volume := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name + "-volume", UID: types.UID(name + "-volume-uid")},
		Spec: corev1.PersistentVolumeSpec{
			Capacity:               corev1.ResourceList{corev1.ResourceStorage: size},
			AccessModes:            []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName:       storageClassName,
			PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: "example.com/csi", VolumeHandle: name}},
			ClaimRef:               &corev1.ObjectReference{Kind: "PersistentVolumeClaim", Namespace: "default", Name: name, UID: claimUID},
		},
		Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound},
	}
	if legacyLabel {
		volume.Labels = map[string]string{corev1.LabelTopologyZone: zone}
	} else {
		volume.Spec.NodeAffinity = &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
			MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{zone}}},
		}}}}
	}
	className := storageClassName
	// A completed binding carries the PV controller's annotation. Without it,
	// a claim that names a volume is still treated as unbound by the scheduler.
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: claimUID,
			Annotations: map[string]string{"pv.kubernetes.io/bind-completed": "yes", "pv.kubernetes.io/bound-by-controller": "yes"}},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &className,
			VolumeName:       volume.Name,
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: size}},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound, Capacity: corev1.ResourceList{corev1.ResourceStorage: size}},
	}
	return []runtime.Object{class, volume, claim}
}

func withClaim(pod *corev1.Pod, claim string) *corev1.Pod {
	pod.Spec.Volumes = append(pod.Spec.Volumes, corev1.Volume{Name: "data", VolumeSource: corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claim}}})
	return pod
}

func zonalNode(name, zone string) *corev1.Node {
	node := plannerNode(name)
	node.Labels[corev1.LabelTopologyZone] = zone
	return node
}

func blockingBudget(pod *corev1.Pod) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: pod.Name + "-budget", Namespace: "default", UID: types.UID(pod.Name + "-budget-uid")},
		Spec:   policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: pod.Labels}},
		Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0}}
}

func TestReplacementHonorsZonalPersistentVolumes(t *testing.T) {
	for _, test := range []struct {
		name        string
		volume      bool
		legacyLabel bool
		fallback    string
		want        string
	}{
		{name: "no volume uses the preferred zone", fallback: "zone-a", want: "preferred"},
		{name: "volume affinity requires the volume zone", volume: true, fallback: "zone-a", want: "fallback"},
		{name: "legacy volume label requires the volume zone", volume: true, legacyLabel: true, fallback: "zone-a", want: "fallback"},
		{name: "no offering in the volume zone stays uncovered", volume: true, fallback: "zone-b"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := zonalNode("source", "zone-a")
			pod := plannerPod("database", source.Name)
			extra := []runtime.Object{}
			if test.volume {
				withClaim(pod, "data")
				extra = zonalVolume("data", "zone-a", test.legacyLabel)
			}
			planner, client := plannerFixture(t, []*corev1.Node{source}, []*corev1.Pod{pod}, extra...)
			// The expander prefers the other zone whenever native predicates allow it.
			publishGroups(t, planner, map[string]*corev1.Node{"preferred": zonalNode("preferred-template", "zone-b"), "fallback": zonalNode("fallback-template", test.fallback)}, nil, nil)
			result, err := planner.PlanReplacement(context.Background(), replacementRequest(source))
			if err != nil {
				t.Fatal(err)
			}
			if test.want == "" {
				if result.Covered || len(result.Additions) != 0 || result.UncoveredPods != 1 {
					t.Fatalf("zonal workload was planned outside its volume zone: %+v", result)
				}
			} else if !result.Covered || len(result.Additions) != 1 || result.Additions[0].NodeGroupId != test.want || result.Additions[0].Count != 1 {
				t.Fatalf("expected one %s addition: %+v", test.want, result)
			}
			assertReadOnly(t, client)
		})
	}
}

func TestRelocationTreatsVolumeZoneConflictAsIncompatible(t *testing.T) {
	for _, legacyLabel := range []bool{false, true} {
		t.Run(fmt.Sprintf("legacy-label=%t", legacyLabel), func(t *testing.T) {
			source, sameZone, otherZone := zonalNode("source", "zone-a"), zonalNode("same-zone", "zone-a"), zonalNode("other-zone", "zone-b")
			pod := withClaim(plannerPod("database", source.Name), "data")
			extra := append(zonalVolume("data", "zone-a", legacyLabel), blockingBudget(pod))
			planner, client := plannerFixture(t, []*corev1.Node{source, sameZone, otherZone}, []*corev1.Pod{pod}, extra...)
			for _, test := range []struct {
				name         string
				destinations []*corev1.Node
				want         api.PlacementResult
				used         string
			}{
				{"other zone only", []*corev1.Node{otherZone}, api.PlacementResult_PLACEMENT_INCOMPATIBLE, ""},
				{"same zone", []*corev1.Node{sameZone}, api.PlacementResult_PLACEMENT_FEASIBLE, string(sameZone.UID)},
				{"both zones", []*corev1.Node{otherZone, sameZone}, api.PlacementResult_PLACEMENT_FEASIBLE, string(sameZone.UID)},
			} {
				t.Run(test.name, func(t *testing.T) {
					request := &api.RelocationRequest{Source: identity(source, "source")}
					for _, node := range test.destinations {
						request.Destinations = append(request.Destinations, identity(node, "target"))
					}
					result, err := planner.ExplainRelocation(context.Background(), request)
					if err != nil {
						t.Fatal(err)
					}
					if !result.PdbBlocked || result.OtherDisruptionBlocked || result.Placement != test.want || len(result.Pods) != 1 || result.Pods[0].Placement != test.want {
						t.Fatalf("volume zone and disruption were not classified separately: %+v", result)
					}
					if test.used != "" && (!reflect.DeepEqual(result.UsedNodeUids, []string{test.used}) || result.Pods[0].DestinationNodeUid != test.used) {
						t.Fatalf("relocation did not identify the same-zone destination: %+v", result)
					}
					if test.used == "" && len(result.UsedNodeUids) != 0 {
						t.Fatalf("an incompatible destination was reported as used: %+v", result)
					}
				})
			}
			assertReadOnly(t, client)
		})
	}
}

func TestDrainGateRequiresRoomInTheVolumeZone(t *testing.T) {
	for _, destinationZone := range []string{"zone-b", "zone-a"} {
		t.Run(destinationZone, func(t *testing.T) {
			source := zonalNode("source", "zone-a")
			pod := withClaim(plannerPod("database", source.Name), "data")
			planner, client := plannerFixture(t, []*corev1.Node{source, zonalNode("destination", destinationZone)}, []*corev1.Pod{pod}, zonalVolume("data", "zone-a", false)...)
			result, err := planner.PlanDrain(context.Background(), &api.DrainRequest{Sources: []*api.NodeIdentity{identity(source, "source")}, DeadlineUnixMillis: time.Now().Add(2 * time.Minute).UnixMilli()})
			if err != nil {
				t.Fatal(err)
			}
			if destinationZone == "zone-b" && (!result.WaitForCapacity || len(result.Evictions) != 0) {
				t.Fatalf("drain was authorized with room only outside the volume zone: %+v", result)
			}
			if destinationZone == "zone-a" && (result.WaitForCapacity || len(result.Evictions) != 1 || result.Evictions[0].Uid != string(pod.UID)) {
				t.Fatalf("drain did not use same-zone room: %+v", result)
			}
			assertReadOnly(t, client)
		})
	}
}

func repeatabilityFixture(t *testing.T) (*Planner, *fake.Clientset, []*corev1.Node, []*corev1.Pod) {
	t.Helper()
	nodes := []*corev1.Node{zonalNode("source", "zone-a"), zonalNode("same-zone", "zone-a"), zonalNode("other-zone", "zone-b"), zonalNode("spare", "zone-a")}
	pods := []*corev1.Pod{withClaim(plannerPod("database", "source"), "data"), plannerPod("web-1", "source"), plannerPod("web-2", "same-zone"),
		plannerPod("web-3", "other-zone"), plannerPod("batch", "spare")}
	pods[1].Labels = map[string]string{"app": "web"}
	pods[2].Labels = map[string]string{"app": "web"}
	pods[3].Labels = map[string]string{"app": "web"}
	// Leave equal free room in both zones and almost none on the spare Node, so
	// the Pending Pod's placement decides whether the zonal database still fits.
	// A correct planner must make that choice identically on every call.
	pods[4].Spec.Containers[0].Resources.Requests[corev1.ResourceCPU] = resource.MustParse("1900m")
	pods[4].Spec.Containers[0].Resources.Limits[corev1.ResourceCPU] = resource.MustParse("1900m")
	pending := plannerPod("pending", "")
	pending.Status.Phase = corev1.PodPending
	pods = append(pods, pending)
	extra := append(zonalVolume("data", "zone-a", false), blockingBudget(pods[0]),
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default", UID: "web-budget-uid", ResourceVersion: "1"},
			Spec:   policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}},
			Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 1}})
	planner, client := plannerFixture(t, nodes, pods, extra...)
	publishGroups(t, planner, map[string]*corev1.Node{"preferred": zonalNode("preferred-template", "zone-b"), "fallback": zonalNode("fallback-template", "zone-a")}, nil, nil)
	return planner, client, nodes, pods
}

func TestPlanningIsRepeatableForUnchangedState(t *testing.T) {
	planner, client, nodes, _ := repeatabilityFixture(t)
	source := identity(nodes[0], "source")
	deadline := time.Now().Add(2 * time.Minute).UnixMilli()
	replacement := &api.ReplacementRequest{Sources: []*api.NodeIdentity{source}, DeadlineUnixMillis: deadline, MaximumAdditionalNodes: 4, AllowedNodeGroupIds: []string{"preferred", "fallback"}}
	relocation := &api.RelocationRequest{Source: source, Destinations: []*api.NodeIdentity{identity(nodes[2], "target"), identity(nodes[1], "target")}}
	drain := &api.DrainRequest{Sources: []*api.NodeIdentity{source}, DeadlineUnixMillis: deadline}

	var baseline string
	var plan *api.ReplacementResponse
	var explanation *api.RelocationResponse
	var gate *api.DrainResponse
	for i := 0; i < 25; i++ {
		replaced, err := planner.PlanReplacement(context.Background(), replacement)
		if err != nil {
			t.Fatal(err)
		}
		explained, err := planner.ExplainRelocation(context.Background(), relocation)
		if err != nil {
			t.Fatal(err)
		}
		drained, err := planner.PlanDrain(context.Background(), drain)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			baseline, plan, explanation, gate = replaced.SnapshotId, replaced, explained, drained
			if baseline == "" || !plan.Covered {
				t.Fatalf("fixture did not produce a covered plan: %+v", plan)
			}
		}
		// One unchanged cluster state has one identity across planning calls.
		if replaced.SnapshotId != baseline || explained.SnapshotId != baseline || drained.SnapshotId != baseline {
			t.Fatalf("iteration %d changed the snapshot identity: %s %s %s, expected %s", i, replaced.SnapshotId, explained.SnapshotId, drained.SnapshotId, baseline)
		}
		if !reflect.DeepEqual(additions(replaced), additions(plan)) || replaced.Covered != plan.Covered || replaced.UncoveredPods != plan.UncoveredPods {
			t.Fatalf("iteration %d changed the replacement plan: %+v, expected %+v", i, replaced, plan)
		}
		if explained.Placement != explanation.Placement || explained.PdbBlocked != explanation.PdbBlocked || !reflect.DeepEqual(podPlacements(explained), podPlacements(explanation)) ||
			!reflect.DeepEqual(sortedStrings(explained.UsedNodeUids), sortedStrings(explanation.UsedNodeUids)) {
			t.Fatalf("iteration %d changed the relocation explanation: %+v, expected %+v", i, explained, explanation)
		}
		if drained.WaitForCapacity != gate.WaitForCapacity || drained.PdbBlocked != gate.PdbBlocked || !reflect.DeepEqual(evictions(drained), evictions(gate)) {
			t.Fatalf("iteration %d changed the drain decision: %+v, expected %+v", i, drained, gate)
		}
	}
	// The zonal database can only be relocated into its volume zone.
	if !slices.ContainsFunc(explanation.Pods, func(pod *api.PodPlacement) bool {
		return pod.Pod.Name == "database" && pod.DestinationNodeUid == string(nodes[1].UID)
	}) {
		t.Fatalf("zonal workload was not relocated into its volume zone: %+v", explanation)
	}
	assertReadOnly(t, client)
}

func TestSnapshotIdentityTracksPlanningInputs(t *testing.T) {
	for _, change := range []string{"pod", "budget", "node"} {
		t.Run(change, func(t *testing.T) {
			planner, client, nodes, pods := repeatabilityFixture(t)
			drain := &api.DrainRequest{Sources: []*api.NodeIdentity{identity(nodes[0], "source")}, DeadlineUnixMillis: time.Now().Add(2 * time.Minute).UnixMilli()}
			before, err := planner.PlanDrain(context.Background(), drain)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			switch change {
			case "pod":
				updated := pods[3].DeepCopy()
				updated.ResourceVersion = "2"
				if _, err := client.CoreV1().Pods("default").Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				eventually(t, func() bool {
					current, err := planner.listers.AllPodLister().List()
					return err == nil && slices.ContainsFunc(current, func(pod *corev1.Pod) bool { return pod.Name == updated.Name && pod.ResourceVersion == "2" })
				})
			case "budget":
				budget, err := client.PolicyV1().PodDisruptionBudgets("default").Get(ctx, "web", metav1.GetOptions{})
				if err != nil {
					t.Fatal(err)
				}
				budget = budget.DeepCopy()
				budget.ResourceVersion, budget.Status.DisruptionsAllowed = "2", 0
				if _, err := client.PolicyV1().PodDisruptionBudgets("default").UpdateStatus(ctx, budget, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				eventually(t, func() bool {
					current, err := planner.listers.PodDisruptionBudgetLister().List()
					return err == nil && slices.ContainsFunc(current, func(item *policyv1.PodDisruptionBudget) bool {
						return item.Name == "web" && item.Status.DisruptionsAllowed == 0
					})
				})
			case "node":
				updated := nodes[3].DeepCopy()
				updated.ResourceVersion = "2"
				if _, err := client.CoreV1().Nodes().Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
					t.Fatal(err)
				}
				eventually(t, func() bool {
					current, err := planner.listers.AllNodeLister().List()
					return err == nil && slices.ContainsFunc(current, func(node *corev1.Node) bool { return node.Name == updated.Name && node.ResourceVersion == "2" })
				})
			}
			after, err := planner.PlanDrain(context.Background(), drain)
			if err != nil {
				t.Fatal(err)
			}
			if after.SnapshotId == before.SnapshotId {
				t.Fatalf("snapshot identity did not change after a %s update", change)
			}
		})
	}
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("informer did not observe the update")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func assertReadOnly(t *testing.T, client *fake.Clientset) {
	t.Helper()
	for _, action := range client.Actions() {
		if verb := action.GetVerb(); verb != "list" && verb != "watch" && verb != "get" {
			t.Fatalf("read-only planning issued %s %s", verb, action.GetResource().Resource)
		}
	}
}

func additions(response *api.ReplacementResponse) []string {
	result := []string{}
	for _, addition := range response.Additions {
		result = append(result, fmt.Sprintf("%s=%d", addition.NodeGroupId, addition.Count))
	}
	return result
}

func podPlacements(response *api.RelocationResponse) []string {
	result := []string{}
	for _, pod := range response.Pods {
		result = append(result, fmt.Sprintf("%s/%s:%s:%t:%s", pod.Pod.Namespace, pod.Pod.Name, pod.Placement, pod.PdbBlocked, pod.DestinationNodeUid))
	}
	return sortedStrings(result)
}

func evictions(response *api.DrainResponse) []string {
	result := []string{}
	for _, pod := range response.Evictions {
		result = append(result, pod.Namespace+"/"+pod.Name+":"+pod.Uid)
	}
	return sortedStrings(result)
}

func sortedStrings(values []string) []string {
	result := slices.Clone(values)
	slices.Sort(result)
	return result
}
