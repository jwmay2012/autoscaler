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
	"slices"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	ca "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	api "k8s.io/autoscaler/cluster-autoscaler/externalpolicy/protos"
	"k8s.io/autoscaler/cluster-autoscaler/observers/planning"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	utiltest "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
)

type preferGroup string

func (p preferGroup) BestOption(options []expander.Option, _ map[string]*framework.NodeInfo) *expander.Option {
	for i := range options {
		if options[i].NodeGroup.Id() == string(p) {
			return &options[i]
		}
	}
	if len(options) == 0 {
		return nil
	}
	return &options[0]
}

func TestUnavailableNodesNeverSatisfyBlanketTolerantDisplacement(t *testing.T) {
	for _, mode := range []string{"source", "unready", "deleting", "startup", "cordoned"} {
		t.Run(mode, func(t *testing.T) {
			source := plannerNode("source")
			pod := plannerPod("work", source.Name)
			pod.Spec.Tolerations = []corev1.Toleration{{Operator: corev1.TolerationOpExists}}
			nodes := []*corev1.Node{source}
			if mode != "source" {
				other := plannerNode("other")
				switch mode {
				case "unready":
					utiltest.SetNodeReadyState(other, false, time.Now())
				case "deleting":
					other.Spec.Taints = []corev1.Taint{{Key: taints.ToBeDeletedTaint, Effect: corev1.TaintEffectNoSchedule}}
				case "startup":
					other.Spec.Taints = []corev1.Taint{{Key: taints.StartupTaintPrefix + "initializing", Effect: corev1.TaintEffectNoSchedule}}
				case "cordoned":
					other.Spec.Unschedulable = true
				}
				nodes = append(nodes, other)
			}
			planner, _ := plannerFixture(t, nodes, []*corev1.Pod{pod})
			publishGroups(t, planner, map[string]*corev1.Node{"preferred": plannerNode("template")}, nil, nil)
			request := replacementRequest(source)
			result, err := planner.PlanReplacement(context.Background(), request)
			if err != nil || !result.Covered || len(result.Additions) != 1 || result.Additions[0].Count != 1 {
				t.Fatalf("unavailable room suppressed replacement: %+v %v", result, err)
			}
			drain, err := planner.PlanDrain(context.Background(), &api.DrainRequest{Sources: request.Sources, DeadlineUnixMillis: request.DeadlineUnixMillis})
			if err != nil || !drain.WaitForCapacity || len(drain.Evictions) != 0 {
				t.Fatalf("unavailable room authorized drain: %+v %v", drain, err)
			}
		})
	}
}

func TestReplacementHonorsNativeSafetyAndLimits(t *testing.T) {
	for _, mode := range []string{"backoff", "unsafe-incoming", "node-cap", "resource-cap", "pending-resource-cap"} {
		t.Run(mode, func(t *testing.T) {
			source := plannerNode("source")
			planner, _ := plannerFixture(t, []*corev1.Node{source}, []*corev1.Pod{plannerPod("work", source.Name)})
			publishGroups(t, planner, map[string]*corev1.Node{"preferred": plannerNode("preferred-template"), "fallback": plannerNode("fallback-template")}, map[string]int{"preferred": 1}, nil)
			request := replacementRequest(source)
			if mode == "backoff" || mode == "unsafe-incoming" {
				planner.view.groups["preferred"].safe = false
				if mode == "backoff" {
					planner.view.upcoming = nil
				}
			} else {
				planner.view.upcoming = nil
				if mode == "node-cap" {
					planner.options.MaxNodesTotal = 1
				} else {
					for _, group := range planner.view.groups {
						group.delta = map[string]int64{"cpu": 2}
					}
					planner.view.resources = map[string]int64{"cpu": 1}
					if mode == "pending-resource-cap" {
						planner.view.resources["cpu"] = 2
						request.PendingTargetSizes = map[string]int32{"preferred": 1}
						planner.view.groups["preferred"].safe = false
					}
				}
			}
			result, err := planner.PlanReplacement(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "backoff" || mode == "unsafe-incoming" {
				if !result.Covered || len(result.Additions) != 1 || result.Additions[0].NodeGroupId != "fallback" {
					t.Fatalf("unsafe group was counted or selected: %+v", result)
				}
			} else if result.Covered || len(result.Additions) != 0 {
				t.Fatalf("native limit bypassed: %+v", result)
			}
		})
	}
}

func TestRegisteredUpcomingBecomingReadyIsNotCountedTwice(t *testing.T) {
	source, ready := plannerNode("source"), plannerNode("ready")
	pods := []*corev1.Pod{plannerPod("first", source.Name), plannerPod("second", source.Name), plannerPod("third", source.Name)}
	planner, _ := plannerFixture(t, []*corev1.Node{source, ready}, pods)
	publishGroups(t, planner, map[string]*corev1.Node{"preferred": plannerNode("template")}, map[string]int{"preferred": 1}, map[string][]string{"preferred": {ready.Name}})
	result, err := planner.PlanReplacement(context.Background(), replacementRequest(source))
	if err != nil || !result.Covered || len(result.Additions) != 1 || result.Additions[0].Count != 1 {
		t.Fatalf("Ready node and its stale upcoming placeholder were both credited: %+v %v", result, err)
	}
}

func TestConcurrentNoticesUseOneCombinedPlan(t *testing.T) {
	a, b := plannerNode("a"), plannerNode("b")
	pods := []*corev1.Pod{plannerPod("first", a.Name), plannerPod("second", b.Name)}
	for i, pod := range pods {
		pod.Spec.NodeSelector = map[string]string{"example.com/class": []string{"a", "b"}[i]}
	}
	planner, _ := plannerFixture(t, []*corev1.Node{a, b}, pods)
	x, y := plannerNode("x"), plannerNode("y")
	x.Labels["example.com/class"], y.Labels["example.com/class"] = "a", "b"
	publishGroups(t, planner, map[string]*corev1.Node{"preferred": x, "fallback": y}, nil, nil)
	request := replacementRequest(a)
	request.Sources = append(request.Sources, identity(b, "source"))
	result, err := planner.PlanReplacement(context.Background(), request)
	if err != nil || !result.Covered || len(result.Additions) != 2 {
		t.Fatalf("mixed simultaneous losses were not jointly planned: %+v %v", result, err)
	}
	request.PendingTargetSizes = map[string]int32{"preferred": 1, "fallback": 1}
	second, err := planner.PlanReplacement(context.Background(), request)
	if err != nil || !second.Covered || len(second.Additions) != 0 {
		t.Fatalf("accepted joint plan was duplicated: %+v %v", second, err)
	}
}

func TestRelocationPreservesCompatiblePDBWorkAndUnknownTransientState(t *testing.T) {
	for _, mode := range []string{"mixed", "transient", "not-safe-to-evict"} {
		t.Run(mode, func(t *testing.T) {
			source, destination := plannerNode("source"), plannerNode("destination")
			pod := plannerPod("allowed", source.Name)
			pods := []*corev1.Pod{pod}
			switch mode {
			case "mixed":
				destination.Spec.Taints = []corev1.Taint{{Key: "example.com/spot", Effect: corev1.TaintEffectNoSchedule}}
				pod.Spec.Tolerations = []corev1.Toleration{{Key: "example.com/spot", Operator: corev1.TolerationOpExists}}
				pods = append(pods, plannerPod("incompatible", source.Name))
			case "transient":
				destination.Spec.Taints = []corev1.Taint{{Key: taints.StatusTaintPrefix + "initializing", Effect: corev1.TaintEffectNoSchedule}}
			case "not-safe-to-evict":
				pod.Annotations = map[string]string{"cluster-autoscaler.kubernetes.io/safe-to-evict": "false"}
			}
			budget := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "protect", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: pod.Labels}}, Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0}}
			planner, _ := plannerFixture(t, []*corev1.Node{source, destination}, pods, budget)
			result, err := planner.ExplainRelocation(context.Background(), &api.RelocationRequest{Source: identity(source, "source"), Destinations: []*api.NodeIdentity{identity(destination, "target")}})
			if err != nil || !result.PdbBlocked || result.Placement == api.PlacementResult_PLACEMENT_INCOMPATIBLE {
				t.Fatalf("compatible or uncertain work lost retention: %+v %v", result, err)
			}
			if mode == "mixed" && (!slices.ContainsFunc(result.Pods, func(p *api.PodPlacement) bool {
				return p.Pod.Name == "allowed" && p.PdbBlocked && p.Placement == api.PlacementResult_PLACEMENT_FEASIBLE
			}) || len(result.UsedNodeUids) != 1) {
				t.Fatalf("mixed work was not classified separately: %+v", result)
			}
			if mode == "not-safe-to-evict" && !result.OtherDisruptionBlocked {
				t.Fatal("PDB hid another disruption restriction")
			}
		})
	}
}

func plannerNode(name string) *corev1.Node {
	node := utiltest.BuildTestNode(name, 2000, 8*1024*1024*1024)
	node.UID, node.ResourceVersion = types.UID(name+"-uid"), "1"
	node.Spec.ProviderID = "provider://" + name
	node.Status.NodeInfo.BootID = "boot-1"
	node.Labels["example.com/managed"] = "true"
	node.Labels[corev1.LabelTopologyZone] = "zone-a"
	utiltest.SetNodeReadyState(node, true, time.Now().Add(-time.Minute))
	return node
}

func plannerPod(name, node string) *corev1.Pod {
	pod := utiltest.BuildTestPod(name, 800, 128*1024*1024)
	pod.Namespace, pod.Spec.NodeName = "default", node
	pod.UID, pod.ResourceVersion = types.UID(name+"-uid"), "1"
	pod.Labels = map[string]string{"app": "example"}
	pod.Status.Phase = corev1.PodRunning
	pod.Spec.Containers[0].Resources.Limits = pod.Spec.Containers[0].Resources.Requests.DeepCopy()
	controller := true
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "example", UID: "owner-uid", Controller: &controller}}
	grace := int64(30)
	pod.Spec.TerminationGracePeriodSeconds = &grace
	return pod
}

func plannerFixture(t *testing.T, nodes []*corev1.Node, pods []*corev1.Pod, extra ...runtime.Object) (*Planner, *fake.Clientset) {
	t.Helper()
	objects := []runtime.Object{&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "default"}}}
	for _, node := range nodes {
		objects = append(objects, node)
	}
	for _, pod := range pods {
		objects = append(objects, pod)
	}
	objects = append(objects, extra...)
	client := fake.NewSimpleClientset(objects...)
	factory := informers.NewSharedInformerFactory(client, 0)
	plan, err := NewPlanner(factory, config.AutoscalingOptions{ScanInterval: 10 * time.Second}, "example.com/managed=true")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	factory.Start(ctx.Done())
	for _, synced := range factory.WaitForCacheSync(ctx.Done()) {
		if !synced {
			t.Fatal("informer failed to synchronize")
		}
	}
	return plan, client
}

func publishGroups(t *testing.T, planner *Planner, templates map[string]*corev1.Node, upcoming map[string]int, registered map[string][]string) {
	t.Helper()
	provider := testprovider.NewTestCloudProviderBuilder().WithOnScaleUp(func(string, int) error { t.Fatal("planner mutated provider"); return nil }).Build()
	infos := map[string]*framework.NodeInfo{}
	safe := map[string]bool{}
	for name, node := range templates {
		provider.AddNodeGroup(name, 0, 4, 0)
		infos[name] = framework.NewTestNodeInfo(node)
		safe[name] = true
	}
	planner.Observe(&ca.AutoscalingContext{CloudProvider: provider, ExpanderStrategy: preferGroup("preferred")}, planning.Snapshot{Templates: infos, Upcoming: upcoming, Registered: registered, SafeToScale: safe, ObservedAt: time.Now()})
}

func replacementRequest(source *corev1.Node) *api.ReplacementRequest {
	return &api.ReplacementRequest{Sources: []*api.NodeIdentity{identity(source, "source")}, DeadlineUnixMillis: time.Now().Add(2 * time.Minute).UnixMilli(), MaximumAdditionalNodes: 4, AllowedNodeGroupIds: []string{"preferred", "fallback"}}
}

func TestReplacementUsesNativeConstraintsAndRemovesOriginalPlacement(t *testing.T) {
	for _, constraint := range []string{"anti-affinity", "taint", "selector", "resource"} {
		t.Run(constraint, func(t *testing.T) {
			source := plannerNode("source")
			pod := plannerPod("work", source.Name)
			preferred, fallback := plannerNode("preferred-template"), plannerNode("fallback-template")
			want := "preferred"
			switch constraint {
			case "anti-affinity":
				pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{LabelSelector: &metav1.LabelSelector{MatchLabels: pod.Labels}, TopologyKey: corev1.LabelTopologyZone}}}}
			case "taint":
				preferred.Spec.Taints = []corev1.Taint{{Key: "example.com/spot", Value: "true", Effect: corev1.TaintEffectNoSchedule}}
				want = "fallback"
			case "selector":
				preferred.Labels["example.com/type"], fallback.Labels["example.com/type"] = "other", "required"
				pod.Spec.NodeSelector = map[string]string{"example.com/type": "required"}
				want = "fallback"
			case "resource":
				preferred.Status.Capacity = utiltest.BuildTestNode("small", 500, 8*1024*1024*1024).Status.Capacity
				preferred.Status.Allocatable = preferred.Status.Capacity.DeepCopy()
				want = "fallback"
			}
			planner, client := plannerFixture(t, []*corev1.Node{source}, []*corev1.Pod{pod})
			publishGroups(t, planner, map[string]*corev1.Node{"preferred": preferred, "fallback": fallback}, nil, nil)
			result, err := planner.PlanReplacement(context.Background(), replacementRequest(source))
			if err != nil || !result.Covered || len(result.Additions) != 1 || result.Additions[0].NodeGroupId != want || result.Additions[0].Count != 1 {
				t.Fatalf("incorrect native replacement plan: %+v %v", result, err)
			}
			actual, _ := client.CoreV1().Pods("default").Get(context.Background(), pod.Name, metav1.GetOptions{})
			if actual.Spec.NodeName != source.Name || actual.DeletionTimestamp != nil {
				t.Fatal("virtual loss changed the real Pod")
			}
		})
	}
}

func TestReplacementAccountsForExistingAndIncomingRoomOnce(t *testing.T) {
	for _, room := range []string{"existing", "upcoming", "accepted-target", "lost-upcoming-source", "bounded"} {
		t.Run(room, func(t *testing.T) {
			source := plannerNode("source")
			nodes := []*corev1.Node{source}
			if room == "existing" {
				nodes = append(nodes, plannerNode("ready"))
			}
			planner, _ := plannerFixture(t, nodes, []*corev1.Pod{plannerPod("work", source.Name)})
			upcoming, registered := map[string]int{}, map[string][]string{}
			request := replacementRequest(source)
			if room == "upcoming" {
				upcoming["preferred"] = 1
			}
			if room == "accepted-target" {
				request.PendingTargetSizes = map[string]int32{"preferred": 1}
			}
			if room == "lost-upcoming-source" {
				upcoming["preferred"], registered["preferred"] = 1, []string{source.Name}
			}
			if room == "bounded" {
				request.MaximumAdditionalNodes = 0
			}
			publishGroups(t, planner, map[string]*corev1.Node{"preferred": plannerNode("template")}, upcoming, registered)
			result, err := planner.PlanReplacement(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if room == "bounded" {
				if result.Covered || len(result.Additions) != 0 || result.UncoveredPods != 1 {
					t.Fatalf("budget was bypassed: %+v", result)
				}
			} else if room == "lost-upcoming-source" {
				if !result.Covered || len(result.Additions) != 1 {
					t.Fatalf("the doomed source satisfied its own replacement: %+v", result)
				}
			} else if !result.Covered || len(result.Additions) != 0 {
				t.Fatalf("existing supply was counted twice: %+v", result)
			}
		})
	}
}

func TestRelocationDistinguishesPDBFromHardPlacement(t *testing.T) {
	for _, tainted := range []bool{false, true} {
		source, destination := plannerNode("source"), plannerNode("destination")
		if tainted {
			destination.Spec.Taints = []corev1.Taint{{Key: "example.com/spot", Effect: corev1.TaintEffectNoSchedule}}
		}
		pod := plannerPod("work", source.Name)
		budget := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "protect", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: pod.Labels}}, Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0}}
		planner, _ := plannerFixture(t, []*corev1.Node{source, destination}, []*corev1.Pod{pod}, budget)
		request := &api.RelocationRequest{Source: identity(source, "source"), Destinations: []*api.NodeIdentity{identity(destination, "target")}}
		result, err := planner.ExplainRelocation(context.Background(), request)
		want := api.PlacementResult_PLACEMENT_FEASIBLE
		if tainted {
			want = api.PlacementResult_PLACEMENT_INCOMPATIBLE
		}
		if err != nil || !result.PdbBlocked || result.OtherDisruptionBlocked || result.Placement != want {
			t.Fatalf("PDB and placement were conflated: %+v %v", result, err)
		}
		if !tainted && (len(result.UsedNodeUids) != 1 || result.UsedNodeUids[0] != string(destination.UID)) {
			t.Fatal("explanation did not identify the required retained capacity")
		}
	}
}

func TestDrainWaitsForRealCapacityAndNeverOverridesPDB(t *testing.T) {
	for _, mode := range []string{"waiting", "ready", "deadline", "pdb"} {
		t.Run(mode, func(t *testing.T) {
			source := plannerNode("source")
			nodes := []*corev1.Node{source}
			if mode == "ready" {
				nodes = append(nodes, plannerNode("destination"))
			}
			pod := plannerPod("work", source.Name)
			extra := []runtime.Object{}
			if mode == "pdb" {
				extra = append(extra, &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: "protect", Namespace: "default"}, Spec: policyv1.PodDisruptionBudgetSpec{Selector: &metav1.LabelSelector{MatchLabels: pod.Labels}}, Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0}})
			}
			planner, client := plannerFixture(t, nodes, []*corev1.Pod{pod}, extra...)
			deadline := time.Now().Add(2 * time.Minute)
			if mode == "deadline" || mode == "pdb" {
				deadline = time.Now().Add(20 * time.Second)
			}
			result, err := planner.PlanDrain(context.Background(), &api.DrainRequest{Sources: []*api.NodeIdentity{identity(source, "source")}, DeadlineUnixMillis: deadline.UnixMilli()})
			if err != nil || result.WaitForCapacity != (mode == "waiting") || result.PdbBlocked != (mode == "pdb") {
				t.Fatalf("incorrect drain gate: %+v %v", result, err)
			}
			want := 1
			if mode == "waiting" || mode == "pdb" {
				want = 0
			}
			if len(result.Evictions) != want {
				t.Fatalf("incorrect eviction plan: %+v", result)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "list" && action.GetVerb() != "watch" {
					t.Fatal("read-only planner mutated the workload cluster")
				}
			}
		})
	}
}
