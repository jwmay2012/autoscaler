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
	"testing"
	"time"

	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	ca "k8s.io/autoscaler/cluster-autoscaler/context"
	coretest "k8s.io/autoscaler/cluster-autoscaler/core/test"
	api "k8s.io/autoscaler/cluster-autoscaler/externalpolicy/protos"
	"k8s.io/autoscaler/cluster-autoscaler/processors/deletion"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	kubeutil "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	utiltest "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/kubernetes/fake"
)

type policyStub struct {
	order   func(context.Context, *api.OrderCandidatesRequest) (*api.OrderCandidatesResponse, error)
	approve func(context.Context, *api.ApproveRemovalsRequest) (*api.ApproveRemovalsResponse, error)
}

func (p policyStub) OrderCandidates(ctx context.Context, request *api.OrderCandidatesRequest, _ ...grpc.CallOption) (*api.OrderCandidatesResponse, error) {
	return p.order(ctx, request)
}
func (p policyStub) ApproveRemovals(ctx context.Context, request *api.ApproveRemovalsRequest, _ ...grpc.CallOption) (*api.ApproveRemovalsResponse, error) {
	return p.approve(ctx, request)
}

func clientFixture(t *testing.T) (*Client, *ca.AutoscalingContext, []*corev1.Node) {
	t.Helper()
	provider := testprovider.NewTestCloudProviderBuilder().Build()
	provider.AddNodeGroup("workers", 0, 10, 3)
	registry := kubeutil.NewListerRegistry(nil, nil, kubeutil.NewTestPodLister(nil), kubeutil.NewTestPodDisruptionBudgetLister(nil), nil, nil, nil, nil, nil)
	state, err := coretest.NewScaleTestAutoscalingContext(config.AutoscalingOptions{}, fake.NewSimpleClientset(), registry, provider, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	nodes := []*corev1.Node{}
	for _, name := range []string{"first", "unmanaged", "second"} {
		node := utiltest.BuildTestNode(name, 2000, 8*1024*1024*1024)
		node.UID, node.ResourceVersion = types.UID(name+"-uid"), "1"
		if name != "unmanaged" {
			node.Labels["example.com/managed"] = "true"
		}
		provider.AddNode("workers", node)
		nodes = append(nodes, node)
	}
	if err := state.ClusterSnapshot.SetClusterState(nodes, nil, nil); err != nil {
		t.Fatal(err)
	}
	selector, _ := labels.Parse("example.com/managed=true")
	stub := policyStub{
		order: func(_ context.Context, request *api.OrderCandidatesRequest) (*api.OrderCandidatesResponse, error) {
			return &api.OrderCandidatesResponse{SnapshotId: request.SnapshotId, PolicyRevision: "policy-1", OrderedNodeUids: []string{"second-uid", "first-uid"}}, nil
		},
		approve: func(_ context.Context, request *api.ApproveRemovalsRequest) (*api.ApproveRemovalsResponse, error) {
			return &api.ApproveRemovalsResponse{SnapshotId: request.SnapshotId, PolicyRevision: request.PolicyRevision, Approved: true}, nil
		},
	}
	return &Client{config: Config{Mode: "enforce"}, selector: selector, rpc: stub, timeout: time.Second, rejected: map[string]time.Time{}}, &state, nodes
}

func TestOrderPreservesUnmanagedPositionsAndDestinations(t *testing.T) {
	client, state, input := clientFixture(t)
	ordered, err := client.GetScaleDownCandidates(state, input)
	if err != nil || len(ordered) != 3 || ordered[0].Name != "second" || ordered[1].Name != "unmanaged" || ordered[2].Name != "first" {
		t.Fatalf("incorrect policy order: %v %v", ordered, err)
	}
	stub := client.rpc.(policyStub)
	stub.order = func(_ context.Context, request *api.OrderCandidatesRequest) (*api.OrderCandidatesResponse, error) {
		return &api.OrderCandidatesResponse{SnapshotId: request.SnapshotId, PolicyRevision: "policy-2", OrderedNodeUids: []string{"second-uid"}, Exclusions: []*api.Exclusion{{NodeUid: "first-uid", Reason: "reserved"}}}, nil
	}
	client.rpc = stub
	ordered, err = client.GetScaleDownCandidates(state, input)
	if err != nil || len(ordered) != 2 {
		t.Fatalf("exclusion failed: %v %v", ordered, err)
	}
	destinations, _ := client.GetPodDestinationCandidates(state, input)
	if len(destinations) != 3 || destinations[0].Name != "first" {
		t.Fatal("an excluded source disappeared as a scheduling destination")
	}
}

func TestInvalidPolicyDefersManagedNodesButObservationIsInert(t *testing.T) {
	for _, mode := range []string{"observe", "enforce"} {
		for _, failure := range []string{"error", "snapshot", "duplicate", "omission", "unknown"} {
			t.Run(mode+"/"+failure, func(t *testing.T) {
				client, state, input := clientFixture(t)
				client.config.Mode = mode
				stub := client.rpc.(policyStub)
				base := stub.order
				stub.order = func(ctx context.Context, request *api.OrderCandidatesRequest) (*api.OrderCandidatesResponse, error) {
					response, _ := base(ctx, request)
					switch failure {
					case "error":
						return nil, fmt.Errorf("unavailable")
					case "snapshot":
						response.SnapshotId = "old"
					case "duplicate":
						response.OrderedNodeUids = []string{"first-uid", "first-uid"}
					case "omission":
						response.OrderedNodeUids = nil
					case "unknown":
						response.OrderedNodeUids = []string{"new-uid", "second-uid"}
					}
					return response, nil
				}
				client.rpc = stub
				result, err := client.GetScaleDownCandidates(state, input)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "observe" {
					if len(result) != len(input) || result[0] != input[0] {
						t.Fatal("observation altered the native decision")
					}
				} else if len(result) != 1 || result[0].Name != "unmanaged" {
					t.Fatal("invalid policy failed open for managed nodes or blocked unmanaged nodes")
				}
			})
		}
	}
}

func TestRemovalAuthorizationIsBoundToRevisionAndFinalBatch(t *testing.T) {
	client, state, input := clientFixture(t)
	if _, err := client.GetScaleDownCandidates(state, input); err != nil {
		t.Fatal(err)
	}
	stub := client.rpc.(policyStub)
	stub.approve = func(_ context.Context, request *api.ApproveRemovalsRequest) (*api.ApproveRemovalsResponse, error) {
		if len(request.Batches) != 1 || len(request.Batches[0].Nodes) != 1 || request.Batches[0].Nodes[0].Uid != "first-uid" {
			t.Fatal("authorization did not receive the exact supplied batch")
		}
		return &api.ApproveRemovalsResponse{SnapshotId: request.SnapshotId, PolicyRevision: "old-policy", Approved: true}, nil
	}
	client.rpc = stub
	batch := []deletion.Batch{{NodeGroupID: "workers", Nodes: input[:1], Empty: true}}
	if allowed, _ := client.AuthorizeDeletion(state, batch, false); allowed {
		t.Fatal("stale authorization was accepted")
	}
	if !client.rejected["first-uid"].After(time.Now()) {
		t.Fatal("late rejection was not retained for the next simulation")
	}
	client.config.Mode = "observe"
	if allowed, _ := client.AuthorizeDeletion(state, batch, false); !allowed {
		t.Fatal("observation blocked native deletion")
	}
	client.config.Mode = "enforce"
	if allowed, _ := client.AuthorizeDeletion(state, []deletion.Batch{{Nodes: input[1:2]}}, false); !allowed {
		t.Fatal("unmanaged deletion called the policy")
	}
}

func TestManagedWorkProtectionIsOptionalAndDoesNotAffectNoticePlanning(t *testing.T) {
	client, state, nodes := clientFixture(t)
	client.config.ProtectedControllers = []string{"batch/Job"}
	pod := utiltest.BuildTestPod("job", 100, 100*1024*1024)
	pod.Spec.Containers[0].Resources.Limits = pod.Spec.Containers[0].Resources.Requests.DeepCopy()
	pod.Spec.NodeName = nodes[0].Name
	controller := true
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "work", Controller: &controller}}
	if err := state.ClusterSnapshot.SetClusterState(nodes, []*corev1.Pod{pod}, nil); err != nil {
		t.Fatal(err)
	}
	stub := client.rpc.(policyStub)
	stub.order = func(_ context.Context, request *api.OrderCandidatesRequest) (*api.OrderCandidatesResponse, error) {
		response := &api.OrderCandidatesResponse{SnapshotId: request.SnapshotId, PolicyRevision: "policy"}
		for _, candidate := range request.Candidates {
			response.OrderedNodeUids = append(response.OrderedNodeUids, candidate.Node.Uid)
		}
		return response, nil
	}
	client.rpc = stub
	client.config.Mode = "enforce"
	if result := client.Drainable(nil, pod, framework.NewTestNodeInfo(nodes[0])); result.Outcome != drainability.BlockDrain {
		t.Fatal("actual drain did not recheck newly arrived work")
	}
	if result := client.Drainable(nil, pod, framework.NewTestNodeInfo(nodes[1])); result.Outcome != drainability.UndefinedOutcome {
		t.Fatal("work protection changed unmanaged behavior")
	}
	for _, mode := range []string{"enforce", "observe"} {
		client.config.Mode = mode
		result, err := client.GetScaleDownCandidates(state, nodes)
		want := 3
		if mode == "enforce" {
			want = 2
		}
		if err != nil || len(result) != want {
			t.Fatalf("work protection or observation changed: %+v %v", result, err)
		}
	}
	for _, phase := range []corev1.PodPhase{corev1.PodSucceeded, corev1.PodFailed} {
		pod.Status.Phase = phase
		if client.protectedWork(pod) {
			t.Fatal("completed work remained protected")
		}
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Annotations = map[string]string{"cluster-autoscaler.kubernetes.io/safe-to-evict": "true"}
	if client.protectedWork(pod) {
		t.Fatal("explicit eviction allowance was ignored")
	}
}
