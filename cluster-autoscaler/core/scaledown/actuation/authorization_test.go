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

package actuation

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	testprovider "k8s.io/autoscaler/cluster-autoscaler/cloudprovider/test"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	ca "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/deletiontracker"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/status"
	coretest "k8s.io/autoscaler/cluster-autoscaler/core/test"
	"k8s.io/autoscaler/cluster-autoscaler/observers/nodegroupchange"
	"k8s.io/autoscaler/cluster-autoscaler/processors/deletion"
	"k8s.io/autoscaler/cluster-autoscaler/processors/nodegroupconfig"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	kubeutil "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	utiltest "k8s.io/autoscaler/cluster-autoscaler/utils/test"
	"k8s.io/client-go/kubernetes/fake"
)

type recordingAuthorizer struct {
	batches []deletion.Batch
	force   bool
}

func (r *recordingAuthorizer) AuthorizeDeletion(_ *ca.AutoscalingContext, batches []deletion.Batch, force bool) (bool, string) {
	r.batches, r.force = batches, force
	return false, "capacity retained"
}

func TestAuthorizationRunsAfterBudgetBeforeAnySideEffect(t *testing.T) {
	for _, atomic := range []bool{false, true} {
		for _, force := range []bool{false, true} {
			client := fake.NewSimpleClientset()
			provider := testprovider.NewTestCloudProviderBuilder().Build()
			provider.AddNodeGroupWithCustomOptions("workers", 0, 10, 2, &config.NodeGroupAutoscalingOptions{ZeroOrMaxNodeScaling: atomic})
			input := []*corev1.Node{}
			for _, name := range []string{"first", "second"} {
				node := utiltest.BuildTestNode(name, 2000, 8*1024*1024*1024)
				node.UID = types.UID(name + "-uid")
				provider.AddNode("workers", node)
				input = append(input, node)
			}
			registry := kubeutil.NewListerRegistry(nil, nil, kubeutil.NewTestPodLister(nil), kubeutil.NewTestPodDisruptionBudgetLister(nil), nil, nil, nil, nil, nil)
			state, err := coretest.NewScaleTestAutoscalingContext(config.AutoscalingOptions{MaxScaleDownParallelism: 1, MaxDrainParallelism: 1}, client, registry, provider, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			authorizer := &recordingAuthorizer{}
			deleteOptions := options.NewNodeDeleteOptions(state.AutoscalingOptions)
			actuator := NewActuator(&state, nodegroupchange.NewNodeGroupChangeObserversList(), deletiontracker.NewNodeDeletionTracker(0), deleteOptions, rules.Default(deleteOptions), nodegroupconfig.NewDefaultNodeGroupConfigProcessor(state.NodeGroupDefaults), authorizer)
			client.ClearActions()
			var result status.ScaleDownResult
			if force {
				result, _, err = actuator.StartForceDeletion(input, nil)
			} else {
				result, _, err = actuator.StartDeletion(input, nil)
			}
			if err != nil || result != status.ScaleDownNoNodeDeleted || len(authorizer.batches) != 1 || authorizer.force != force {
				t.Fatalf("authorization was skipped or reported as a cloud error: %v %v %+v", result, err, authorizer)
			}
			want := 1
			if atomic {
				want = 2
			}
			if len(authorizer.batches[0].Nodes) != want || authorizer.batches[0].Atomic != atomic {
				t.Fatal("policy saw pre-budget nodes or a partial atomic group")
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatalf("rejected batch had a side effect: %v", action)
				}
			}
		}
	}
}
