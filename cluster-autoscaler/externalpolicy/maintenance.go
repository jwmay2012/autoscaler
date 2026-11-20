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
	"time"

	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	api "k8s.io/autoscaler/cluster-autoscaler/externalpolicy/protos"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
)

// Planned retirement uses the same voluntary-disruption rules as scale-down.
// No deadline may turn a blocked Job, PDB, or missing destination into a grant.
func (p *Planner) maintenanceRestrictions(ctx context.Context, state *planningState) ([]string, bool, bool, error) {
	deleteOptions := options.NewNodeDeleteOptions(p.options)
	drainRules := append(append(rules.Rules(nil), p.extraRules...), rules.Default(deleteOptions)...)
	reasons := []string{}
	for _, source := range state.sources {
		if err := ctx.Err(); err != nil {
			return nil, false, false, status.FromContextError(err).Err()
		}
		info := framework.NewNodeInfo(state.nodes[source.Name].DeepCopy(), nil)
		for _, pod := range state.moving {
			if state.origins[string(pod.UID)] == source.Name {
				info.AddPod(&framework.PodInfo{Pod: pod})
			}
		}
		_, _, _, err := simulator.GetPodsToMove(info, deleteOptions, drainRules, p.listers, nil, time.Now())
		if err != nil {
			reasons = append(reasons, err.Error())
		}
	}
	otherBlocked := len(reasons) > 0
	allowed, _, _ := state.pdbs.CanRemovePods(state.moving)
	if !allowed {
		reasons = append(reasons, "PodDisruptionBudget blocks planned maintenance")
	}
	return reasons, !allowed, otherBlocked, nil
}

func (p *Planner) maintenanceDrain(ctx context.Context, state *planningState) (*api.DrainResponse, error) {
	reasons, pdbBlocked, otherBlocked, err := p.maintenanceRestrictions(ctx, state)
	if err != nil {
		return nil, err
	}
	response := &api.DrainResponse{SnapshotId: state.id, PdbBlocked: pdbBlocked, OtherDisruptionBlocked: otherBlocked, Reasons: reasons}
	if _, _, err := unscheduled(state.snapshot, state.pending, scheduling.ScheduleAnywhere); err != nil {
		return nil, err
	}
	remaining, _, err := unscheduled(state.snapshot, state.moving, scheduling.ScheduleAnywhere)
	if err != nil {
		return nil, err
	}
	response.WaitForCapacity = len(reasons) != 0 || len(remaining) != 0
	if len(remaining) != 0 {
		response.Reasons = append(response.Reasons, "planned maintenance requires usable capacity on the supplied destinations")
	}
	for _, source := range state.sources {
		response.Nodes = append(response.Nodes, &api.NodeDrainStatus{NodeUid: source.Uid, Waiting: response.WaitForCapacity})
	}
	if !response.WaitForCapacity {
		for _, pod := range state.moving {
			// Native node drain may proceed sequentially under a PDB. Report
			// only its currently permitted Pod batch; the caller's Eviction
			// API remains authoritative while replacements become Ready.
			if allowed, _, _ := state.pdbs.CanRemovePods([]*corev1.Pod{pod}); allowed {
				state.pdbs.RemovePods([]*corev1.Pod{pod})
				response.Evictions = append(response.Evictions, &api.PodReference{Namespace: pod.Namespace, Name: pod.Name, Uid: string(pod.UID), ResourceVersion: pod.ResourceVersion})
			}
		}
	}
	return response, nil
}
