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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	api "k8s.io/autoscaler/cluster-autoscaler/externalpolicy/protos"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	"k8s.io/autoscaler/cluster-autoscaler/utils/drain"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/volumebinding"
)

func (p *Planner) ExplainRelocation(ctx context.Context, request *api.RelocationRequest) (*api.RelocationResponse, error) {
	done, err := p.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	state, err := p.state(request.Sources, nil, false)
	if err != nil {
		return nil, err
	}
	response := &api.RelocationResponse{SnapshotId: state.id, Placement: api.PlacementResult_PLACEMENT_UNKNOWN}
	destinations := map[string]bool{}
	for _, destination := range request.Destinations {
		if err := matchingNode(destination, state.nodes); err != nil {
			return nil, err
		}
		if slices.ContainsFunc(state.sources, func(source *api.NodeIdentity) bool { return destination.Uid == source.Uid }) {
			return nil, status.Error(codes.InvalidArgument, "source cannot be its own destination")
		}
		destinations[destination.Name] = true
	}
	if len(destinations) == 0 {
		return nil, status.Error(codes.InvalidArgument, "destination identities are required")
	}
	// Evaluate native disruption rules separately from placement. The source
	// was virtually emptied, so use its original Pod set for drainability.
	deleteOptions := options.NewNodeDeleteOptions(p.options)
	drainRules := append(append(rules.Rules(nil), p.extraRules...), rules.Default(deleteOptions)...)
	otherBlocked := map[string]bool{}
	for _, source := range state.sources {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		info := framework.NewNodeInfo(state.nodes[source.Name].DeepCopy(), nil)
		for _, pod := range state.moving {
			if state.origins[string(pod.UID)] == source.Name {
				info.AddPod(&framework.PodInfo{Pod: pod})
			}
		}
		_, _, blocker, disruptionErr := simulator.GetPodsToMove(info, deleteOptions, drainRules, p.listers, state.pdbs, time.Now())
		if disruptionErr != nil {
			response.OtherDisruptionBlocked = response.OtherDisruptionBlocked || blocker == nil || blocker.Reason != drain.NotEnoughPdb
			response.Reasons = append(response.Reasons, disruptionErr.Error())
		}
		// An early PDB rejection can hide another disruption restriction.
		_, _, _, otherErr := simulator.GetPodsToMove(info, deleteOptions, drainRules, p.listers, nil, time.Now())
		if otherErr != nil {
			response.OtherDisruptionBlocked = true
			otherBlocked[source.Name] = true
			response.Reasons = append(response.Reasons, otherErr.Error())
		}
	}
	state.snapshot.Fork()
	remaining, placed, err := unscheduled(state.snapshot, state.moving, func(node *framework.NodeInfo) bool { return ctx.Err() == nil && destinations[node.Node().Name] })
	state.snapshot.Revert()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err != nil {
		response.Reasons = append(response.Reasons, err.Error())
		return response, nil
	}
	response.Placement = api.PlacementResult_PLACEMENT_INFEASIBLE
	if len(remaining) == 0 {
		response.Placement = api.PlacementResult_PLACEMENT_FEASIBLE
	} else {
		response.Reasons = append(response.Reasons, "the complete source workload does not currently fit on the supplied destinations")
	}
	// Only native hard-constraint failures prove incompatibility. A capacity
	// shortage or an unknown predicate must not be presented as permanent
	// ineligibility to a policy deciding whether to retain scarce capacity.
	allIncompatible := len(state.moving) > 0
	for _, pod := range state.moving {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		item := &api.PodPlacement{Pod: &api.PodReference{Namespace: pod.Namespace, Name: pod.Name, Uid: string(pod.UID), ResourceVersion: pod.ResourceVersion}, Placement: api.PlacementResult_PLACEMENT_UNKNOWN}
		item.SourceNodeUid = string(state.nodes[state.origins[string(pod.UID)]].UID)
		item.OtherDisruptionBlocked = otherBlocked[state.origins[string(pod.UID)]]
		allowed, _, _ := state.pdbs.CanRemovePods([]*corev1.Pod{pod})
		item.PdbBlocked = !allowed
		response.PdbBlocked = response.PdbBlocked || item.PdbBlocked
		// This describes what is blocked now, not a request to evict the whole
		// diagnostic batch simultaneously. Only PlanDrain spends its PDB budget.
		for _, assignment := range placed {
			if assignment.Pod.UID == pod.UID {
				item.Placement = api.PlacementResult_PLACEMENT_FEASIBLE
				item.DestinationNodeUid = string(state.nodes[assignment.NodeName].UID)
				if !slices.Contains(response.UsedNodeUids, item.DestinationNodeUid) {
					response.UsedNodeUids = append(response.UsedNodeUids, item.DestinationNodeUid)
				}
			}
		}
		incompatibleEverywhere := true
		for name := range destinations {
			if err := ctx.Err(); err != nil {
				return nil, status.FromContextError(err).Err()
			}
			failure := state.snapshot.CheckPredicates(pod, name)
			if failure != nil && failure.FailingPredicateName() == "TaintToleration" {
				// Transient lifecycle taints do not prove that the offering is
				// unsuitable. Remain unknown until native fit can evaluate it.
				node := state.nodes[name]
				if len(taints.SanitizeTaints(node.Spec.Taints, taints.NewTaintConfig(p.options))) != len(node.Spec.Taints) {
					incompatibleEverywhere = false
					break
				}
			}
			if !hardMismatch(failure) {
				incompatibleEverywhere = false
				break
			}
		}
		if incompatibleEverywhere {
			item.Placement = api.PlacementResult_PLACEMENT_INCOMPATIBLE
		}
		allIncompatible = allIncompatible && incompatibleEverywhere
		response.Pods = append(response.Pods, item)
	}
	if allIncompatible {
		response.Placement = api.PlacementResult_PLACEMENT_INCOMPATIBLE
	}
	return response, nil
}

func hardMismatch(failure clustersnapshot.SchedulingError) bool {
	if failure == nil || failure.Type() != clustersnapshot.FailingPredicateError {
		return false
	}
	switch failure.FailingPredicateName() {
	case "NodeAffinity", "NodeName", "TaintToleration", "VolumeZone":
		return true
	case "VolumeBinding":
		return slices.Contains(failure.FailingPredicateReasons(), string(volumebinding.ErrReasonNodeConflict))
	default:
		return false
	}
}

func (p *Planner) PlanDrain(ctx context.Context, request *api.DrainRequest) (*api.DrainResponse, error) {
	done, err := p.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if request.PlannedMaintenance && (request.DeadlineUnixMillis != 0 || len(request.SourceDeadlinesUnixMillis) != 0) {
		return nil, status.Error(codes.InvalidArgument, "planned maintenance must not have loss deadlines")
	}
	if !request.PlannedMaintenance && request.DeadlineUnixMillis <= 0 {
		return nil, status.Error(codes.InvalidArgument, "an absolute node-loss deadline is required")
	}
	if !request.PlannedMaintenance && len(request.Destinations) != 0 {
		return nil, status.Error(codes.InvalidArgument, "destination restrictions require planned maintenance")
	}
	state, err := p.state(request.Sources, request.Destinations, request.PlannedMaintenance)
	if err != nil {
		return nil, err
	}
	if request.PlannedMaintenance {
		return p.maintenanceDrain(ctx, state)
	}
	response := &api.DrainResponse{SnapshotId: state.id}
	decisions := map[string]*api.NodeDrainStatus{}
	for _, source := range state.sources {
		deadline := request.DeadlineUnixMillis
		if specific, exists := request.SourceDeadlinesUnixMillis[source.Uid]; exists {
			if specific <= 0 {
				return nil, status.Error(codes.InvalidArgument, "invalid source loss deadline")
			}
			deadline = specific
		}
		cutoff := time.UnixMilli(deadline).Add(-state.graceBySource[source.Name] - 5*time.Second)
		decision := &api.NodeDrainStatus{NodeUid: source.Uid, LatestDrainStartUnixMillis: cutoff.UnixMilli(), DeadlineReached: !time.Now().Before(cutoff)}
		decisions[source.Name] = decision
		response.Nodes = append(response.Nodes, decision)
		response.DeadlineReached = response.DeadlineReached || decision.DeadlineReached
		if response.LatestDrainStartUnixMillis == 0 || cutoff.UnixMilli() < response.LatestDrainStartUnixMillis {
			response.LatestDrainStartUnixMillis = cutoff.UnixMilli()
		}
	}
	// This gate uses only real, Ready destinations. Upcoming capacity cannot
	// authorize eviction, even if it covered the earlier replacement estimate.
	if _, _, err := unscheduled(state.snapshot, state.pending, scheduling.ScheduleAnywhere); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	remaining, _, err := unscheduled(state.snapshot, state.moving, scheduling.ScheduleAnywhere)
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	response.ReplacementCapacityReady = len(remaining) == 0
	if !response.ReplacementCapacityReady {
		response.WaitForCapacity = true
		response.Reasons = append(response.Reasons, "replacement room is not yet Ready and schedulable")
		for _, decision := range decisions {
			decision.Waiting = true
		}
		return response, nil
	}
	for _, pod := range state.moving {
		decision := decisions[state.origins[string(pod.UID)]]
		allowed, _, _ := state.pdbs.CanRemovePods([]*corev1.Pod{pod})
		if !allowed {
			response.PdbBlocked = true
			decision.Waiting = true
			continue
		}
		state.pdbs.RemovePods([]*corev1.Pod{pod})
		response.Evictions = append(response.Evictions, &api.PodReference{Namespace: pod.Namespace, Name: pod.Name, Uid: string(pod.UID), ResourceVersion: pod.ResourceVersion})
	}
	if response.PdbBlocked {
		response.Reasons = append(response.Reasons, "PodDisruptionBudget currently blocks part of the drain")
	}
	return response, nil
}
