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
	"crypto/sha256"
	"fmt"
	"maps"
	"net"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/autoscaler/cluster-autoscaler/cloudprovider"
	"k8s.io/autoscaler/cluster-autoscaler/config"
	ca "k8s.io/autoscaler/cluster-autoscaler/context"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaledown/pdb"
	"k8s.io/autoscaler/cluster-autoscaler/core/scaleup/resource"
	"k8s.io/autoscaler/cluster-autoscaler/estimator"
	"k8s.io/autoscaler/cluster-autoscaler/expander"
	api "k8s.io/autoscaler/cluster-autoscaler/externalpolicy/protos"
	"k8s.io/autoscaler/cluster-autoscaler/observers/planning"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/predicate"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/clustersnapshot/store"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/scheduling"
	kubeutil "k8s.io/autoscaler/cluster-autoscaler/utils/kubernetes"
	podutil "k8s.io/autoscaler/cluster-autoscaler/utils/pod"
	"k8s.io/autoscaler/cluster-autoscaler/utils/taints"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// Planner never uses the main loop's mutable scheduler handle or snapshot.
// Its own handle is serialized, while provider publications are immutable.
type Planner struct {
	api.UnimplementedCapacityPlannerServer
	gate      chan struct{}
	mu        sync.RWMutex
	view      providerView
	framework *framework.Handle
	listers   kubeutil.ListerRegistry
	synced    func() bool
	selector  labels.Selector
	options   config.AutoscalingOptions
	server    *grpc.Server
}

type providerView struct {
	groups      map[string]*planningGroup
	upcoming    map[string]int
	registered  map[string][]string
	observed    time.Time
	expander    expander.Strategy
	resources   map[string]int64
	limitsError string
	pdbs        []*policyv1.PodDisruptionBudget
}

type planningState struct {
	snapshot    clustersnapshot.ClusterSnapshot
	nodes       map[string]*corev1.Node
	moving      []*corev1.Pod
	pending     []*corev1.Pod
	pdbs        pdb.RemainingPdbTracker
	id          string
	sources     []*api.NodeIdentity
	grace       time.Duration
	unavailable map[string]bool
}

func NewPlanner(factory informers.SharedInformerFactory, options config.AutoscalingOptions, selector string) (*Planner, error) {
	if options.DynamicResourceAllocationEnabled {
		return nil, fmt.Errorf("external planner does not yet support DRA snapshots")
	}
	handle, err := framework.NewHandle(factory, options.SchedulerConfig, false)
	if err != nil {
		return nil, err
	}
	managed, err := labels.Parse(selector)
	if err != nil {
		return nil, err
	}
	listers := kubeutil.NewListerRegistryWithDefaultListers(factory)
	syncs := []cache.InformerSynced{factory.Core().V1().Nodes().Informer().HasSynced, factory.Core().V1().Pods().Informer().HasSynced,
		factory.Policy().V1().PodDisruptionBudgets().Informer().HasSynced, factory.Apps().V1().ReplicaSets().Informer().HasSynced,
		factory.Apps().V1().StatefulSets().Informer().HasSynced, factory.Batch().V1().Jobs().Informer().HasSynced,
		factory.Core().V1().ReplicationControllers().Informer().HasSynced, factory.Apps().V1().DaemonSets().Informer().HasSynced}
	return &Planner{gate: make(chan struct{}, 1), framework: handle, listers: listers, selector: managed, options: options,
		synced: func() bool {
			for _, ready := range syncs {
				if !ready() {
					return false
				}
			}
			return true
		}}, nil
}

func (p *Planner) Start(ctx context.Context, config PlannerConfig) error {
	tlsConfig, err := serverTLS(config)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", config.Listen)
	if err != nil {
		return err
	}
	p.server = grpc.NewServer(grpc.Creds(credentials.NewTLS(tlsConfig)), grpc.MaxConcurrentStreams(16), grpc.MaxRecvMsgSize(4<<20))
	api.RegisterCapacityPlannerServer(p.server, p)
	go func() {
		<-ctx.Done()
		p.server.Stop()
	}()
	go func() {
		if err := p.server.Serve(listener); err != nil && ctx.Err() == nil {
			klog.Errorf("Capacity planner server stopped: %v", err)
		}
	}()
	return nil
}

// Observe is called on the native loop, where provider access is already safe.
// RPC handlers receive only copies and cannot issue provider mutations.
func (p *Planner) Observe(state *ca.AutoscalingContext, input planning.Snapshot) {
	view := providerView{groups: map[string]*planningGroup{}, upcoming: map[string]int{}, registered: map[string][]string{}, observed: input.ObservedAt,
		expander: state.ExpanderStrategy, resources: maps.Clone(input.ResourcesLeft), limitsError: input.LimitsError}
	for _, budget := range input.PDBs {
		view.pdbs = append(view.pdbs, budget.DeepCopy())
	}
	for _, group := range state.CloudProvider.NodeGroups() {
		template := input.Templates[group.Id()]
		if template == nil {
			continue
		}
		target, err := group.TargetSize()
		if err != nil {
			continue
		}
		groupOptions, err := group.GetOptions(state.NodeGroupDefaults)
		if err != nil && err != cloudprovider.ErrNotImplemented {
			continue
		}
		if groupOptions == nil {
			defaults := state.NodeGroupDefaults
			groupOptions = &defaults
		}
		view.groups[group.Id()] = &planningGroup{id: group.Id(), minimum: group.MinSize(), maximum: group.MaxSize(), target: target, options: *groupOptions,
			template: template.DeepCopy(), safe: input.SafeToScale[group.Id()], delta: maps.Clone(input.ResourceDelta[group.Id()])}
		view.upcoming[group.Id()] = input.Upcoming[group.Id()]
		view.registered[group.Id()] = append([]string(nil), input.Registered[group.Id()]...)
	}
	p.mu.Lock()
	p.view = view
	p.mu.Unlock()
}

func (p *Planner) begin(ctx context.Context) (func(), error) {
	select {
	case p.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if p.synced != nil && !p.synced() {
		<-p.gate
		return nil, status.Error(codes.Unavailable, "planner caches have not synchronized")
	}
	return func() { <-p.gate }, nil
}

func (p *Planner) state(sources []*api.NodeIdentity) (*planningState, error) {
	if len(sources) == 0 || len(sources) > 100 {
		return nil, status.Error(codes.InvalidArgument, "one to one hundred source identities are required")
	}
	nodes, err := p.listers.AllNodeLister().List()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	pods, err := p.listers.AllPodLister().List()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	budgets, err := p.listers.PodDisruptionBudgetLister().List()
	if err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	// Preserve native in-flight disruption reservations where they are more
	// conservative than the fresh API status. Eviction still rechecks live PDBs.
	p.mu.RLock()
	view := p.view
	p.mu.RUnlock()
	if time.Since(view.observed) < max(30*time.Second, 3*p.options.ScanInterval) {
		for i, budget := range budgets {
			budgets[i] = budget.DeepCopy()
			for _, reserved := range view.pdbs {
				if reserved.UID == budget.UID && reserved.ResourceVersion == budget.ResourceVersion {
					budgets[i].Status.DisruptionsAllowed = min(budgets[i].Status.DisruptionsAllowed, reserved.Status.DisruptionsAllowed)
				}
			}
		}
	}
	result := &planningState{nodes: map[string]*corev1.Node{}, pdbs: pdb.NewBasicRemainingPdbTracker(), unavailable: map[string]bool{}}
	if err := result.pdbs.SetPdbs(budgets); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	for _, node := range nodes {
		result.nodes[node.Name] = node
	}
	lost := map[string]bool{}
	for _, source := range sources {
		if err := matchingNode(source, result.nodes); err != nil {
			return nil, err
		}
		node := result.nodes[source.Name]
		if !p.selector.Matches(labels.Set(node.Labels)) || lost[node.Name] {
			return nil, status.Error(codes.InvalidArgument, "source is not uniquely enrolled in the planner")
		}
		lost[node.Name] = true
		result.sources = append(result.sources, identity(node, source.NodeGroupId))
	}
	virtualNodes := make([]*corev1.Node, 0, len(nodes))
	for _, node := range nodes {
		copy := node.DeepCopy()
		if lost[node.Name] || !p.available(node) {
			result.unavailable[node.Name] = true
			copy.Spec.Unschedulable = true
		}
		if lost[node.Name] {
			copy.Spec.Taints = append(copy.Spec.Taints, corev1.Taint{Key: taints.ToBeDeletedTaint, Effect: corev1.TaintEffectNoSchedule})
		}
		virtualNodes = append(virtualNodes, copy)
	}
	scheduled := []*corev1.Pod{}
	hash := sha256.New()
	slices.SortFunc(pods, func(a, b *corev1.Pod) int { return strings.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name) })
	slices.SortFunc(budgets, func(a, b *policyv1.PodDisruptionBudget) int {
		return strings.Compare(a.Namespace+"/"+a.Name, b.Namespace+"/"+b.Name)
	})
	for _, budget := range budgets {
		_, _ = fmt.Fprintf(hash, "pdb:%s:%s:%d;", budget.UID, budget.ResourceVersion, budget.Status.DisruptionsAllowed)
	}
	for _, pod := range pods {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		_, _ = fmt.Fprintf(hash, "%s/%s:%s:%s;", pod.Namespace, pod.Name, pod.UID, pod.ResourceVersion)
		if pod.Spec.NodeName == "" {
			if pod.DeletionTimestamp == nil {
				result.pending = append(result.pending, pod.DeepCopy())
			}
			continue
		}
		if lost[pod.Spec.NodeName] && len(podutil.FilterRecreatablePods([]*corev1.Pod{pod})) > 0 {
			// Terminating source Pods are removed from the virtual snapshot too;
			// their real replacements, if needed, are already Pending demand.
			if pod.DeletionTimestamp == nil {
				copy := pod.DeepCopy()
				copy.Spec.NodeName = ""
				result.moving = append(result.moving, copy)
				grace := int64(30)
				if pod.Spec.TerminationGracePeriodSeconds != nil {
					grace = max(0, *pod.Spec.TerminationGracePeriodSeconds)
				}
				if p.options.MaxGracefulTerminationSec > 0 {
					grace = min(grace, int64(p.options.MaxGracefulTerminationSec))
				}
				result.grace = max(result.grace, time.Duration(grace)*time.Second)
			}
			continue
		}
		scheduled = append(scheduled, pod.DeepCopy())
	}
	result.id = fmt.Sprintf("%s-%x", snapshotID(nodes), hash.Sum(nil))
	result.snapshot = &destinationSnapshot{ClusterSnapshot: predicate.NewPredicateSnapshot(store.NewBasicSnapshotStore(), p.framework, false), unavailable: result.unavailable}
	if err := result.snapshot.SetClusterState(virtualNodes, scheduled, nil); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	return result, nil
}

func (p *Planner) available(node *corev1.Node) bool {
	if node == nil || node.DeletionTimestamp != nil || !kubeutil.IsNodeReadyAndSchedulable(node) || taints.HasToBeDeletedTaint(node) {
		return false
	}
	config := taints.NewTaintConfig(p.options)
	for _, taint := range node.Spec.Taints {
		if config.IsStartupTaint(taint.Key) || taint.Key == corev1.TaintNodeNotReady || taint.Key == corev1.TaintNodeUnreachable || taint.Key == "node.cloudprovider.kubernetes.io/uninitialized" {
			return false
		}
	}
	return true
}

func matchingNode(reference *api.NodeIdentity, nodes map[string]*corev1.Node) error {
	if reference == nil || reference.Name == "" || reference.Uid == "" || reference.ProviderId == "" || reference.BootId == "" {
		return status.Error(codes.InvalidArgument, "a complete source Node identity is required")
	}
	node := nodes[reference.Name]
	if node == nil || string(node.UID) != reference.Uid || node.Spec.ProviderID != reference.ProviderId || node.Status.NodeInfo.BootID != reference.BootId ||
		(reference.SystemUuid != "" && node.Status.NodeInfo.SystemUUID != reference.SystemUuid) || node.DeletionTimestamp != nil {
		return status.Error(codes.FailedPrecondition, "Node identity changed")
	}
	return nil
}

func addVirtualNodes(snapshot clustersnapshot.ClusterSnapshot, group *planningGroup, count int, prefix string) error {
	for i := 0; i < count; i++ {
		copy, err := simulator.SanitizedNodeInfo(group.template, fmt.Sprintf("external-%s-%d", prefix, i))
		if err != nil {
			return err
		}
		copy.Node().Spec.Unschedulable = false
		if err := snapshot.AddNodeInfo(copy); err != nil {
			return err
		}
	}
	return nil
}

func unscheduled(snapshot clustersnapshot.ClusterSnapshot, pods []*corev1.Pod, match func(*framework.NodeInfo) bool) ([]*corev1.Pod, []scheduling.Status, error) {
	placed, _, err := scheduling.NewHintingSimulator().TrySchedulePods(snapshot, pods, match, false)
	if err != nil {
		return nil, nil, err
	}
	seen := map[string]bool{}
	for _, item := range placed {
		seen[item.Pod.Namespace+"/"+item.Pod.Name] = true
	}
	remaining := []*corev1.Pod{}
	for _, pod := range pods {
		if !seen[pod.Namespace+"/"+pod.Name] {
			remaining = append(remaining, pod)
		}
	}
	return remaining, placed, nil
}

func (p *Planner) PlanReplacement(ctx context.Context, request *api.ReplacementRequest) (*api.ReplacementResponse, error) {
	done, err := p.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	if request.MaximumAdditionalNodes < 0 || request.MaximumAdditionalNodes > 1000 {
		return nil, status.Error(codes.InvalidArgument, "invalid additional-node bound")
	}
	if request.DeadlineUnixMillis <= 0 {
		return nil, status.Error(codes.InvalidArgument, "an absolute node-loss deadline is required")
	}
	if len(request.AllowedNodeGroupIds) == 0 {
		return nil, status.Error(codes.InvalidArgument, "explicit permitted node groups are required")
	}
	budgetFor, budgetLeft := map[string]int{}, map[int]int{}
	for i, budget := range request.Budgets {
		if budget == nil || budget.MaximumAdditionalNodes < 0 {
			return nil, status.Error(codes.InvalidArgument, "invalid shared expansion budget")
		}
		budgetLeft[i] = int(budget.MaximumAdditionalNodes)
		for _, id := range budget.NodeGroupIds {
			if _, duplicate := budgetFor[id]; duplicate || !slices.Contains(request.AllowedNodeGroupIds, id) {
				return nil, status.Error(codes.InvalidArgument, "shared budgets must partition the permitted node groups")
			}
			budgetFor[id] = i
		}
	}
	if len(request.Budgets) > 0 {
		for _, id := range request.AllowedNodeGroupIds {
			if _, exists := budgetFor[id]; !exists {
				return nil, status.Error(codes.InvalidArgument, "permitted node group has no shared budget")
			}
		}
	}
	state, err := p.state(request.Sources)
	if err != nil {
		return nil, err
	}
	p.mu.RLock()
	view := p.view
	p.mu.RUnlock()
	if view.expander == nil || time.Since(view.observed) > max(30*time.Second, 3*p.options.ScanInterval) {
		return nil, status.Error(codes.Unavailable, "provider templates are not fresh")
	}
	if view.limitsError != "" {
		return nil, status.Error(codes.Unavailable, "native cluster resource limits unavailable: "+view.limitsError)
	}
	response := &api.ReplacementResponse{SnapshotId: state.id, ObservedAtUnixMillis: time.Now().UnixMilli(), Sources: state.sources,
		LatestDrainStartUnixMillis: time.UnixMilli(request.DeadlineUnixMillis).Add(-state.grace - 5*time.Second).UnixMilli()}
	groups := map[string]*planningGroup{}
	resources := maps.Clone(view.resources)
	committed := len(state.nodes)
	for id, original := range view.groups {
		copy := *original
		groups[id] = &copy
		pending := max(0, int(request.PendingTargetSizes[id])-original.target)
		incoming := view.upcoming[id] + pending
		committed += max(0, incoming-len(view.registered[id]))
		chargeResources(resources, original.delta, pending)
		for _, name := range view.registered[id] {
			if p.available(state.nodes[name]) || slices.ContainsFunc(request.Sources, func(source *api.NodeIdentity) bool { return source.Name == name }) {
				incoming--
			}
		}
		if original.safe {
			if err := addVirtualNodes(state.snapshot, &copy, max(0, incoming), "incoming"); err != nil {
				return nil, status.Error(codes.Unavailable, err.Error())
			}
		}
		copy.target = max(copy.target, int(request.PendingTargetSizes[id]))
	}
	// Real Pending demand gets existing room before hypothetical displacement;
	// it is otherwise handled by the ordinary autoscaling loop.
	if _, _, err := unscheduled(state.snapshot, state.pending, scheduling.ScheduleAnywhere); err != nil {
		return nil, status.Error(codes.Unavailable, err.Error())
	}
	remaining := state.moving
	left := int(request.MaximumAdditionalNodes)
	if p.options.MaxNodesTotal > 0 {
		left = min(left, max(0, p.options.MaxNodesTotal-committed))
	}
	previous := len(remaining) + 1
	for len(remaining) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		remaining, _, err = unscheduled(state.snapshot, remaining, scheduling.ScheduleAnywhere)
		if err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		if len(remaining) == 0 || left == 0 {
			break
		}
		if len(remaining) >= previous {
			response.Reasons = append(response.Reasons, "native estimate made no scheduling progress; further expansion deferred")
			break
		}
		previous = len(remaining)
		options := []expander.Option{}
		templates := map[string]*framework.NodeInfo{}
		ids := append([]string(nil), request.AllowedNodeGroupIds...)
		sort.Strings(ids)
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return nil, status.FromContextError(err).Err()
			}
			group := groups[id]
			if group == nil || !group.safe {
				continue
			}
			limit := min(left, group.maximum-group.target)
			if budget, exists := budgetFor[id]; exists {
				limit = min(limit, budgetLeft[budget])
			}
			for key, delta := range group.delta {
				if available, limited := resources[key]; limited && delta > 0 {
					if available == resource.LimitUnknown {
						limit = 0
					} else {
						limit = min(limit, int(max(0, available)/delta))
					}
				}
			}
			if limit <= 0 || (group.options.ZeroOrMaxNodeScaling && limit < group.maximum-group.target) {
				continue
			}
			limiter := estimator.NewThresholdBasedEstimationLimiter([]estimator.Threshold{estimator.NewStaticThreshold(limit, time.Second)})
			packer := estimator.NewBinpackingNodeEstimator(state.snapshot, limiter, estimator.NewDecreasingPodOrderer(), estimator.NewEstimationContext(0, nil, len(state.nodes)), nil)
			// The native estimator expects prefiltered equivalence groups. Use
			// the actual scheduler predicates on one proposed template Node.
			state.snapshot.Fork()
			if err := addVirtualNodes(state.snapshot, group, 1, "eligibility"); err != nil {
				state.snapshot.Revert()
				return nil, status.Error(codes.Unavailable, err.Error())
			}
			templateName := group.template.Node().Name + "-external-eligibility-0"
			equivalence := []estimator.PodEquivalenceGroup{}
			for _, pod := range remaining {
				if err := state.snapshot.CheckPredicates(pod, templateName); err == nil {
					equivalence = append(equivalence, estimator.PodEquivalenceGroup{Pods: []*corev1.Pod{pod}})
				} else if err.Type() == clustersnapshot.SchedulingInternalError {
					state.snapshot.Revert()
					return nil, status.Error(codes.Unavailable, err.Error())
				}
			}
			state.snapshot.Revert()
			if len(equivalence) == 0 {
				continue
			}
			count, covered := packer.Estimate(equivalence, group.template, group)
			if count == 0 || len(covered) == 0 {
				continue
			}
			if group.options.ZeroOrMaxNodeScaling {
				count = group.maximum - group.target
			}
			options = append(options, expander.Option{NodeGroup: group, NodeCount: count, Pods: covered})
			templates[id] = group.template
		}
		if len(options) == 0 {
			response.Reasons = append(response.Reasons, "no permitted offering can cover the remaining displacement within its bounds")
			break
		}
		selected := view.expander.BestOption(options, templates)
		if selected == nil || selected.NodeCount <= 0 || selected.NodeCount > left {
			response.Reasons = append(response.Reasons, "capacity policy did not select an admissible expansion")
			break
		}
		group := groups[selected.NodeGroup.Id()]
		if group == nil || selected.NodeCount > group.maximum-group.target {
			return nil, status.Error(codes.FailedPrecondition, "expander returned an invalid option")
		}
		if budget, exists := budgetFor[group.id]; exists && selected.NodeCount > budgetLeft[budget] {
			return nil, status.Error(codes.FailedPrecondition, "expander exceeded a shared budget")
		}
		if err := addVirtualNodes(state.snapshot, group, selected.NodeCount, fmt.Sprintf("replacement-%d", len(response.Additions))); err != nil {
			return nil, status.Error(codes.Unavailable, err.Error())
		}
		response.Additions = append(response.Additions, &api.GroupAddition{NodeGroupId: group.id, Count: int32(selected.NodeCount)})
		group.target += selected.NodeCount
		if budget, exists := budgetFor[group.id]; exists {
			budgetLeft[budget] -= selected.NodeCount
		}
		chargeResources(resources, group.delta, selected.NodeCount)
		left -= selected.NodeCount
	}
	response.Covered, response.UncoveredPods = len(remaining) == 0, int32(len(remaining))
	if !response.Covered && left == 0 {
		response.Reasons = append(response.Reasons, "additional-node allowance or native cluster node limit reached")
	}
	return response, nil
}

func chargeResources(left map[string]int64, delta map[string]int64, nodes int) {
	for key, perNode := range delta {
		if remaining, exists := left[key]; exists && remaining != resource.LimitUnknown {
			left[key] = max(0, remaining-int64(nodes)*perNode)
		}
	}
}
