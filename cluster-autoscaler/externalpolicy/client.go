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
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ca "k8s.io/autoscaler/cluster-autoscaler/context"
	api "k8s.io/autoscaler/cluster-autoscaler/externalpolicy/protos"
	"k8s.io/autoscaler/cluster-autoscaler/processors/deletion"
	"k8s.io/autoscaler/cluster-autoscaler/simulator"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/drainability/rules"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/framework"
	"k8s.io/autoscaler/cluster-autoscaler/simulator/options"
	"k8s.io/autoscaler/cluster-autoscaler/utils/drain"
	caerrors "k8s.io/autoscaler/cluster-autoscaler/utils/errors"
	"k8s.io/klog/v2"
)

// Client is both a pre-simulation processor and a post-budget authorizer.
// Exclusions never remove a Node from the destination set.
type Client struct {
	config    Config
	selector  labels.Selector
	rpc       api.ScaleDownPolicyClient
	conn      *grpc.ClientConn
	timeout   time.Duration
	mu        sync.Mutex
	snapshot  string
	revision  string
	ordered   time.Time
	permitted map[string]*api.NodeIdentity
	rejected  map[string]time.Time
}

func NewClient(config Config) (*Client, error) {
	tlsConfig, err := clientTLS(config)
	if err != nil {
		return nil, err
	}
	connection, err := grpc.NewClient(config.Endpoint, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	if err != nil {
		return nil, err
	}
	selector, err := labels.Parse(config.ManagedNodeSelector)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	timeout, err := time.ParseDuration(config.Timeout)
	if err != nil || timeout <= 0 {
		_ = connection.Close()
		return nil, fmt.Errorf("invalid external policy timeout")
	}
	return &Client{config: config, selector: selector, rpc: api.NewScaleDownPolicyClient(connection), conn: connection, timeout: timeout, rejected: map[string]time.Time{}}, nil
}

func (c *Client) managed(node *corev1.Node) bool { return c.selector.Matches(labels.Set(node.Labels)) }

func identity(node *corev1.Node, group string) *api.NodeIdentity {
	return &api.NodeIdentity{Name: node.Name, Uid: string(node.UID), ProviderId: node.Spec.ProviderID, NodeGroupId: group, ResourceVersion: node.ResourceVersion, BootId: node.Status.NodeInfo.BootID, Labels: node.Labels, SystemUuid: node.Status.NodeInfo.SystemUUID}
}

func snapshotID(nodes []*corev1.Node) string {
	hash := sha256.New()
	sorted := slices.Clone(nodes)
	slices.SortFunc(sorted, func(a, b *corev1.Node) int { return strings.Compare(a.Name, b.Name) })
	for _, node := range sorted {
		_, _ = fmt.Fprintf(hash, "%s\x00%s\x00%s\x00", node.Name, node.UID, node.ResourceVersion)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func (c *Client) GetPodDestinationCandidates(_ *ca.AutoscalingContext, input []*corev1.Node) ([]*corev1.Node, caerrors.AutoscalerError) {
	return input, nil
}

func (c *Client) GetScaleDownCandidates(state *ca.AutoscalingContext, input []*corev1.Node) ([]*corev1.Node, caerrors.AutoscalerError) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.snapshot, c.revision, c.ordered = "", "", time.Time{}
	c.permitted = map[string]*api.NodeIdentity{}
	now := time.Now()
	for uid, expiry := range c.rejected {
		if !expiry.After(now) {
			delete(c.rejected, uid)
		}
	}
	request := &api.OrderCandidatesRequest{SnapshotId: snapshotID(input), ObservedAtUnixMillis: now.UnixMilli()}
	managed := map[string]*corev1.Node{}
	unmanaged := []*corev1.Node{}
	for _, node := range input {
		if !c.managed(node) {
			unmanaged = append(unmanaged, node)
			continue
		}
		uid := string(node.UID)
		if uid == "" || c.rejected[uid].After(now) {
			continue
		}
		group, err := state.CloudProvider.NodeGroupForNode(node)
		if err != nil || group == nil {
			continue
		}
		candidate := &api.Candidate{Node: identity(node, group.Id())}
		if info, err := state.ClusterSnapshot.GetNodeInfo(node.Name); err == nil {
			protected := false
			for _, pod := range info.Pods() {
				protected = protected || c.protectedWork(pod.Pod)
			}
			if protected && c.config.Mode != "observe" {
				continue
			}
			deleteOptions := options.NewNodeDeleteOptions(state.AutoscalingOptions)
			pods, _, _, err := simulator.GetPodsToMove(info, deleteOptions, rules.Default(deleteOptions), state.ListerRegistry, state.RemainingPdbTracker, now)
			candidate.Empty = err == nil && len(pods) == 0
		}
		managed[uid] = node
		request.Candidates = append(request.Candidates, candidate)
	}
	if len(managed) == 0 {
		if c.config.Mode == "observe" {
			return input, nil
		}
		return unmanaged, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	response, err := c.rpc.OrderCandidates(ctx, request)
	var ordered []*corev1.Node
	if err == nil {
		ordered, err = validateOrder(request, response, managed)
	}
	if err != nil {
		klog.Warningf("External scale-down policy unavailable; deferring managed candidates: %v", err)
		if c.config.Mode == "observe" {
			return input, nil
		}
		return unmanaged, nil
	}
	c.snapshot, c.revision, c.ordered = request.SnapshotId, response.PolicyRevision, now
	for _, node := range ordered {
		c.permitted[string(node.UID)] = identity(node, "")
	}
	klog.V(2).Infof("External scale-down policy: mode=%s candidates=%d allowed=%d excluded=%d", c.config.Mode, len(managed), len(ordered), len(response.Exclusions))
	if c.config.Mode == "observe" {
		return input, nil
	}
	// Preserve the native positions/order of unmanaged nodes. Replace managed
	// slots with the policy's ordered subsequence; excluded slots disappear.
	result, next := []*corev1.Node{}, 0
	for _, node := range input {
		if !c.managed(node) {
			result = append(result, node)
		} else if next < len(ordered) {
			result = append(result, ordered[next])
			next++
		}
	}
	return result, nil
}

func (c *Client) protectedWork(pod *corev1.Pod) bool {
	if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed || pod.Annotations["cluster-autoscaler.kubernetes.io/safe-to-evict"] == "true" {
		return false
	}
	owner := metav1.GetControllerOf(pod)
	if owner == nil {
		return false
	}
	key := owner.Kind
	if group, _, found := strings.Cut(owner.APIVersion, "/"); found {
		key = group + "/" + key
	}
	return slices.Contains(c.config.ProtectedControllers, key)
}

// Name and Drainable also install protection in native simulation and the
// actual drain, where Pods may have arrived after the loop's initial snapshot.
func (c *Client) Name() string { return "ProtectedController" }
func (c *Client) Drainable(_ *drainability.DrainContext, pod *corev1.Pod, node *framework.NodeInfo) drainability.Status {
	if c.config.Mode == "enforce" && c.managed(node.Node()) && c.protectedWork(pod) {
		return drainability.NewBlockedStatus(drain.ProtectedController, fmt.Errorf("unfinished pod %s/%s belongs to a protected controller", pod.Namespace, pod.Name))
	}
	return drainability.NewUndefinedStatus()
}

func validateOrder(request *api.OrderCandidatesRequest, response *api.OrderCandidatesResponse, known map[string]*corev1.Node) ([]*corev1.Node, error) {
	if response == nil || response.SnapshotId != request.SnapshotId || response.PolicyRevision == "" {
		return nil, fmt.Errorf("response has no matching snapshot or policy revision")
	}
	seen := map[string]bool{}
	result := []*corev1.Node{}
	for _, uid := range response.OrderedNodeUids {
		if known[uid] == nil || seen[uid] {
			return nil, fmt.Errorf("unknown or duplicate candidate identity")
		}
		seen[uid] = true
		result = append(result, known[uid])
	}
	for _, exclusion := range response.Exclusions {
		if exclusion == nil || known[exclusion.NodeUid] == nil || seen[exclusion.NodeUid] || exclusion.Reason == "" {
			return nil, fmt.Errorf("invalid candidate exclusion")
		}
		seen[exclusion.NodeUid] = true
	}
	if len(seen) != len(known) {
		return nil, fmt.Errorf("response omitted a managed candidate")
	}
	return result, nil
}

// AuthorizeDeletion sees the exact native post-budget batch. A late rejection
// defers that entire batch; it never silently changes the simulated selection.
func (c *Client) AuthorizeDeletion(state *ca.AutoscalingContext, batches []deletion.Batch, force bool) (bool, string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	request := &api.ApproveRemovalsRequest{SnapshotId: c.snapshot, PolicyRevision: c.revision, ObservedAtUnixMillis: c.ordered.UnixMilli(), Force: force}
	managed := []*corev1.Node{}
	for _, batch := range batches {
		view := &api.RemovalBatch{NodeGroupId: batch.NodeGroupID, Empty: batch.Empty, Atomic: batch.Atomic}
		for _, node := range batch.Nodes {
			view.Nodes = append(view.Nodes, identity(node, batch.NodeGroupID))
			if c.managed(node) {
				managed = append(managed, node)
			}
		}
		request.Batches = append(request.Batches, view)
	}
	if len(managed) == 0 {
		return true, ""
	}
	allowed, reason := false, "no fresh external policy decision"
	identitiesMatch := true
	for _, node := range managed {
		previous := c.permitted[string(node.UID)]
		if previous == nil || previous.Name != node.Name || previous.ProviderId != node.Spec.ProviderID || previous.BootId != node.Status.NodeInfo.BootID || previous.ResourceVersion != node.ResourceVersion {
			identitiesMatch = false
			reason = "removal identity differs from the permitted simulation candidates"
		}
	}
	if identitiesMatch && c.snapshot != "" && time.Since(c.ordered) < time.Minute {
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		response, err := c.rpc.ApproveRemovals(ctx, request)
		if err != nil {
			reason = "external policy unavailable: " + err.Error()
		} else if response == nil || response.SnapshotId != request.SnapshotId || response.PolicyRevision != request.PolicyRevision {
			reason = "external policy response is stale or incomplete"
		} else {
			allowed, reason = response.Approved, response.Reason
		}
	}
	klog.V(2).Infof("External removal authorization: mode=%s allowed=%t reason=%q", c.config.Mode, allowed, reason)
	if c.config.Mode == "observe" {
		return true, ""
	}
	if !allowed {
		for _, node := range managed {
			recheck := state.UnremovableNodeRecheckTimeout
			if recheck <= 0 {
				recheck = 5 * time.Minute
			}
			c.rejected[string(node.UID)] = time.Now().Add(recheck)
			state.LogRecorder.Eventf(corev1.EventTypeNormal, "ScaleDownDeferred", "External policy deferred node %s: %s", node.Name, reason)
		}
	}
	return allowed, reason
}

func (c *Client) CleanUp() {
	if c.conn != nil {
		_ = c.conn.Close()
	}
}
