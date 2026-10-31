# External capacity policy (proposal)

This optional integration separates capacity policy from Kubernetes scheduling
semantics. An external policy ranks capacity and protects reservations. Cluster
Autoscaler retains predicates, estimation, disruption checks and node deletion.
All integrations are disabled without an explicit configuration file.

The existing gRPC expander chooses among native expansion options. Two proposed
scale-down calls add ordering before simulation and synchronous, all-or-nothing
authorization of the actual removal batch after deletion budgets. Authorization
cannot add nodes to the batch or bypass a native constraint. An unavailable or
invalid policy defers voluntary removal of enrolled nodes. Enrollment is known
locally through a configured Node label selector.

The read-only planner supports impending node loss without waiting for Pods to
become Pending. It builds an independent scheduling snapshot, virtually removes
the source placements, retains unavailable source nodes as topology domains,
and uses the native binpacking estimator. Its response is an estimate, not a
capacity grant: callers still enforce native group limits, track accepted
requests and revalidate before acting. Incoming nodes are charged once.
All simultaneous losses must be supplied together; independent per-node calls
cannot reserve the same destination room. Permitted node-group IDs are required.
Native backoff, group health, node and resource limits apply to additions. A
NotReady or disappearing node remains a topology domain but never a destination,
even for Pods with blanket tolerations. This conservative topology model can
temporarily require more room under hard topology spread.

Relocation explanations distinguish disruption restrictions from scheduling
feasibility. Ignoring PDBs for a diagnostic simulation never authorizes an
eviction. Drain plans identify Pods and their original UIDs; actual eviction
must use the Kubernetes Eviction API and its live PDB checks.
Per-Pod results distinguish positively compatible work from hard incompatibility
and unknown or currently full destinations. Multiple explanations are independent
estimates, not additive reservations of destination space. Controller-less Pods
remain visible, matching native drained-node demand; a plan does not promise that
a workload controller will recreate them.

Policy responses are bound to a snapshot and Node UIDs. Unknown identities,
incomplete decisions and stale policy revisions fail closed for enrolled
retirement. Observation mode reports decisions while preserving native
behavior. The extension does not replace the cloud provider or infer that
equally ranked node groups are interchangeable scheduling destinations.
Late rejection defers the whole batch (including any co-batched unmanaged nodes),
then excludes its managed nodes for the native unremovable recheck interval. This
allows unmanaged contraction to continue without altering the validated batch.

Both services require TLS. The planner requires a trusted client certificate;
the policy client supports an optional client certificate. Keep service trust
scoped and apply network access policy. The policy expander must answer from
cached state: never hold a policy-state lock across a planner call, because
planning may call the expander back. The native expander and the planner share
a serialized strategy and its existing timeout. A planner response is advisory
and has no provider or Kubernetes write side effects.

The protocol is experimental. Generic APIs and focused commits are kept
separate from downstream deployment configuration to support upstream review.
