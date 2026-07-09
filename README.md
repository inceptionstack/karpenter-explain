# kexplain — an EXPLAIN plan for Karpenter

`kexplain` answers "**why did Karpenter give me *this* node?**" the way `EXPLAIN`
answers "why did the database run this query this way?" — after the fact, or
before it.

Karpenter's reasoning is scattered across ephemeral controller logs, NodeClaims
that get deleted with their nodes, and events that expire after an hour.
`kexplain` continuously harvests all three into a local store
(`~/.kexplain/<cluster>/`) so decisions remain explainable even after the node
is long gone.

## Requirements

- `kubectl` pointed at the cluster (Karpenter v1.x in `kube-system`)
- `aws` CLI (optional — used for spot prices and the `plan` command)
- Karpenter logging at `debug` level captures the richest traces
  (`--set logLevel=debug` on the helm chart), but `info` works too.
- Python 3 (stdlib only, no pip installs)

## Commands

```
kexplain nodes                # all karpenter nodes: live, disrupted, and gone
kexplain nodes --live         # only currently-running
kexplain history              # timeline of every provisioning/disruption decision
kexplain history --since 2    # last 2 hours
kexplain explain <node>       # full decision trace (node, nodeclaim, or instance id)
kexplain explain <node> --json
kexplain plan -f pod.yaml     # BEFORE the fact: which instance types could this pod get
kexplain sync                 # just harvest (any other command auto-syncs first)
```

Run `kexplain sync` periodically (cron/systemd timer) if you want history to
survive Karpenter pod restarts — logs only go back as far as the current +
previous container.

## What `explain` shows, per node

```
TRIGGER              which unschedulable pods started the provisioning loop —
                     or which consolidation command this node is a replacement for
CONSTRAINTS          NodePool requirements ∩ pod constraints → resolved NodeClaim
                     reqs, with "← narrowed by pod constraints" attribution
CANDIDATES           how many instance types survived filtering
FUNNEL               customer-funnel view: the EC2 type universe shrinking
                     stage by stage — each NodePool requirement, each
                     pod-injected constraint, then resource fit — with counts,
                     eliminated totals, and survivor names once the set is small
LAUNCH DECISION      what EC2 CreateFleet actually picked (type/capacity/zone/price)
                     + "why not cheaper?" spot price comparison
FEATURE ATTRIBUTION  premium features of the chosen type (NVMe, enhanced network,
                     latest gen): were they *required* by a constraint, or did
                     CreateFleet just pick them? Includes how to block them.
LIFECYCLE            created → launched → registered → initialized, with latencies
DISRUPTION           consolidation/drift/expiry decision that removed it,
                     with estimated $/hr savings
```

The FEATURE ATTRIBUTION section answers the classic "why do I keep getting
expensive NVMe / high-network / latest-gen instances I never asked for?" —
it distinguishes *your constraints demanded it* from *CreateFleet chose it*
(price-capacity-optimized weighs interruption risk, not just price, for spot).

This mirrors Karpenter's actual pipeline: pending pods → scheduling simulation
→ requirement intersection → instance-type filtering (price-ordered) →
CreateFleet with price-capacity-optimized (spot) or lowest-price (on-demand) →
node lifecycle → disruption controller.

## Example

```
$ kexplain explain ip-192-168-71-166.ec2.internal

NODE ip-192-168-71-166.ec2.internal  nodeclaim=default-abc123  [RUNNING]
├─ TRIGGER  @ 2026-07-09 10:15:02Z
│  └─ 5 unschedulable pod(s) could not fit on existing nodes:
│     ├─ default/inflate-5c8f9-xyz
│     └─ …
├─ CONSTRAINTS
│  ├─ NodePool default requirements:
│  │  ├─ karpenter.sh/capacity-type in [spot, on-demand]
│  │  └─ karpenter.k8s.aws/instance-category in [c, m, r]
│  └─ Aggregated resource requests: cpu=5150m memory=5Gi
├─ CANDIDATES
│  └─ 142 instance types satisfied all constraints
├─ FUNNEL  (recomputed from the live EC2 catalog)
│  ├─ ██████████████████████████████ 1348  EC2 instance types in region
│  ├─ ███████████████████████        1045  NodePool: instance-category in [c, m, r]  −303
│  ├─ ██████                          309  pod constraint: instance-local-nvme >= 101  −718
│  ├─ ████                            187  pod constraint: cpu-manufacturer = intel  −122
│  ├─ ███                             173  resource fit: cpu≥3350m, mem≥8448Mi  −14
│  └─ ▏                                 1  CreateFleet picks: m5d.xlarge (on-demand, us-east-1b)
├─ LAUNCH DECISION  @ 2026-07-09 10:15:04Z
│  ├─ EC2 CreateFleet chose c6a.2xlarge (spot) in us-east-1b @ ~$0.0512/hr
│  └─ allocation strategy: price-capacity-optimized across spot offerings
├─ LIFECYCLE
│  └─ created → launched (+2s) → registered (+31s) → initialized (+18s)
└─ DISRUPTION
   └─ none — node is running and not marked for disruption
```

## Repo layout

```
kexplain                 the tool (single-file python, stdlib only)
infra/create-cluster.sh  provision demo EKS cluster + Karpenter v1.13
infra/teardown.sh        delete everything
test/workloads.yaml      deployments that exercise varied constraints
```
