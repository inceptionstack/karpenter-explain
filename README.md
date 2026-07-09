# kexplain — an EXPLAIN plan for Karpenter

**`kexplain` answers "why did Karpenter give me *this* node?" the way SQL
`EXPLAIN` answers "why did the database run my query this way?"** — after the
fact, or before it.

```
$ kexplain explain ip-192-168-43-145.ec2.internal

NODE ip-192-168-43-145.ec2.internal  nodeclaim=default-8bk6c  [RUNNING]
├─ TRIGGER  @ 2026-07-09 09:56:18Z
│  └─ 3 unschedulable pod(s) could not fit on existing nodes:
│     └─ default/expensive-mystery-d68bd97d8-h7qzj …
├─ CONSTRAINTS
│  ├─ NodePool default requirements: …
│  └─ Resolved NodeClaim requirements (NodePool ∩ pod constraints):
│     ├─ karpenter.k8s.aws/instance-local-nvme >= 101   ← narrowed by pod constraints
│     └─ karpenter.k8s.aws/instance-cpu-manufacturer = intel   ← narrowed by pod constraints
├─ FUNNEL  (recomputed from the live EC2 catalog)
│  ├─ ██████████████████████████████ 1348  EC2 instance types in region
│  ├─ ███████████████████████        1045  NodePool: instance-category in [c, m, r]  −303
│  ├─ ██████                          309  pod constraint: instance-local-nvme >= 101  −718
│  ├─ ████                            187  pod constraint: cpu-manufacturer = intel  −122
│  ├─ ███                             173  resource fit: cpu≥3350m, mem≥8448Mi  −14
│  └─ ▏                                 1  CreateFleet picks: m5d.xlarge (on-demand, us-east-1b)
├─ FEATURE ATTRIBUTION
│  └─ local NVMe storage ('d' variant): explicitly required by a scheduling constraint
├─ LIFECYCLE
│  └─ created → launched (+3s) → registered (+19s) → initialized (+14s)
└─ DISRUPTION
   └─ @ 10:01:56Z: disrupted via Underutilized → replace (saves $0.23/hr)
```

Karpenter's reasoning is scattered across ephemeral controller logs,
NodeClaims that are deleted with their nodes, and events that expire after an
hour. `kexplain` harvests all three into a local store on every run, so a node
that was consolidated away 3 days ago is still fully explainable.

## Why you might want this

- **"Why do I keep getting expensive NVMe / high-network / latest-gen
  instances?"** — `FEATURE ATTRIBUTION` tells you whether a premium feature
  was *required by your constraints* or *picked by CreateFleet*, and how to
  block it.
- **"Why wasn't the cheap type picked?"** — `--why-not <type>` shows every
  rule that eliminated it, or the CreateFleet-level price/pool-depth reason if
  it survived the rules.
- **"What just happened in my cluster?"** — `history` is a running timeline of
  every provisioning and consolidation decision, with $/hr savings.
- **"What will this deployment get?"** — `plan` simulates the funnel for a pod
  spec *before you apply it*.

## Requirements

| What | Why | Required? |
|---|---|---|
| `kubectl` + kubeconfig for the cluster | reads logs, NodeClaims, events | yes |
| Python 3.8+ (stdlib only, no pip installs) | runs the tool | yes |
| `aws` CLI with read-only EC2 access | spot prices, funnel, `plan`, `--why-not` | recommended |
| Karpenter v1.x in `kube-system` | the thing being explained | yes |

Works best with Karpenter's helm chart set to `logLevel: debug` (richest
traces, including candidate lists). `info` works; you lose candidate details.

If Karpenter runs in a different namespace: `export KARPENTER_NAMESPACE=<ns>`.

The AWS calls are `ec2:DescribeInstanceTypes` and
`ec2:DescribeSpotPriceHistory` — both read-only. Without AWS credentials
everything still works except prices and the funnel/plan/why-not catalog.

## Install

```bash
git clone <this-repo> && cd karpenter-explain
cp kexplain ~/.local/bin/        # or anywhere on PATH
kexplain --help
```

## Five-minute tour

```bash
# point kubectl at your cluster
aws eks update-kubeconfig --name my-cluster

# what Karpenter nodes exist (or existed)?
kexplain nodes
NODECLAIM      NODE                            TYPE         CAPACITY   ZONE        INSTANCE-ID          STATUS     LIFETIME
default-8bk6c  ip-192-168-43-145.ec2.internal  m5d.xlarge   on-demand  us-east-1b  i-09284…             DISRUPTED  7m33s
default-mm8ln  ip-192-168-81-189.ec2.internal  c5.large     spot       us-east-1a  i-05412…             RUNNING    4m13s

# what has Karpenter been deciding?
kexplain history --since 24
09:56:18  PENDING    3 unschedulable pod(s): default/expensive-mystery-…
09:56:18  CREATE     nodeclaim default-8bk6c (nodepool default, 98 candidate types)
09:56:21  LAUNCH     default-8bk6c → m5d.xlarge (on-demand) in us-east-1b
10:01:56  DISRUPT    default-8bk6c via Underutilized (replace, 1 replacement(s), saves $0.23/hr)

# the full decision trace for one node
# (accepts node name, nodeclaim name, instance id, or any unique prefix)
kexplain explain default-8bk6c

# interrogate a specific type
kexplain explain default-8bk6c --why-not c8g.xlarge
  ✗ ELIMINATED by 3 rule(s) before reaching CreateFleet:
    ✗ rule: karpenter.k8s.aws/instance-local-nvme >= 101   (pod constraints)
      c8g.xlarge has: 0 GB
    …

# before the fact: what would this deployment get?
kexplain plan -f my-deployment.yaml
  NodePool default: 223 candidate instance types
     c7g.2xlarge   8 vCPU   16.0 GiB  arm64  spot✓
     …
```

## Command reference

| Command | What it does |
|---|---|
| `kexplain nodes [--live]` | All Karpenter nodes — running, disrupted, and deleted ones from the store |
| `kexplain history [--since HOURS]` | Timeline: PENDING → DECIDE → CREATE → LAUNCH → REGISTER → READY → DISRUPT → DELETE |
| `kexplain explain <target>` | Full decision trace (sections below) |
| `kexplain explain <target> --why-not TYPE` | Why a specific type wasn't chosen |
| `kexplain explain <target> --json` | Machine-readable story |
| `kexplain plan -f FILE` | Simulate the funnel for a Pod/Deployment/Job yaml before applying |
| `kexplain sync` | Harvest only (see "Keeping history" below) |

Global flags: `--no-color`, `--no-sync` (skip harvesting), and for `explain`:
`--no-prices`, `--no-funnel`.

### The sections of `explain`

| Section | Answers |
|---|---|
| `TRIGGER` | Which pending pods started this — or which consolidation command this node replaced |
| `CONSTRAINTS` | NodePool requirements ∩ pod constraints, with `← narrowed by pod constraints` provenance |
| `CANDIDATES` | How many types survived, per Karpenter's own logs |
| `FUNNEL` | The type universe shrinking stage by stage, with counts and survivor names |
| `LAUNCH DECISION` | What CreateFleet picked, the allocation strategy, live spot price, "why not cheaper?" |
| `FEATURE ATTRIBUTION` | Premium features (NVMe / enhanced network / latest gen): required, or just picked? |
| `LIFECYCLE` | created → launched → registered → initialized latencies |
| `DISRUPTION` | The consolidation/drift/expiry decision that removed it, with $/hr savings |

## Keeping history

Every command auto-harvests before running, so history accumulates just by
using the tool. But Karpenter's logs only reach back to the current + previous
controller container. To keep history across Karpenter restarts, cron the
harvest:

```bash
*/15 * * * * kexplain sync >/dev/null 2>&1
```

The store lives in `~/.kexplain/<cluster>/` (override with `KEXPLAIN_STORE`).
It contains node names, private IPs, and instance IDs — treat it like your
kubeconfig. See [SECURITY.md](SECURITY.md).

## How it works / honest limitations

`kexplain` reconstructs Karpenter's actual pipeline — pending pods →
requirement intersection → instance-type filtering → EC2 CreateFleet →
lifecycle → disruption — from three sources: controller JSON logs, NodeClaim/
NodePool snapshots, and Kubernetes events.

- The **FUNNEL is recomputed** from the current EC2 catalog, because Karpenter
  doesn't log per-stage counts. A reconciliation line compares the funnel's
  final count with what Karpenter itself reported; the residual gap is zone
  offerings and AMI compatibility, which only Karpenter's runtime sees.
- The **final 1-of-N choice belongs to EC2 CreateFleet**, not Karpenter. For
  spot, `price-capacity-optimized` deliberately trades price for pool depth /
  lower interruption risk — that's why the absolute cheapest type often loses.
  kexplain names the trade-off and shows live prices, but EC2 doesn't expose
  its per-pool ranking.
- Prices in `DISRUPT … saves $X/hr` come from Karpenter's own consolidation
  math, parsed from its logs — not recomputed.

## Trying it without a cluster to lose

`infra/create-cluster.sh` provisions a complete demo environment (EKS +
Karpenter v1.13 + a spot/on-demand NodePool) in us-east-1, and
`test/workloads.yaml` contains deployments with varied constraints (arm64,
on-demand + zone-pinned, plain burst) to generate real decisions:

```bash
./infra/create-cluster.sh          # ~20 min; creates real AWS resources ($)
kubectl apply -f test/workloads.yaml
kubectl scale deploy inflate --replicas 5
sleep 120 && kexplain history
./infra/teardown.sh                # when done — this deletes the cluster
```

## Repo layout

```
kexplain                 the tool — single-file python, stdlib only
infra/create-cluster.sh  demo cluster provisioning (eksctl + helm)
infra/teardown.sh        full cleanup
test/workloads.yaml      constraint-varied workloads to generate decisions
SECURITY.md              access footprint and data-handling notes
```

## License

MIT — see [LICENSE](LICENSE).
