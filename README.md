# kexplain

kexplain answers "why did Karpenter give me this node?" the way SQL EXPLAIN
answers "why did the database run my query this way?". It works after the
fact, and before it.

```
▌       ▜   ▘
▙▘█▌▚▘▛▌▐ ▀▌▌▛▌
▛▖▙▖▞▖▙▌▐▖█▌▌▌▌
      ▌
  an EXPLAIN plan for Karpenter

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

## Install

Pick whichever you like; the tool is a single stdlib-only Python file either way.

```bash
# pipx / uvx, straight from GitHub (recommended)
pipx install git+https://github.com/inceptionstack/karpenter-explain
# or run without installing:
uvx --from git+https://github.com/inceptionstack/karpenter-explain kexplain doctor

# pip
pip install --user git+https://github.com/inceptionstack/karpenter-explain

# no python packaging at all: it is one file
curl -fsSLo ~/.local/bin/kexplain \
  https://raw.githubusercontent.com/inceptionstack/karpenter-explain/main/kexplain.py
chmod +x ~/.local/bin/kexplain

# from a clone
git clone https://github.com/inceptionstack/karpenter-explain && cd karpenter-explain
pip install --user .
```

Then:

```bash
kexplain --version               # kexplain 0.1.x (alpha)
kexplain doctor                  # checks everything and tells you what to fix
```

kexplain is alpha software; the version is also shown in the banner on
every interactive run.

`doctor` verifies kubectl, cluster access, Karpenter, NodePools, debug
logging, AWS access, and the local store, with a fix hint for anything
broken. Regular commands run a fast preflight too, so a missing kubeconfig
gets you a diagnosis instead of a stack trace.

Agents and scripts: see [AGENTS.md](AGENTS.md) for the non-interactive
contract (`doctor --json`, exit codes, json surfaces, no-TTY rules).

## The wizard: start here if you are human

Run `kexplain` with no arguments (or `kexplain wizard`) and you get a guided
investigation instead of a wall of flags:

```
▌       ▜   ▘
▙▘█▌▚▘▛▌▐ ▀▌▌▛▌
▛▖▙▖▞▖▙▌▐▖█▌▌▌▌
      ▌
  an EXPLAIN plan for Karpenter  |  interactive investigation

  checking your setup...
  ✓ setup looks good

  1. Why did I get this specific node / instance type?
  2. Why did Karpenter pick an expensive / weird instance?
  3. Why was a type I expected NOT chosen?
  4. What happened in my cluster recently?
  5. What would a new deployment get? (before the fact)
  6. Check my setup (doctor)

What do you want to investigate? [1-6, q to quit]:
```

The wizard runs the doctor checks on startup, so a broken kubeconfig or a
missing Karpenter install is caught before you pick an investigation. If
something required is broken it shows the failing checks with fix hints and
asks whether to continue. Optional gaps (no AWS credentials, no debug
logging) are noted in one line and the wizard carries on.

Each flow asks plain questions, lists your nodes with numbered menus (no
copy-pasting node names), runs the right commands for you, and offers
follow-ups: for example after explaining an expensive node it offers to
compare against the type you expected instead. `q` quits at any prompt.

Everything the wizard does maps to a direct command (`explain`, `history`,
`plan`, `--why-not`), so once you know the flow you can skip straight to
those; they are listed in the command reference below.

## Or: let your AI agent set it up

Using Claude Code or another coding agent? Paste this prompt and you are done:

```text
Set up the kexplain tool from https://github.com/inceptionstack/karpenter-explain
and smoke test it against my cluster.

1. Read AGENTS.md in that repo first, it is the guide for agents.
2. Install kexplain onto my PATH (pipx install from the repo url works).
3. Connect kubectl to my EKS cluster: <CLUSTER-NAME> in <REGION>.
   (Skip this if kubectl already points at the right cluster.)
4. Run `kexplain doctor --json` and fix anything it flags, using the fix
   hints in its output. Two checks are optional: aws ec2 access and debug
   logging. Tell me if either is missing and what I lose without it.
5. Smoke test: run `kexplain --no-color nodes` and
   `kexplain --no-color history --since 24`. If there are any nodes, pick
   one and run `kexplain --no-color explain <that-node>`.
6. Report back: doctor result, how many Karpenter nodes you found, and a
   short summary of the most recent provisioning or consolidation decision.

Do not create, modify, or delete anything in the cluster or in AWS. kexplain
itself is read-only; your job is only to install, verify, and report.
```

Replace `<CLUSTER-NAME>` and `<REGION>`. If Karpenter runs outside
`kube-system`, add that to the prompt (`export KARPENTER_NAMESPACE=<ns>`).

This exact flow has been tested end to end: a fresh agent on a clean machine,
given only this repo, produced a green setup and a correct explain trace on
its first try.

## Why you might want this

Karpenter's reasoning is scattered across controller logs that rotate away,
NodeClaims that get deleted with their nodes, and events that expire after an
hour. kexplain saves all three into a local store every time you run it, so
you can still explain a node that was consolidated away days ago.

- "Why do I keep getting expensive NVMe / high-network / latest-gen
  instances?" The FEATURE ATTRIBUTION section tells you whether a premium
  feature was actually required by your constraints, or whether CreateFleet
  just picked it. It also tells you how to block it.
- "Why wasn't the cheap type picked?" Run `--why-not <type>` and you get
  every rule that eliminated it. If nothing eliminated it, you get the
  CreateFleet-level reason (price or spot pool depth) instead.
- "What just happened in my cluster?" `history` is a running timeline of
  every provisioning and consolidation decision, including the $/hr savings
  Karpenter calculated.
- "What will this deployment get?" `plan` simulates the funnel for a pod
  spec before you apply it.

## Requirements

| What | Why | Required? |
|---|---|---|
| kubectl + kubeconfig for the cluster | reads logs, NodeClaims, events | yes |
| Python 3.8+ (stdlib only, nothing to pip install) | runs the tool | yes |
| aws CLI with read-only EC2 access | spot prices, funnel, plan, why-not | recommended |
| Karpenter v1.x in kube-system | the thing being explained | yes |

Works best with Karpenter's helm chart set to `logLevel: debug`, which logs
the candidate lists. `info` works too, you just lose candidate details.

If Karpenter runs in a different namespace: `export KARPENTER_NAMESPACE=<ns>`.

The AWS calls are `ec2:DescribeInstanceTypes` and
`ec2:DescribeSpotPriceHistory`, both read-only. Without AWS credentials
everything still works except prices and the funnel/plan/why-not catalog.

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
| `kexplain nodes [--live] [--json]` | All Karpenter nodes, including disrupted and deleted ones from the store |
| `kexplain history [--since HOURS] [--json]` | Timeline: PENDING → DECIDE → CREATE → LAUNCH → REGISTER → READY → DISRUPT → DELETE |
| `kexplain explain <target>` | Full decision trace (sections below) |
| `kexplain explain <target> --why-not TYPE` | Why a specific type wasn't chosen |
| `kexplain explain <target> --json` | Machine-readable story |
| `kexplain plan -f FILE` | Simulate the funnel for a Pod/Deployment/Job yaml before applying |
| `kexplain sync` | Harvest only (see "Keeping history" below) |
| `kexplain wizard` | Interactive guided investigation (also: bare `kexplain` on a TTY) |
| `kexplain doctor [--json]` | Check prerequisites; exit 0 ready, 3 broken |

Global flags: `--no-color`, `--no-sync` (skip harvesting). For `explain`:
`--no-prices`, `--no-funnel`.

### The sections of `explain`

| Section | Answers |
|---|---|
| TRIGGER | Which pending pods started this (and why they did not fit, from FailedScheduling events), or which consolidation command this node replaced |
| CONSTRAINTS | NodePool requirements ∩ pod constraints, with provenance for each narrowed rule |
| CANDIDATES | How many types survived, per Karpenter's own logs |
| FUNNEL | The type universe shrinking stage by stage, with counts and survivor names |
| LAUNCH DECISION | What CreateFleet picked, the allocation strategy, live spot price, "why not cheaper?" |
| FEATURE ATTRIBUTION | Premium features (NVMe, enhanced network, latest gen): required, or just picked? |
| LIFECYCLE | created → launched → registered → initialized latencies |
| DISRUPTION | The consolidation/drift/expiry decision that removed it, with $/hr savings |

## Keeping history

Every command harvests before running, so history builds up just by using
the tool. But Karpenter's logs only reach back to the current and previous
controller container. To keep history across Karpenter restarts, cron the
harvest:

```bash
*/15 * * * * kexplain sync >/dev/null 2>&1
```

The store lives in `~/.kexplain/<cluster>/` (override with `KEXPLAIN_STORE`).
It contains node names, private IPs, and instance IDs, so treat it like your
kubeconfig. See [SECURITY.md](SECURITY.md).

## How it works, and limitations

kexplain reconstructs Karpenter's actual pipeline (pending pods, requirement
intersection, instance-type filtering, EC2 CreateFleet, lifecycle,
disruption) from three sources: controller JSON logs, NodeClaim/NodePool
snapshots, and Kubernetes events.

- The FUNNEL is recomputed from the current EC2 catalog, because Karpenter
  doesn't log per-stage counts. A reconciliation line compares the funnel's
  final count with what Karpenter itself reported. The leftover gap is zone
  offerings and AMI compatibility, which only Karpenter's runtime sees.
- The final 1-of-N choice belongs to EC2 CreateFleet, not Karpenter. For
  spot, price-capacity-optimized deliberately trades price for pool depth and
  lower interruption risk. That's why the absolute cheapest type often loses.
  kexplain names the trade-off and shows live prices, but EC2 doesn't expose
  its per-pool ranking.
- The savings in `DISRUPT ... saves $X/hr` come from Karpenter's own
  consolidation math, parsed from its logs. They are not recomputed.

## Trying it without a cluster to lose

`infra/create-cluster.sh` provisions a complete demo environment (EKS,
Karpenter v1.13, a spot/on-demand NodePool) in us-east-1, and
`test/workloads.yaml` has deployments with varied constraints (arm64,
on-demand + zone-pinned, plain burst) to generate real decisions:

```bash
./infra/create-cluster.sh          # ~20 min; creates real AWS resources ($)
kubectl apply -f test/workloads.yaml
kubectl scale deploy inflate --replicas 5
sleep 120 && kexplain history
./infra/teardown.sh                # when done. this deletes the cluster
```

## Repo layout

```
kexplain.py              the tool, single-file python, stdlib only
pyproject.toml           packaging so pipx/pip install works from the repo url
tests/                   unit tests (offline, fixture-based): python3 -m unittest discover tests
AGENTS.md                contract for AI agents and scripts driving the tool
infra/create-cluster.sh  demo cluster provisioning (eksctl + helm)
infra/teardown.sh        full cleanup
test/workloads.yaml      constraint-varied workloads to generate decisions
SECURITY.md              access footprint and data-handling notes
```

## License

MIT, see [LICENSE](LICENSE).
