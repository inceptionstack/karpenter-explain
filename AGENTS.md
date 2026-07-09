# Agent guide

This file is for AI agents (and scripts) that need to set up or drive
kexplain for a user. Everything here is non-interactive and machine-checkable.

## What this tool is

`kexplain` explains Karpenter provisioning decisions: which pods triggered a
node, which constraints narrowed the choice, what EC2 CreateFleet picked and
why, and what disruption removed it. Single-file Python 3.8+, stdlib only,
no dependencies to install.

## Setup for a user, step by step

```bash
# 1. install (any dir on PATH)
pipx install git+https://github.com/inceptionstack/karpenter-explain
# or: pip install --user git+https://github.com/inceptionstack/karpenter-explain
# or copy the single file: cp kexplain.py ~/.local/bin/kexplain && chmod +x ~/.local/bin/kexplain

# 2. point kubectl at the user's cluster
aws eks update-kubeconfig --name <cluster> --region <region>

# 3. verify everything, machine-readable
kexplain doctor --json
```

`doctor --json` prints `{"ok": true|false, "checks": [...]}` where each check
has `check`, `ok`, `detail`, and (on failure) `fix` with the exact remediation.
Exit code 0 means ready, 3 means something required is broken. The checks
`aws ec2 access` and `debug logging` are optional: `ok` stays true without
them, but prices/funnel/plan/why-not need the former and candidate lists need
the latter.

If `doctor` says karpenter runs in a nonstandard namespace, set
`KARPENTER_NAMESPACE=<ns>`. If the store path is not writable, set
`KEXPLAIN_STORE=<dir>`.

Every regular command runs a fast preflight first and falls through to
`doctor` output automatically when the basics are broken, so you will never
get a raw stack trace for a missing kubeconfig.

## Driving it non-interactively

Do NOT use `kexplain wizard` (it requires a TTY and will exit immediately for
you). Use the direct commands:

| Task | Command |
|---|---|
| health check | `kexplain doctor --json` |
| harvest state (cron this for history retention) | `kexplain sync` |
| list nodes incl. deleted ones | `kexplain --no-color nodes` |
| decision timeline | `kexplain --no-color history --since 24` |
| full trace for a node | `kexplain --no-color explain <target> --json` |
| why type X was rejected | `kexplain --no-color explain <target> --why-not <type>` |
| simulate a deployment | `kexplain --no-color plan -f <file>` |

`<target>` accepts a node name, nodeclaim name, EC2 instance id, or any
unique prefix.

Always pass `--no-color` when parsing output. `explain --json` returns the
full story object (timestamps, trigger pods, candidates, disruption).
`nodes`/`history`/`plan` are text; parse them line-wise or prefer the json
surfaces.

Exit codes: 0 success, 1 error (bad target, unparseable file), 3 doctor found
required checks failing.

## Data and side effects

- Reads: kubectl (logs, pods, nodes, nodeclaims, nodepools, events), and two
  read-only AWS calls (`ec2:DescribeInstanceTypes`,
  `ec2:DescribeSpotPriceHistory`). It never mutates cluster or AWS state.
- Writes: only `~/.kexplain/<cluster>/` (or `KEXPLAIN_STORE`). This store
  contains node names, private IPs, and instance ids. Do not commit it or
  send it anywhere.
- Every command auto-harvests before running; `--no-sync` skips that when you
  need speed and the store is fresh.

## Provisioning a demo cluster (only if the user asks)

`infra/create-cluster.sh` creates a real EKS cluster + Karpenter v1.13 in
us-east-1 and costs real money (about $0.35/hr idle). Takes ~20 minutes.
`infra/teardown.sh` removes everything. Never run either without the user
explicitly asking.

## Development

- Code lives in the single `kexplain.py` file, organized by section markers
  (`# --- store`, `# --- decision model`, `# --- commands`, ...).
- Tests: `python3 -m unittest discover tests`. They run offline against
  fixtures captured from a real Karpenter v1.13 cluster; no cluster or AWS
  needed. Add a fixture-based test when you change log parsing, the funnel,
  or requirement matching.
- Style rules that are hard requirements in this repo: no em-dashes anywhere
  (docs, comments, output strings), plain direct prose, and commit messages
  carry no AI attribution of any kind.
- Before pushing: `python3 -m unittest discover tests` must pass and
  `grep -rnP '\x{2014}' $(git ls-files)` (the em-dash gate) must come back
  empty. CI enforces both.
