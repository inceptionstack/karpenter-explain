#!/usr/bin/env python3
"""
kexplain: an EXPLAIN plan for Karpenter.

Reconstructs, per node, how Karpenter reached its provisioning decision:
which pending pods triggered it, what constraints were in play, which
instance types were candidates, what CreateFleet actually chose, the node
lifecycle, and any later disruption (consolidation/drift/expiry).

Data sources (harvested into a local store on every run, because they are
ephemeral in the cluster):
  * karpenter controller JSON logs (debug level)
  * NodeClaim / NodePool / EC2NodeClass / Node objects
  * kubernetes events (Nominated, DisruptionBlocked, ...)

Commands:
  kexplain sync                harvest cluster state into the local store
  kexplain nodes               list karpenter-managed nodes (live + historical)
  kexplain history             timeline of provisioning & disruption decisions
  kexplain explain <node>      full decision trace for a node / nodeclaim
  kexplain plan -f pod.yaml    before-the-fact: candidate instance types for a pod
  kexplain wizard              interactive guided investigation (also: bare kexplain)
  kexplain doctor              prerequisite checks with fix hints

Layout of this file (single file by design; grep for the section markers):
  constants        tunables and karpenter label keys
  utilities        shell, time, color, logo, status line
  store            local jsonl + snapshot persistence (~/.kexplain)
  sync             harvesting logs/events/objects from the cluster
  decision model   NodeStory + parsers from logs/events/snapshots
  pricing          spot price lookup (cached)
  commands         nodes / history / explain (per-section renderers) / why-not
  plan             before-the-fact simulation against the EC2 catalog
  wizard           interactive flows
  doctor           health checks (run_checks is reused by the wizard)
  main             argparse wiring, preflight
"""
import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone, timedelta

# Alpha. Patch number is auto-bumped to 0.1.<commits-on-main> by the
# version-bump GitHub Action on every push to main. Do not edit by hand.
__version__ = "0.1.22"

STORE_ROOT = os.environ.get("KEXPLAIN_STORE", os.path.expanduser("~/.kexplain"))
KARPENTER_NS = os.environ.get("KARPENTER_NAMESPACE", "kube-system")

# How much of an instance's raw capacity we assume is schedulable after
# kubelet/system reservation and daemonsets. Rough heuristics; Karpenter
# computes exact overhead from instance type at runtime.
CPU_ALLOCATABLE_RATIO = 0.9
MEM_ALLOCATABLE_RATIO = 0.85

# A "created nodeclaim" is attributed to the provisioning session or
# disruption command that happened at most this many seconds before it.
PROVISION_LINK_WINDOW_S = 60
REPLACEMENT_LINK_WINDOW_S = 15

EC2_CATALOG_TTL_S = 7 * 24 * 3600
KARPENTER_LOG_TAIL = 200

# funnel rendering: bar width in characters, and how small the surviving
# set must be before we list type names / family names under a stage
FUNNEL_BAR_WIDTH = 30
FUNNEL_NAME_SURVIVORS_MAX = 12
FUNNEL_NAME_FAMILIES_MAX = 40

# Requirement keys injected by karpenter itself; not user-meaningful
# when explaining a decision.
INTERNAL_REQ_KEYS = frozenset({
    "karpenter.sh/nodepool", "karpenter.k8s.aws/ec2nodeclass",
    "kubernetes.io/os",
})

# ---------------------------------------------------------------- utilities

def sh(cmd, check=True, timeout=120):
    p = subprocess.run(cmd, shell=isinstance(cmd, str), capture_output=True,
                       text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError(f"command failed: {cmd}\n{p.stderr.strip()}")
    return p.stdout

def kubectl_json(args):
    out = sh(f"kubectl {args} -o json")
    return json.loads(out)

def parse_ts(s):
    if not s:
        return None
    s = s.replace("Z", "+00:00")
    # trim sub-second precision to 6 digits for fromisoformat
    m = re.match(r"(.*\.\d{1,6})\d*(\+.*|$)", s)
    if m:
        s = m.group(1) + (m.group(2) or "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None

def fmt_ts(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ") if dt else "?"

def fmt_dur(seconds):
    if seconds is None:
        return "?"
    seconds = int(seconds)
    if seconds < 120:
        return f"{seconds}s"
    if seconds < 7200:
        return f"{seconds // 60}m{seconds % 60:02d}s"
    return f"{seconds // 3600}h{(seconds % 3600) // 60:02d}m"

USE_COLOR = sys.stdout.isatty()
def c(code, s):
    return f"\033[{code}m{s}\033[0m" if USE_COLOR else str(s)
def bold(s):    return c("1", s)
def dim(s):     return c("2", s)
def green(s):   return c("32", s)
def yellow(s):  return c("33", s)
def red(s):     return c("31", s)
def cyan(s):    return c("36", s)
def magenta(s): return c("35", s)

def print_logo(subtitle=""):
    """Small banner for interactive use. Skipped when piped."""
    if not sys.stdout.isatty():
        return
    art = (
        "▌       ▜   ▘\n"
        "▙▘█▌▚▘▛▌▐ ▀▌▌▛▌\n"
        "▛▖▙▖▞▖▙▌▐▖█▌▌▌▌\n"
        "      ▌"
    )
    print(cyan(art))
    print(dim(f"  an EXPLAIN plan for Karpenter  v{__version__} (alpha)"
              f"{('  |  ' + subtitle) if subtitle else ''}\n"))

def status(msg=None):
    """Transient one-line progress message; each call replaces the previous
    one, and status() with no argument clears it. No-op when piped."""
    if not sys.stdout.isatty():
        return
    sys.stdout.write("\r\033[2K")
    if msg:
        sys.stdout.write(dim(f"  {msg}"))
    sys.stdout.flush()

# ---------------------------------------------------------------- store

class Store:
    """Local persistence: logs.jsonl (deduped), object snapshots, events."""

    def __init__(self, cluster):
        self.dir = os.path.join(STORE_ROOT, cluster)
        for sub in ("", "nodeclaims", "nodes", "nodepools", "ec2nodeclasses"):
            os.makedirs(os.path.join(self.dir, sub), exist_ok=True)
        self.log_path = os.path.join(self.dir, "logs.jsonl")
        self.events_path = os.path.join(self.dir, "events.jsonl")

    # ---- generic jsonl with dedup by content hash
    def _load_jsonl(self, path):
        rows = []
        if os.path.exists(path):
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            rows.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
        return rows

    def _append_jsonl(self, path, rows, key_fn):
        seen = set()
        for r in self._load_jsonl(path):
            seen.add(key_fn(r))
        added = 0
        with open(path, "a") as f:
            for r in rows:
                k = key_fn(r)
                if k not in seen:
                    seen.add(k)
                    f.write(json.dumps(r, separators=(",", ":")) + "\n")
                    added += 1
        return added

    @staticmethod
    def _log_key(r):
        return hashlib.sha1(json.dumps(
            [r.get("time"), r.get("message"), r.get("NodeClaim"), r.get("Pods"),
             r.get("command-id"), r.get("controller")],
            sort_keys=True).encode()).hexdigest()

    @staticmethod
    def _event_key(e):
        return f'{e.get("uid")}:{e.get("count")}:{e.get("lastTimestamp")}'

    def add_logs(self, rows):
        return self._append_jsonl(self.log_path, rows, self._log_key)

    def add_events(self, rows):
        return self._append_jsonl(self.events_path, rows, self._event_key)

    def logs(self):
        rows = self._load_jsonl(self.log_path)
        rows.sort(key=lambda r: r.get("time") or "")
        return rows

    def events(self):
        return self._load_jsonl(self.events_path)

    # ---- object snapshots (latest wins; survive deletion in-cluster)
    def snapshot(self, kind_dir, obj):
        name = obj["metadata"]["name"]
        with open(os.path.join(self.dir, kind_dir, name + ".json"), "w") as f:
            json.dump(obj, f)

    def objects(self, kind_dir):
        out = {}
        d = os.path.join(self.dir, kind_dir)
        for fn in os.listdir(d):
            if fn.endswith(".json"):
                with open(os.path.join(d, fn)) as f:
                    try:
                        obj = json.load(f)
                        out[obj["metadata"]["name"]] = obj
                    except (json.JSONDecodeError, KeyError):
                        pass
        return out

# ---------------------------------------------------------------- sync

def current_cluster():
    try:
        ctx = sh("kubectl config current-context").strip()
    except RuntimeError:
        sys.exit("error: no kubectl context. Is your kubeconfig set up?")
    # eksctl contexts look like user@cluster.region.eksctl.io
    m = re.search(r"@?([\w-]+)\.([\w-]+)\.eksctl\.io", ctx)
    if m:
        return m.group(1)
    m = re.search(r"cluster/([\w-]+)", ctx)
    return m.group(1) if m else re.sub(r"[^\w.-]", "_", ctx)

def sync(store, quiet=False):
    def note(msg):
        if not quiet:
            print(dim(f"  sync: {msg}"))

    # -- karpenter controller logs (current + previous container)
    raw = ""
    for flag in ("", "--previous"):
        try:
            raw += sh(f"kubectl logs -n {KARPENTER_NS} "
                      f"-l app.kubernetes.io/name=karpenter "
                      f"--all-containers --tail=-1 {flag}", check=False)
        except Exception:
            pass
    rows = []
    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    note(f"{store.add_logs(rows)} new log lines ({len(rows)} fetched)")

    # -- events from all namespaces (karpenter events land on pods/nodes/nodeclaims)
    try:
        evs = kubectl_json("get events -A")["items"]
        keep = []
        for e in evs:
            src = (e.get("source", {}) or {}).get("component", "") or \
                  e.get("reportingComponent", "")
            if "karpenter" in src or e.get("reason") in (
                    "Nominated", "FailedScheduling", "DisruptionBlocked",
                    "Unconsolidatable", "DisruptionTerminating", "Evicted"):
                keep.append({
                    "uid": e["metadata"]["uid"],
                    "reason": e.get("reason"),
                    "message": e.get("message"),
                    "count": e.get("count"),
                    "kind": e.get("involvedObject", {}).get("kind"),
                    "name": e.get("involvedObject", {}).get("name"),
                    "namespace": e.get("involvedObject", {}).get("namespace"),
                    "lastTimestamp": e.get("lastTimestamp") or
                                     e.get("eventTime") or
                                     e.get("firstTimestamp"),
                    "source": src,
                })
        note(f"{store.add_events(keep)} new events")
    except Exception as ex:
        note(f"events failed: {ex}")

    # -- object snapshots
    for kind, kdir in (("nodeclaims", "nodeclaims"), ("nodepools", "nodepools"),
                       ("ec2nodeclasses", "ec2nodeclasses")):
        try:
            for obj in kubectl_json(f"get {kind}")["items"]:
                obj.pop("managedFields", None)
                store.snapshot(kdir, obj)
        except Exception:
            pass
    try:
        for obj in kubectl_json("get nodes")["items"]:
            obj["metadata"].pop("managedFields", None)
            store.snapshot("nodes", obj)
    except Exception:
        pass
    note("object snapshots updated")

# ---------------------------------------------------------------- decision model

class NodeStory:
    """Everything we know about one nodeclaim's life."""
    def __init__(self, name):
        self.name = name                     # nodeclaim name
        self.node = None                     # k8s node name
        self.nodepool = None
        self.provider_id = None
        self.instance_type = None
        self.zone = None
        self.capacity_type = None
        self.allocatable = None
        self.requests = None                 # aggregated requests at creation
        self.candidate_types = None          # truncated list from logs
        self.candidate_count = None
        self.trigger_pods = []               # [(ns/pod, ...)]
        self.trigger_reasons = []            # [(reason, count)] why the pods didn't fit
        self.nominated_pods = []
        self.t_created = self.t_launched = self.t_registered = None
        self.t_initialized = self.t_deleted = None
        self.disruption = None               # dict: reason/decision/replacements/ts
        self.disruption_blocked = []         # [(ts, message)]
        self.replaces = None                 # dict: this node replaced others via consolidation
        self.raw_claim = None

    @property
    def has_claim_spec(self):
        """True when the store captured the NodeClaim object itself. Without
        it we know WHAT launched (from logs) but not the resolved
        requirements, so pod-constraint attribution is impossible."""
        return bool(self.raw_claim)

def _nodeclaim_name(row):
    nc = row.get("NodeClaim")
    return nc.get("name") if isinstance(nc, dict) else nc

def _parse_candidate_types(itypes):
    """Parse the created-nodeclaim 'instance-types' field, e.g.
    "c3.2xlarge, c3.4xlarge, c3.8xlarge and 595 other(s)" -> (list, count).
    The last named type is glued to "and N other(s)"."""
    extra = 0
    m = re.search(r"\s+and (\d+) other\(s\)", itypes)
    if m:
        extra = int(m.group(1))
        itypes = itypes[:m.start()]
    types = [p.strip() for p in itypes.split(",") if p.strip()]
    return types, len(types) + extra

def _latest_within(entries, ts, window_s):
    """Most recent entry whose timestamp is at most window_s before ts."""
    for entry in reversed(entries):
        ets = entry[0]
        if ets and ts and 0 <= (ts - ets).total_seconds() <= window_s:
            return entry
    return None

def _apply_created(s, row, ts, sessions, replace_cmds):
    s.t_created = ts
    np = row.get("NodePool")
    s.nodepool = np.get("name") if isinstance(np, dict) else np
    s.requests = row.get("requests")
    itypes = row.get("instance-types", "")
    if isinstance(itypes, str):
        s.candidate_types, s.candidate_count = _parse_candidate_types(itypes)
    # attribute this nodeclaim to a provisioning session or to the
    # consolidation command it replaces, whichever happened just before
    session = _latest_within(sessions, ts, PROVISION_LINK_WINDOW_S)
    if session:
        s.trigger_pods = [p.strip() for p in session[1].split(",") if p.strip()]
    repl = _latest_within(replace_cmds, ts, REPLACEMENT_LINK_WINDOW_S)
    if repl:
        _, rnames, rreason, rsav = repl
        s.replaces = {"nodes": rnames, "reason": rreason, "savings": rsav}

def _apply_launched(s, row, ts):
    s.t_launched = ts
    s.provider_id = row.get("provider-id")
    s.instance_type = row.get("instance-type")
    s.zone = row.get("zone")
    s.capacity_type = row.get("capacity-type")
    s.allocatable = row.get("allocatable")

def _parse_disruption(row, ts, fallback_name):
    """Parse a v1.x "disrupting node(s)" log line. The "command" field looks
    like "Empty/<uuid>: delete: nodepools=[default]: [node-a] (savings: $0.27)";
    reason is the prefix, savings appears for consolidation decisions.
    Returns (disrupted nodeclaim names, disruption dict)."""
    cmd = row.get("command", "") or ""
    reason = row.get("reason") or (cmd.split("/", 1)[0] if "/" in cmd else None)
    m = re.search(r"savings: \$([\d.]+)", cmd)
    savings = float(m.group(1)) if m else None
    disrupted = row.get("disrupted-nodes") or row.get("nodes") or []
    if isinstance(disrupted, dict):
        disrupted = [disrupted]
    names = []
    for d in disrupted:
        if isinstance(d, dict):
            nc = d.get("NodeClaim")
            nm = nc.get("name") if isinstance(nc, dict) else nc
            if nm:
                names.append(nm)
    if fallback_name and not names:
        names = [fallback_name]
    disruption = {
        "ts": ts, "reason": reason, "decision": row.get("decision"),
        "replacements": row.get("replacement-node-count", 0),
        "pods": row.get("pod-count"),
        "savings": savings,
        "raw": cmd or row.get("message", ""),
    }
    return names, disruption

# kube-scheduler taints on nodes that are still booting; not a real reason
# a pod cannot be placed, so we drop them from the "why didn't it fit" list
TRANSIENT_TAINTS = ("unregistered", "not-ready", "uninitialized")

def scheduling_reasons(msg):
    """Extract the human reasons from a FailedScheduling event message, e.g.
    "0/3 nodes are available: 1 Insufficient cpu, 2 node(s) didn't match
    Pod's node affinity/selector. preemption: ..." -> ["Insufficient cpu",
    "didn't match Pod's node affinity/selector"]. Transient boot taints are
    dropped; the leading per-reason node count is stripped."""
    m = re.search(r"nodes are available:\s*(.*?)(?:\.?\s*preemption:|$)", msg or "")
    if not m:
        return []
    reasons = []
    for part in m.group(1).split(","):
        p = re.sub(r"^\d+\s+", "", part.strip()).rstrip(".")
        if any(t in p for t in TRANSIENT_TAINTS):
            continue
        if "untolerated taint" in p:
            tm = re.search(r"\{([^:} ]+)", p)
            p = f"untolerated taint {tm.group(1)}" if tm else p
        p = p.replace("node(s) ", "").strip()
        if p:
            reasons.append(p)
    return reasons

def _apply_events(stories, events):
    """Fold karpenter events (nominations, disruption blocks) into stories,
    and attribute FailedScheduling reasons to the nodes their pods triggered."""
    # map each trigger pod (ns/name) to the story it launched
    pod_to_story = {}
    for st in stories.values():
        for pod in st.trigger_pods:
            pod_to_story.setdefault(pod, st)
    reason_counts = {}  # story name -> {reason: count}

    for e in events:
        ts = parse_ts(e.get("lastTimestamp"))
        reason = e.get("reason")
        if reason == "Nominated" and e.get("kind") == "Pod":
            m = re.search(r"nodeclaim/([\w-]+)", e.get("message", ""))
            if m and m.group(1) in stories:
                stories[m.group(1)].nominated_pods.append(
                    f'{e.get("namespace")}/{e.get("name")}')
        elif reason in ("DisruptionBlocked", "Unconsolidatable"):
            nm = e.get("name")
            if e.get("kind") == "NodeClaim" and nm in stories:
                stories[nm].disruption_blocked.append((ts, e.get("message")))
        elif reason == "FailedScheduling" and e.get("kind") == "Pod":
            pod = f'{e.get("namespace")}/{e.get("name")}'
            st = pod_to_story.get(pod)
            if st:
                bucket = reason_counts.setdefault(st.name, {})
                for r in scheduling_reasons(e.get("message")):
                    bucket[r] = bucket.get(r, 0) + 1

    for name, bucket in reason_counts.items():
        stories[name].trigger_reasons = sorted(
            bucket.items(), key=lambda kv: -kv[1])

def _apply_snapshot(s, obj):
    """Fill story gaps from a NodeClaim object snapshot (covers nodeclaims
    whose creation predates the harvested logs)."""
    s.raw_claim = obj
    md, st = obj["metadata"], obj.get("status", {})
    labels = md.get("labels", {})
    s.nodepool = s.nodepool or labels.get("karpenter.sh/nodepool")
    s.instance_type = s.instance_type or labels.get("node.kubernetes.io/instance-type")
    s.zone = s.zone or labels.get("topology.kubernetes.io/zone")
    s.capacity_type = s.capacity_type or labels.get("karpenter.sh/capacity-type")
    s.provider_id = s.provider_id or st.get("providerID")
    s.node = s.node or st.get("nodeName")
    if not s.t_created:
        s.t_created = parse_ts(md.get("creationTimestamp"))

def build_stories(store):
    """Parse harvested logs, events, and snapshots into per-nodeclaim
    NodeStory objects, keyed by nodeclaim name."""
    stories = {}
    def get(name):
        if name not in stories:
            stories[name] = NodeStory(name)
        return stories[name]

    sessions = []      # (ts, pods_str, row) per "found provisionable pod(s)"
    replace_cmds = []  # (ts, disrupted names, reason, savings) per replace command

    for r in store.logs():
        msg = r.get("message", "")
        ts = parse_ts(r.get("time"))
        ncn = _nodeclaim_name(r)

        if msg == "found provisionable pod(s)":
            sessions.append((ts, r.get("Pods", ""), r))
        elif msg == "created nodeclaim" and ncn:
            _apply_created(get(ncn), r, ts, sessions, replace_cmds)
        elif msg == "launched nodeclaim" and ncn:
            _apply_launched(get(ncn), r, ts)
        elif msg == "registered nodeclaim" and ncn:
            s = get(ncn)
            s.t_registered = ts
            node = r.get("Node")
            s.node = node.get("name") if isinstance(node, dict) else node
        elif msg == "initialized nodeclaim" and ncn:
            get(ncn).t_initialized = ts
        elif msg == "deleted nodeclaim" and ncn:
            get(ncn).t_deleted = ts
        elif "disrupting node(s)" in msg or "disrupting nodeclaim(s)" in msg:
            names, disruption = _parse_disruption(r, ts, ncn)
            for nm in names:
                get(nm).disruption = disruption
            if disruption["replacements"] > 0:
                replace_cmds.append((ts, names, disruption["reason"],
                                     disruption["savings"]))

    _apply_events(stories, store.events())

    for name, obj in store.objects("nodeclaims").items():
        _apply_snapshot(get(name), obj)

    for s in stories.values():
        s.nominated_pods = sorted(set(s.nominated_pods))
    return stories

def live_nodeclaims():
    try:
        return {o["metadata"]["name"]: o for o in kubectl_json("get nodeclaims")["items"]}
    except Exception:
        return {}

def instance_id(provider_id):
    return provider_id.rsplit("/", 1)[-1] if provider_id else None

# AWS instance name suffix letters (the part after the generation digit)
# and what each one means. Processor letters are informational; the rest
# mark capability variants. Source: AWS EC2 instance type naming convention.
SUFFIX_MEANINGS = {
    "a": "AMD processor",
    "g": "AWS Graviton processor",
    "i": "Intel processor",
    "d": "local NVMe instance storage",
    "n": "network and EBS optimized",
    "e": "extra storage or memory",
    "z": "high CPU frequency",
    "b": "block storage (EBS) optimized",
    "q": "Qualcomm inference accelerator",
    "f": "fractional GPU or flex",
}
PROCESSOR_SUFFIXES = frozenset("agi")

# Family category letters (before the generation digit) per the AWS
# instance-type naming docs. Every resource-specialized family is notable in
# FEATURE ATTRIBUTION (compute, memory, storage, accelerated, HPC, ...): the
# question "did you ask for this profile, or did CreateFleet just pick it?"
# applies equally to all of them. Only the general-purpose baselines
# (m = general purpose, t = burstable) are treated as unremarkable.
CATEGORY_MEANINGS = {
    "m": "general purpose", "t": "burstable general purpose",
    "c": "compute optimized", "r": "memory optimized",
    "x": "high memory", "u": "high memory (u family)",
    "z": "high frequency memory optimized", "i": "storage optimized (NVMe)",
    "is": "storage optimized", "im": "storage optimized",
    "d": "dense HDD storage", "h": "HDD storage",
    "p": "GPU accelerated (training-class)", "g": "GPU accelerated",
    "gr": "GPU accelerated (memory-heavy)", "dl": "deep learning accelerated",
    "inf": "AWS Inferentia ML accelerator", "trn": "AWS Trainium ML accelerator",
    "f": "FPGA accelerated", "vt": "video transcoding",
    "hpc": "HPC optimized", "mac": "Apple macOS",
}
GENERAL_PURPOSE_CATEGORIES = frozenset({"m", "t"})
NOTABLE_CATEGORIES = frozenset(CATEGORY_MEANINGS) - GENERAL_PURPOSE_CATEGORIES

def instance_features(itype):
    """Decode an instance type name per the AWS naming convention:
    category letters, generation digit(s), capability suffix letters, and an
    optional -variant (flex, b200, 3tb1, m4pro, ...). Handles the odd
    families too: mac-m4, u-6tb1, c7i-flex, p6-b200, gr6f."""
    fam = itype.split(".")[0]
    base, _, variant = fam.partition("-")
    m = re.match(r"([a-z]+?)(\d+)([a-z]*)$", base)
    if not m:
        # no generation digit at all (e.g. "mac" in mac-m4, "u" in u-3tb1)
        return {"family": fam, "category": base, "generation": 0, "suffix": "",
                "variant": variant, "capabilities": []}
    suffix = m.group(3)
    caps = [SUFFIX_MEANINGS[ch] for ch in suffix
            if ch not in PROCESSOR_SUFFIXES and ch in SUFFIX_MEANINGS]
    return {"family": fam, "category": m.group(1), "generation": int(m.group(2)),
            "suffix": "".join(ch for ch in suffix if ch not in PROCESSOR_SUFFIXES),
            "variant": variant, "capabilities": caps}

# ---------------------------------------------------------------- pricing (best effort)

_price_cache = {}

def spot_price(itype, az):
    """Latest spot price for (type, az), cached for the process lifetime.
    Returns None when the aws CLI is missing or the call fails."""
    key = (itype, az)
    if key in _price_cache:
        return _price_cache[key]
    try:
        out = json.loads(sh(
            f"aws ec2 describe-spot-price-history --instance-types {itype} "
            f"--availability-zone {az} --product-descriptions 'Linux/UNIX' "
            f"--max-items 1 --output json", timeout=30))
        p = out["SpotPriceHistory"][0]["SpotPrice"]
        _price_cache[key] = float(p)
    except Exception:
        _price_cache[key] = None
    return _price_cache[key]

# ---------------------------------------------------------------- commands

def cmd_nodes(store, args):
    stories = build_stories(store)
    live = live_nodeclaims()

    ordered = sorted(stories.items(),
                     key=lambda kv: kv[1].t_created or datetime.min.replace(tzinfo=timezone.utc))
    records = []
    for name, s in ordered:
        alive = name in live
        if args.live and not alive:
            continue
        state = "RUNNING" if alive else ("DISRUPTED" if s.disruption else "GONE")
        lifetime_s = None
        if s.t_created:
            end = s.t_deleted or datetime.now(timezone.utc)
            lifetime_s = int((end - s.t_created).total_seconds())
        records.append({
            "nodeclaim": name, "node": s.node, "instance_type": s.instance_type,
            "capacity_type": s.capacity_type, "zone": s.zone,
            "instance_id": instance_id(s.provider_id), "status": state,
            "nodepool": s.nodepool, "lifetime_seconds": lifetime_s,
        })

    if args.json:
        print(json.dumps(records, indent=2))
        return
    if not records:
        print("no karpenter nodes found (run some workloads, or `kexplain sync`)")
        return

    color = {"RUNNING": green, "DISRUPTED": red, "GONE": dim}
    rows = [[
        r["nodeclaim"], r["node"] or "-", r["instance_type"] or "?",
        r["capacity_type"] or "?", r["zone"] or "?", r["instance_id"] or "-",
        color[r["status"]](r["status"]),
        fmt_dur(r["lifetime_seconds"]) if r["lifetime_seconds"] is not None else "",
    ] for r in records]
    hdr = ["NODECLAIM", "NODE", "TYPE", "CAPACITY", "ZONE", "INSTANCE-ID", "STATUS", "LIFETIME"]
    widths = [max(len(str(r[i])) if not str(r[i]).startswith("\033") else len(re.sub(r"\033\[\d+m", "", str(r[i])))
                  for r in [hdr] + rows) for i in range(len(hdr))]
    def prow(r, is_hdr=False):
        cells = []
        for i, v in enumerate(r):
            plain = re.sub(r"\033\[\d+m", "", str(v))
            pad = " " * (widths[i] - len(plain))
            cells.append(str(v) + pad)
        line = "  ".join(cells)
        print(bold(line) if is_hdr else line)
    prow(hdr, True)
    for r in rows:
        prow(r)

def _history_events(store):
    """All provisioning/disruption events as dicts with ts (datetime), kind,
    and event-specific fields. Sorted by time. Shared by text and json output."""
    stories = build_stories(store)
    ev = []
    for r in store.logs():
        msg = r.get("message", "")
        ts = parse_ts(r.get("time"))
        if not ts:
            continue
        if msg == "found provisionable pod(s)":
            pods = [p.strip() for p in r.get("Pods", "").split(",") if p.strip()]
            ev.append({"ts": ts, "kind": "PENDING", "pods": pods})
        elif msg == "computed new nodeclaim(s) to fit pod(s)":
            ev.append({"ts": ts, "kind": "DECIDE", "pod_count": r.get("pods"),
                       "nodeclaim_count": r.get("nodeclaims")})
    for name, s in stories.items():
        if s.t_created:
            ev.append({"ts": s.t_created, "kind": "CREATE", "nodeclaim": name,
                       "nodepool": s.nodepool, "candidate_types": s.candidate_count})
        if s.t_launched:
            ev.append({"ts": s.t_launched, "kind": "LAUNCH", "nodeclaim": name,
                       "instance_type": s.instance_type,
                       "capacity_type": s.capacity_type, "zone": s.zone})
        if s.t_registered:
            ev.append({"ts": s.t_registered, "kind": "REGISTER", "nodeclaim": name,
                       "node": s.node})
        if s.t_initialized:
            ev.append({"ts": s.t_initialized, "kind": "READY", "nodeclaim": name})
        if s.disruption and s.disruption.get("ts"):
            d = s.disruption
            ev.append({"ts": d["ts"], "kind": "DISRUPT", "nodeclaim": name,
                       "reason": d.get("reason"), "decision": d.get("decision"),
                       "replacements": d.get("replacements", 0),
                       "savings_per_hour": d.get("savings")})
        if s.t_deleted:
            ev.append({"ts": s.t_deleted, "kind": "DELETE", "nodeclaim": name})
    ev.sort(key=lambda e: e["ts"])
    return ev

def _history_line(e):
    k = e["kind"]
    if k == "PENDING":
        pods = e["pods"]
        return (f'{yellow("PENDING")}    {len(pods)} unschedulable pod(s): '
                f'{", ".join(pods[:4])}{" …" if len(pods) > 4 else ""}')
    if k == "DECIDE":
        return (f'{cyan("DECIDE")}     fit {e["pod_count"]} pod(s) onto '
                f'{e["nodeclaim_count"]} new nodeclaim(s)')
    if k == "CREATE":
        return (f'{cyan("CREATE")}     nodeclaim {bold(e["nodeclaim"])} '
                f'(nodepool {e["nodepool"]}, {e["candidate_types"] or "?"} candidate types)')
    if k == "LAUNCH":
        return (f'{green("LAUNCH")}     {bold(e["nodeclaim"])} → {e["instance_type"]} '
                f'({e["capacity_type"]}) in {e["zone"]}')
    if k == "REGISTER":
        return f'{green("REGISTER")}   {e["nodeclaim"]} joined as node {e["node"]}'
    if k == "READY":
        return f'{green("READY")}      {e["nodeclaim"]} initialized'
    if k == "DISRUPT":
        sav = f', saves ${e["savings_per_hour"]:.2f}/hr' \
            if e.get("savings_per_hour") is not None else ""
        return (f'{magenta("DISRUPT")}    {bold(e["nodeclaim"])} via {e.get("reason") or "?"} '
                f'({e.get("decision") or "?"}, {e.get("replacements", 0)} replacement(s){sav})')
    if k == "DELETE":
        return f'{red("DELETE")}     nodeclaim {e["nodeclaim"]} removed'
    return k

def cmd_history(store, args):
    events = _history_events(store)
    if args.since:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=args.since)
        events = [e for e in events if e["ts"] >= cutoff]

    if args.json:
        out = [{**{k: v for k, v in e.items() if k != "ts"},
                "time": fmt_ts(e["ts"])} for e in events]
        print(json.dumps(out, indent=2))
        return
    if not events:
        print("no history yet")
        return
    last_day = None
    for e in events:
        day = e["ts"].strftime("%Y-%m-%d")
        if day != last_day:
            print(bold(f"\n── {day} " + "─" * 40))
            last_day = day
        print(f'{dim(e["ts"].strftime("%H:%M:%S"))}  {_history_line(e)}')

def _tree(lines):
    """lines: list of (depth, text). Renders box-drawing tree."""
    out = []
    for i, (depth, text) in enumerate(lines):
        if depth == 0:
            out.append(text)
            continue
        # is this the last line at this depth before a shallower one?
        last = True
        for d2, _ in lines[i + 1:]:
            if d2 < depth:
                break
            if d2 == depth:
                last = False
                break
        prefix = ""
        for d in range(1, depth):
            # does any later line exist at depth d? then vertical bar
            bar = False
            for d2, _ in lines[i + 1:]:
                if d2 < d:
                    break
                if d2 == d:
                    bar = True
                    break
            prefix += ("│  " if bar else "   ")
        prefix += "└─ " if last else "├─ "
        out.append(dim(prefix) + text)
    return "\n".join(out)

def resolve_target(stories, target):
    """target may be a node name, nodeclaim name, or instance id."""
    if target in stories:
        return stories[target]
    for s in stories.values():
        if s.node == target or instance_id(s.provider_id) == target:
            return s
    # prefix match
    matches = [s for n, s in stories.items() if n.startswith(target)]
    if len(matches) == 1:
        return matches[0]
    return None

def requirement_str(req):
    op = req.get("operator", "")
    vals = req.get("values", [])
    key = req["key"]
    if op == "In" and len(vals) == 1:
        return f"{key} = {vals[0]}"
    if op == "In":
        return f"{key} in [{', '.join(vals)}]"
    if op in ("Gt", "Lt", "Gte", "Lte"):
        sym = {"Gt": ">", "Lt": "<", "Gte": ">=", "Lte": "<="}[op]
        return f"{key} {sym} {vals[0]}"
    if op == "Exists":
        return f"{key} exists"
    return f"{key} {op} {vals}"

def _nodeclass_for(store, s, pool):
    """The EC2NodeClass snapshot backing this node's NodePool, or None."""
    ref = ((pool or {}).get("spec", {}).get("template", {}).get("spec", {})
           .get("nodeClassRef", {}) or {})
    name = ref.get("name")
    classes = store.objects("ec2nodeclasses")
    if name and name in classes:
        return classes[name]
    # fall back to the only class if there is exactly one
    return next(iter(classes.values())) if len(classes) == 1 else None

def ami_compatible_types(cat, nodeclass):
    """Types kept by AMI compatibility: a type survives if it matches at least
    one resolved AMI's requirements (Karpenter's provider-side pre-filter,
    aws/karpenter-provider-aws FilterForNodeClass). Returns None when the
    node class has no resolved AMIs in the store (nothing to compute against)."""
    amis = (nodeclass or {}).get("status", {}).get("amis", [])
    if not amis:
        return None
    def type_ok(info):
        for ami in amis:
            reqs = ami.get("requirements", [])
            if all(match_requirement(info, r) for r in reqs):
                return True
        return False
    return {n: i for n, i in cat.items() if type_ok(i)}

def zone_offerings(store, zones):
    """Set of instance types offered in any of `zones`, from a cached
    describe-instance-type-offerings. Returns None if we cannot determine it
    (no zones known, or no AWS access)."""
    if not zones:
        return None
    cache = os.path.join(store.dir, "offerings.json")
    offerings = {}
    if os.path.exists(cache) and \
       (datetime.now().timestamp() - os.path.getmtime(cache)) < EC2_CATALOG_TTL_S:
        with open(cache) as f:
            offerings = json.load(f)
    missing = [z for z in zones if z not in offerings]
    if missing:
        for z in missing:
            try:
                out = json.loads(sh(
                    "aws ec2 describe-instance-type-offerings "
                    "--location-type availability-zone "
                    f"--filters Name=location,Values={z} --output json", timeout=60))
                offerings[z] = sorted({o["InstanceType"]
                                       for o in out.get("InstanceTypeOfferings", [])})
            except Exception:
                return None  # no AWS access; skip the stage entirely
        with open(cache, "w") as f:
            json.dump(offerings, f)
    available = set()
    for z in zones:
        available.update(offerings.get(z, []))
    return available

def build_funnel(store, s, pool):
    """Recompute the constraint funnel: how each filter shrank the
    instance-type universe, ending at the type CreateFleet picked. Mirrors
    Karpenter's real order: AMI compatibility (provider pre-filter), then
    NodePool + pod requirements, resource fit, then zone offerings (folded
    into the scheduler's fits check). Returns list of
    (label, remaining_names, eliminated_count) stages."""
    try:
        cat = ec2_catalog(store)
    except Exception:
        return None
    if not cat:
        return None

    stages = []
    remaining = dict(cat)
    stages.append(("EC2 instance types in region", dict(remaining), 0))

    def keep(label, predicate):
        nonlocal remaining
        before = len(remaining)
        remaining = {n: i for n, i in remaining.items() if predicate(n, i)}
        stages.append((label, dict(remaining), before - len(remaining)))

    # AMI compatibility: provider-side pre-filter, before pod/pool requirements
    nodeclass = _nodeclass_for(store, s, pool)
    ami_types = ami_compatible_types(cat, nodeclass)
    if ami_types is not None:
        keep("AMI compatibility (EC2NodeClass resolved AMIs)",
             lambda n, i: n in ami_types)

    # one stage per constraint: pool requirements first, then pod-injected
    for source, r in collect_constraints(s, pool):
        label = "NodePool" if source.startswith("NodePool") else "pod constraint"
        keep(f'{label}: {requirement_str(r)}',
             lambda n, i, _r=r: match_requirement(i, _r))

    # resource fit (aggregated requests must fit with system overhead)
    reqs = s.requests if isinstance(s.requests, dict) else None
    if reqs:
        cpu = parse_quantity(reqs.get("cpu", 0))
        mem = parse_quantity(reqs.get("memory", 0))
        keep(f'resource fit: cpu≥{reqs.get("cpu")}, mem≥{reqs.get("memory")}'
             f' (after ~10-15% system overhead)',
             lambda n, i: (not cpu or i["cpu"] * CPU_ALLOCATABLE_RATIO >= cpu) and
                          (not mem or i["memory_mib"] * 2**20 * MEM_ALLOCATABLE_RATIO >= mem))

    # zone offerings: scheduler folds this into its fits check. Scope to the
    # node's actual zone if known, else the node class's subnet zones.
    zones = [s.zone] if s.zone else sorted({
        sub.get("zone") for sub in
        (nodeclass or {}).get("status", {}).get("subnets", []) if sub.get("zone")})
    offered = zone_offerings(store, zones)
    if offered is not None:
        zlabel = zones[0] if len(zones) == 1 else f'{len(zones)} zones'
        keep(f'offered in {zlabel}', lambda n, i: n in offered)
    return stages

def render_funnel(stages, s, L):
    """Append funnel stages to the explain tree lines."""
    L.append((1, bold("FUNNEL") + dim("  (recomputed from the live EC2 catalog)")))
    width0 = len(stages[0][1]) or 1
    BAR = FUNNEL_BAR_WIDTH
    for label, remaining, eliminated in stages:
        n = len(remaining)
        bar = "█" * max(1, int(BAR * n / width0)) if n else "·"
        drop = red(f'  −{eliminated}') if eliminated else ""
        L.append((2, f'{dim(bar.ljust(BAR))} {bold(str(n).rjust(4))}  {label}{drop}'))
        # name survivors when the set gets small
        if 0 < n <= FUNNEL_NAME_SURVIVORS_MAX and eliminated:
            L.append((3, dim(", ".join(sorted(remaining)))))
        elif eliminated and n <= FUNNEL_NAME_FAMILIES_MAX:
            fams = sorted({i["family"] for i in remaining.values()})
            L.append((3, dim(f'families: {", ".join(fams)}')))
    # reconciliation with what karpenter itself reported
    if s.candidate_count:
        n = len(stages[-1][1])
        if abs(n - s.candidate_count) <= max(3, n // 10):
            note = "matches"
        else:
            note = ("close; residual is instance-type overrides and "
                    "runtime capacity that only Karpenter sees")
        L.append((2, dim(f'karpenter itself reported {s.candidate_count} candidates ({note})')))
    if s.instance_type:
        bar = "▏"
        L.append((2, f'{dim(bar.ljust(FUNNEL_BAR_WIDTH))} {bold("   1")}  '
                     f'CreateFleet picks: {green(bold(s.instance_type))} '
                     f'({s.capacity_type}, {s.zone})'))

# claim requirements that either duplicate karpenter bookkeeping or are
# derived (the resolved type list, the chosen zone) rather than user intent
CLAIM_SKIP_KEYS = INTERNAL_REQ_KEYS | {
    "node.kubernetes.io/instance-type", "topology.kubernetes.io/zone",
}

def same_requirement(a, b):
    return a and b and a.get("operator") == b.get("operator") and \
        sorted(a.get("values", [])) == sorted(b.get("values", []))

def normalize_requirement(op, vals):
    """Canonical (operator, values) form for comparing a NodePool requirement
    with the claim's: karpenter rewrites Gt n as Gte n+1 and Lt n as Lte n-1
    when it resolves a nodeclaim."""
    try:
        if op == "Gt":
            return ("Gte", (str(int(vals[0]) + 1),))
        if op == "Lt":
            return ("Lte", (str(int(vals[0]) - 1),))
    except (ValueError, IndexError):
        pass
    return (op, tuple(sorted(vals)))

def collect_constraints(s, pool):
    """The full constraint list applied to this nodeclaim, with provenance.
    Returns [(source, requirement-dict)]: NodePool requirements first, then
    the pod-injected ones (claim requirements narrower than the pool's)."""
    out = []
    pool_reqs = {}
    if pool:
        for r in pool["spec"]["template"]["spec"].get("requirements", []):
            if r["key"] in INTERNAL_REQ_KEYS:
                continue
            pool_reqs[r["key"]] = r
            out.append((f'NodePool {pool["metadata"]["name"]}', r))
    for r in (s.raw_claim or {}).get("spec", {}).get("requirements", []):
        if r["key"] in CLAIM_SKIP_KEYS:
            continue
        if same_requirement(pool_reqs.get(r["key"]), r):
            continue  # identical to a pool requirement, already listed
        out.append(("pod constraints", r))
    return out

def cmd_why_not(store, s, pool, itype):
    """Explain why `itype` was not chosen for nodeclaim `s`."""
    cat = ec2_catalog(store)
    chosen = s.instance_type or "?"
    print(bold(f'\nWHY NOT {itype} for {s.node or s.name} '
               f'(chosen: {chosen})?\n'))

    info = cat.get(itype)
    if info is None:
        # maybe it's a family or a typo
        fam_matches = sorted(n for n in cat if n.split(".")[0] == itype)[:8]
        if fam_matches:
            print(f'  "{itype}" is a family, not a type. Try one of: '
                  f'{", ".join(fam_matches)}')
        else:
            close = sorted(n for n in cat if itype.split(".")[0][:2] in n)[:6]
            print(red(f'  ✗ {itype} does not exist in this region\'s EC2 catalog'))
            if close:
                print(dim(f'    similar available types: {", ".join(close)}'))
        return

    if not s.has_claim_spec:
        print(yellow('  note: the NodeClaim spec is missing from the store, so only '
                     'NodePool rules\n  can be checked; pod-injected constraints are '
                     'invisible here.\n'))
    constraints = collect_constraints(s, pool)
    failures = []
    for source, r in constraints:
        if not match_requirement(info, r):
            failures.append((source, r))

    # resource fit check
    fit_fail = None
    reqs = s.requests if isinstance(s.requests, dict) else None
    if reqs:
        cpu = parse_quantity(reqs.get("cpu", 0))
        mem = parse_quantity(reqs.get("memory", 0))
        if cpu and info["cpu"] * CPU_ALLOCATABLE_RATIO < cpu:
            fit_fail = (f'insufficient cpu: {info["cpu"]} vCPU (~{info["cpu"] * CPU_ALLOCATABLE_RATIO:.1f} '
                        f'allocatable) < {reqs.get("cpu")} requested')
        elif mem and info["memory_mib"] * 2**20 * MEM_ALLOCATABLE_RATIO < mem:
            fit_fail = (f'insufficient memory: {info["memory_mib"] / 1024:.1f} GiB '
                        f'(~{info["memory_mib"] * MEM_ALLOCATABLE_RATIO / 1024:.1f} allocatable) '
                        f'< {reqs.get("memory")} requested')

    if failures or fit_fail:
        n = len(failures) + (1 if fit_fail else 0)
        print(red(f'  ✗ ELIMINATED by {n} rule(s) before reaching CreateFleet:\n'))
        for source, r in failures:
            actual = {
                "kubernetes.io/arch": info["arch"],
                "karpenter.sh/capacity-type":
                    "/".join(x for x, ok in (("spot", info["spot"]), ("on-demand", info["od"])) if ok),
                "karpenter.k8s.aws/instance-category": info["category"],
                "karpenter.k8s.aws/instance-family": info["family"],
                "karpenter.k8s.aws/instance-generation": info["generation"],
                "karpenter.k8s.aws/instance-cpu": info["cpu"],
                "karpenter.k8s.aws/instance-memory": info["memory_mib"],
                "karpenter.k8s.aws/instance-size": info["size"],
                "karpenter.k8s.aws/instance-local-nvme": f'{info["nvme_gb"]} GB',
                "karpenter.k8s.aws/instance-cpu-manufacturer": info["manufacturer"],
                "karpenter.k8s.aws/instance-network-bandwidth": f'{info["bandwidth_mbps"]} Mbps',
            }.get(r["key"], "?")
            print(f'    {red("✗")} rule: {bold(requirement_str(r))}   {dim("(" + source + ")")}')
            print(f'      {itype} has: {yellow(str(actual))}\n')
        if fit_fail:
            print(f'    {red("✗")} rule: resource fit for pending pods   {dim("(aggregated requests)")}')
            print(f'      {yellow(fit_fail)}\n')
        return

    # survived all constraints, so it WAS a candidate
    print(green(f'  ✓ {itype} passed every constraint, so it WAS in the candidate set '
                f'sent to CreateFleet'))
    cap = s.capacity_type or "on-demand"
    print(f'\n  CreateFleet chose {bold(chosen)} over it. Why:')
    if cap == "spot":
        p_it = spot_price(itype, s.zone) if s.zone else None
        p_ch = spot_price(chosen, s.zone) if s.zone else None
        cmp_txt = ""
        if p_it and p_ch:
            cmp_txt = (f'\n    current spot: {itype} ~${p_it:.4f}/hr vs '
                       f'{chosen} ~${p_ch:.4f}/hr')
        print(f'    allocation strategy is price-capacity-optimized: EC2 ranks each\n'
              f'    (type, zone) spot pool by price AND depth/interruption risk.\n'
              f'    {itype} either priced higher or sat in a shallower pool at launch.'
              f'{cmp_txt}')
    else:
        ch_info = cat.get(chosen)
        if ch_info and (info["cpu"], info["memory_mib"]) > (ch_info["cpu"], ch_info["memory_mib"]):
            print(f'    allocation strategy is lowest-price: {chosen} '
                  f'({ch_info["cpu"]} vCPU/{ch_info["memory_mib"] / 1024:.0f} GiB) was smaller/cheaper\n'
                  f'    than {itype} ({info["cpu"]} vCPU/{info["memory_mib"] / 1024:.0f} GiB) '
                  f'while still fitting the pods.')
        else:
            print(f'    allocation strategy is lowest-price across the candidate set;\n'
                  f'    {chosen} priced lower in {s.zone or "the chosen zone"} at launch time.')
    print(dim(f'\n  note: zone offerings and AMI compatibility are also filtered at '
              f'runtime;\n  a type absent from {s.zone or "the zone"} would be dropped '
              f'even if it passes all rules.'))

def _section_header(s, live):
    state = green("RUNNING") if live else \
            (red("DISRUPTED/GONE") if (s.disruption or s.t_deleted) else dim("GONE"))
    L = [(0, f'\n{bold("NODE " + (s.node or "(never registered)"))}  '
             f'{dim("nodeclaim=" + s.name)}  [{state}]')]
    if s.provider_id:
        L.append((0, dim(f'     {s.provider_id}')))
    if not s.has_claim_spec:
        L.append((0, yellow('     partial data: the NodeClaim was deleted before this '
                            'store captured it.')))
        L.append((0, yellow('     Constraint attribution below is limited to what the '
                            'logs recorded; cron `kexplain sync` to avoid this.')))
    return L

MAX_TRIGGER_PODS_SHOWN = 8
MAX_POD_NAMES_SHOWN = 6

def _section_trigger(s):
    L = [(1, bold("TRIGGER") + (f'  @ {fmt_ts(s.t_created)}' if s.t_created else ""))]
    if s.replaces:
        sav = f' (est. savings ${s.replaces["savings"]:.2f}/hr)' \
            if s.replaces.get("savings") is not None else ""
        L.append((2, magenta(f'consolidation replacement: launched to replace '
                             f'{", ".join(s.replaces["nodes"])} '
                             f'via {s.replaces["reason"]}{sav}')))
    if s.trigger_pods:
        L.append((2, f'{len(s.trigger_pods)} unschedulable pod(s) could not fit on existing nodes:'))
        for p in s.trigger_pods[:MAX_TRIGGER_PODS_SHOWN]:
            L.append((3, cyan(p)))
        if len(s.trigger_pods) > MAX_TRIGGER_PODS_SHOWN:
            L.append((3, dim(f'… and {len(s.trigger_pods) - MAX_TRIGGER_PODS_SHOWN} more')))
        if s.trigger_reasons:
            L.append((2, "why they did not fit (from kube-scheduler FailedScheduling events):"))
            for reason, count in s.trigger_reasons:
                L.append((3, f'{yellow(reason)} {dim(f"(x{count})")}'))
    elif s.nominated_pods:
        L.append((2, f'pods nominated to this node: {", ".join(s.nominated_pods[:MAX_POD_NAMES_SHOWN])}'))
    else:
        L.append((2, dim("trigger pods unknown (logs may predate the local store)")))
    return L

def _section_constraints(s, pool):
    L = [(1, bold("CONSTRAINTS"))]
    if pool:
        L.append((2, f'NodePool {bold(s.nodepool)} requirements:'))
        for r in pool["spec"]["template"]["spec"].get("requirements", []):
            L.append((3, requirement_str(r)))
        lim = pool["spec"].get("limits")
        if lim:
            L.append((3, dim(f'limits: {json.dumps(lim)}')))
    elif s.nodepool:
        L.append((2, f'NodePool: {s.nodepool} (spec not in store)'))
    if s.raw_claim:
        creqs = s.raw_claim["spec"].get("requirements", [])
        interesting = [r for r in creqs if r["key"] not in INTERNAL_REQ_KEYS]
        # which requirements were injected by pod scheduling constraints
        # (i.e. not present with the same values in the NodePool template)?
        pool_reqs = {}
        if pool:
            for r in pool["spec"]["template"]["spec"].get("requirements", []):
                pool_reqs[r["key"]] = normalize_requirement(r.get("operator"), r.get("values", []))
        if interesting:
            L.append((2, 'Resolved NodeClaim requirements (NodePool ∩ pod constraints):'))
            for r in interesting:
                txt = requirement_str(r)
                if r["key"] == "node.kubernetes.io/instance-type" and len(r.get("values", [])) > 6:
                    txt = f'node.kubernetes.io/instance-type in [{len(r["values"])} types]'
                pr = pool_reqs.get(r["key"])
                from_pod = pool_reqs and \
                    pr != normalize_requirement(r.get("operator"), r.get("values", [])) \
                    and r["key"] != "node.kubernetes.io/instance-type"
                if from_pod:
                    txt += yellow("   ← narrowed by pod constraints")
                L.append((3, txt))
    if s.requests:
        L.append((2, f'Aggregated resource requests: '
                     f'{json.dumps(s.requests) if not isinstance(s.requests, str) else s.requests}'))
    return L

def _section_candidates(s):
    L = [(1, bold("CANDIDATES"))]
    if s.candidate_count:
        L.append((2, f'{bold(str(s.candidate_count))} instance types satisfied all constraints'))
        if s.candidate_types:
            L.append((2, f'sample candidates: ' + ", ".join(s.candidate_types[:10])
                         + dim(f'  (full set sent to CreateFleet)')))
    else:
        L.append((2, dim("candidate list unknown (created before log harvesting began)")))
    return L

def _cheaper_alternative_note(s):
    """Compare the chosen spot type's price with the cheapest candidates."""
    chosen_p = spot_price(s.instance_type, s.zone)
    alts = []
    for alt in [t for t in s.candidate_types if t != s.instance_type][:3]:
        p = spot_price(alt, s.zone)
        if p:
            alts.append((alt, p))
    if not (chosen_p and alts):
        return None
    cheaper = [(a, p) for a, p in alts if p < chosen_p]
    if cheaper:
        a, p = min(cheaper, key=lambda x: x[1])
        return yellow(
            f'why not cheaper? {a} spot is ~${p:.4f}/hr vs chosen '
            f'~${chosen_p:.4f}/hr. price-capacity-optimized weighs '
            f'interruption risk, not just price')
    return green('chosen type was also the cheapest spot offering among top candidates')

def _section_launch(s, args):
    L = [(1, bold("LAUNCH DECISION") + (f'  @ {fmt_ts(s.t_launched)}' if s.t_launched else ""))]
    if not s.instance_type:
        L.append((2, dim("launch details unknown")))
        return L
    cap = s.capacity_type or "?"
    want_prices = not args.no_prices and cap == "spot" and s.zone
    price_txt = ""
    if want_prices:
        p = spot_price(s.instance_type, s.zone)
        if p:
            price_txt = f' @ ~${p:.4f}/hr (current spot)'
    L.append((2, f'EC2 CreateFleet chose {bold(s.instance_type)} ({cap}) '
                 f'in {s.zone}{price_txt}'))
    strategy = ("price-capacity-optimized across spot offerings"
                if cap == "spot" else "lowest-price across on-demand offerings")
    L.append((2, f'allocation strategy: {strategy}'
                 + (f' ({s.candidate_count} types × zones in the request)' if s.candidate_count else "")))
    if want_prices and s.candidate_types:
        note = _cheaper_alternative_note(s)
        if note:
            L.append((2, note))
    if s.allocatable:
        alloc = s.allocatable if isinstance(s.allocatable, str) else json.dumps(s.allocatable)
        L.append((2, dim(f'allocatable: {alloc}')))
    return L

# generation at or above which we flag "latest generation" as premium
LATEST_GEN_THRESHOLD = 7

# Which karpenter requirement key demands each capability suffix or
# category. Traits without a matching key (z, e, b, q, f suffixes) can only
# be pinned via instance-family/instance-type requirements.
SUFFIX_REQ_KEYS = {
    "d": ("karpenter.k8s.aws/instance-local-nvme",),
    "n": ("karpenter.k8s.aws/instance-network-bandwidth",),
}
GPU_REQ_KEYS = ("karpenter.k8s.aws/instance-gpu-count",
                "karpenter.k8s.aws/instance-gpu-name",
                "karpenter.k8s.aws/instance-gpu-manufacturer",
                "karpenter.k8s.aws/instance-gpu-memory",
                "karpenter.k8s.aws/instance-accelerator-count",
                "karpenter.k8s.aws/instance-accelerator-name",
                "karpenter.k8s.aws/instance-accelerator-manufacturer")
GPU_CATEGORIES = frozenset({"p", "g", "gr", "dl", "inf", "trn", "f", "vt"})

def _premium_features(s):
    """Notable traits of the chosen type (accelerators, memory-optimized,
    storage-optimized categories; NVMe/network/etc. suffixes; latest
    generation) as (label, required?) pairs. required? is None when the
    claim spec is missing from the store, i.e. we cannot know whether a
    constraint demanded the trait."""
    feats = instance_features(s.instance_type)
    claim_reqs = (s.raw_claim or {}).get("spec", {}).get("requirements", [])
    req_keys = {r["key"] for r in claim_reqs}
    known = s.has_claim_spec

    def attributed(required):
        return required if known else None

    # a family/type pin in the claim covers ANY trait of the pinned family
    family_pinned = any(
        r["key"] in ("karpenter.k8s.aws/instance-family",
                     "node.kubernetes.io/instance-type")
        and r.get("operator") == "In"
        and any(v.split(".")[0] == feats["family"] for v in r.get("values", []))
        for r in claim_reqs)

    traits = []

    # 1. family category (compute-opt, memory-opt, GPU, storage-opt, HPC, ...)
    cat = feats["category"]
    if cat in NOTABLE_CATEGORIES:
        # an instance-category pin counts as demanding this category only when
        # it narrows away the general-purpose baselines and lands on this one
        cat_required = family_pinned or any(
            r["key"] == "karpenter.k8s.aws/instance-category"
            and r.get("operator") == "In"
            and set(r.get("values", [])) - GENERAL_PURPOSE_CATEGORIES == {cat[0]}
            for r in claim_reqs)
        if cat in GPU_CATEGORIES:
            cat_required = cat_required or any(k in req_keys for k in GPU_REQ_KEYS)
        if cat in ("r", "x", "u", "z"):  # memory-optimized profiles
            cat_required = cat_required or \
                "karpenter.k8s.aws/instance-memory" in req_keys
        if cat == "c":  # compute-optimized: a high vCPU floor is the tell
            cat_required = cat_required or \
                "karpenter.k8s.aws/instance-cpu" in req_keys
        label = f'{CATEGORY_MEANINGS.get(cat, cat)} family ({feats["family"]})'
        traits.append((label, attributed(cat_required)))

    # 2. capability suffix letters (d, n, e, z, b, q, f)
    for ch in feats["suffix"]:
        meaning = SUFFIX_MEANINGS.get(ch)
        if not meaning or ch in PROCESSOR_SUFFIXES:
            continue
        specific = SUFFIX_REQ_KEYS.get(ch, ())
        required = any(k in req_keys for k in specific) or family_pinned
        traits.append((f"{meaning} ('{ch}' variant)", attributed(required)))

    # 3. latest generation
    if feats["generation"] >= LATEST_GEN_THRESHOLD:
        gen_req = any(r["key"] == "karpenter.k8s.aws/instance-generation" and
                      r.get("operator") in ("Gt", "Gte") and
                      r.get("values") and float(r["values"][0]) >= LATEST_GEN_THRESHOLD - 1
                      for r in claim_reqs)
        traits.append((f'latest generation (gen {feats["generation"]})',
                       attributed(gen_req or family_pinned)))
    return traits

def _section_features(s):
    """Premium features of the chosen type: required by a constraint, just
    what CreateFleet picked, or unknown when the claim spec is gone?"""
    premium = _premium_features(s)
    if not premium:
        return []
    L = [(1, bold("FEATURE ATTRIBUTION") + dim("  (notable traits of the chosen type: asked for, or just picked?)"))]
    for label, required in premium:
        if required is None:
            L.append((2, f'{label}: {yellow("attribution unknown")}. The NodeClaim '
                         f'spec was deleted before this store saw it, so pod '
                         f'constraints cannot be checked'))
        elif required:
            L.append((2, f'{label}: {green("explicitly required")} by a scheduling constraint'))
        else:
            L.append((2, f'{label}: {yellow("NOT required by any constraint")}. '
                         f'CreateFleet picked it from the {s.candidate_count or "?"}-type '
                         f'candidate set ({"interruption-risk weighting" if s.capacity_type == "spot" else "it priced lowest at launch"})'))
    if any(req is False for _, req in premium):
        L.append((2, dim('to prevent: add NodePool requirements, e.g. '
                         'instance-local-nvme DoesNotExist, instance-generation Lt N, '
                         'or instance-family NotIn [...]')))
    if any(req is None for _, req in premium):
        L.append((2, dim('to keep future nodes explainable, cron `kexplain sync` '
                         '(see "Keeping history" in the readme)')))
    return L

def _section_lifecycle(s):
    L = [(1, bold("LIFECYCLE"))]
    steps = [(label, ts) for label, ts in (
        ("created", s.t_created),
        ("launched", s.t_launched),
        (f'registered as {s.node}', s.t_registered),
        ("initialized (ready)", s.t_initialized),
        ("deleted", s.t_deleted),
    ) if ts]
    for i, (label, ts) in enumerate(steps):
        delta = ""
        if i > 0:
            delta = dim(f'  (+{fmt_dur((ts - steps[i-1][1]).total_seconds())})')
        L.append((2, f'{fmt_ts(ts)}  {label}{delta}'))
    if s.t_created and s.t_initialized:
        L.append((2, green(f'pod-schedulable in {fmt_dur((s.t_initialized - s.t_created).total_seconds())} from decision')))
    if s.nominated_pods:
        L.append((2, f'pods scheduled here: {", ".join(s.nominated_pods[:MAX_POD_NAMES_SHOWN])}'
                     + (dim(f' … +{len(s.nominated_pods)-MAX_POD_NAMES_SHOWN}')
                        if len(s.nominated_pods) > MAX_POD_NAMES_SHOWN else "")))
    return L

def _section_disruption(s, live):
    L = [(1, bold("DISRUPTION"))]
    if s.disruption:
        d = s.disruption
        L.append((2, magenta(f'@ {fmt_ts(d.get("ts"))}: disrupted via '
                             f'{d.get("reason") or "?"} → {d.get("decision") or "?"}')))
        if d.get("savings") is not None:
            L.append((3, green(f'estimated savings: ${d["savings"]:.2f}/hr')))
        if d.get("replacements"):
            L.append((3, f'{d["replacements"]} replacement node(s) launched'))
        else:
            L.append((3, "no replacement (workload fits on remaining nodes)"))
        if d.get("pods") is not None:
            L.append((3, f'{d["pods"]} pod(s) rescheduled'))
    elif s.disruption_blocked:
        ts, msg = s.disruption_blocked[-1]
        L.append((2, yellow(f'disruption currently blocked: {msg} ({fmt_ts(ts)})')))
    elif live:
        L.append((2, dim("none. node is running and not marked for disruption")))
    else:
        L.append((2, dim("node is gone; no disruption decision captured in store")))
    return L

def cmd_explain(store, args):
    """Render the full decision trace for one node, section by section."""
    stories = build_stories(store)
    s = resolve_target(stories, args.target)
    if not s:
        sys.exit(f"error: no nodeclaim/node matching '{args.target}'. "
                 f"Try `kexplain nodes` to list known ones.")

    if args.json:
        print(json.dumps({k: (fmt_ts(v) if isinstance(v, datetime) else v)
                          for k, v in vars(s).items() if k != "raw_claim"},
                         indent=2, default=str))
        return

    pool = store.objects("nodepools").get(s.nodepool or "")
    if args.why_not:
        cmd_why_not(store, s, pool, args.why_not)
        return

    live = s.name in live_nodeclaims()
    L = _section_header(s, live)
    L += _section_trigger(s)
    L += _section_constraints(s, pool)
    L += _section_candidates(s)
    if not args.no_funnel:
        stages = build_funnel(store, s, pool)
        if stages:
            render_funnel(stages, s, L)
    L += _section_launch(s, args)
    if s.instance_type:
        L += _section_features(s)
    L += _section_lifecycle(s)
    L += _section_disruption(s, live)

    print(_tree(L))
    print()

def cmd_sync(store, args):
    sync(store, quiet=False)
    logs = store.logs()
    print(f"store: {store.dir}")
    print(f"  {len(logs)} log lines, {len(store.events())} events, "
          f"{len(store.objects('nodeclaims'))} nodeclaims, "
          f"{len(store.objects('nodes'))} nodes")

# ---------------------------------------------------------------- plan

def _parse_bandwidth(perf):
    """'25 Gigabit' → 25000; 'Up to 10 Gigabit' → 10000; else 0."""
    m = re.search(r"([\d.]+)\s*Gigabit", perf or "")
    return int(float(m.group(1)) * 1000) if m else 0

def ec2_catalog(store):
    cache = os.path.join(store.dir, "ec2-catalog-v3.json")
    if os.path.exists(cache) and \
       (datetime.now().timestamp() - os.path.getmtime(cache)) < EC2_CATALOG_TTL_S:
        with open(cache) as f:
            return json.load(f)
    print(dim("  fetching EC2 instance type catalog (cached 7 days)…"))
    out, token, types = None, None, []
    while True:
        cmd = ("aws ec2 describe-instance-types --output json "
               "--filters Name=supported-virtualization-type,Values=hvm ")
        if token:
            cmd += f"--starting-token {token} "
        out = json.loads(sh(cmd, timeout=120))
        types.extend(out["InstanceTypes"])
        token = out.get("NextToken")
        if not token:
            break
    cat = {}
    for t in types:
        name = t["InstanceType"]
        fam = name.split(".")[0]
        m = re.match(r"([a-z]+)(\d+)", fam)
        manuf = (t.get("ProcessorInfo", {}).get("Manufacturer") or "").lower()
        cat[name] = {
            "family": fam,
            "category": m.group(1) if m else fam,
            "generation": int(m.group(2)) if m else 0,
            "size": name.split(".", 1)[1] if "." in name else "",
            "arch": "arm64" if "arm64" in t.get("ProcessorInfo", {}).get("SupportedArchitectures", []) else "amd64",
            "cpu": t.get("VCpuInfo", {}).get("DefaultVCpus", 0),
            "memory_mib": t.get("MemoryInfo", {}).get("SizeInMiB", 0),
            "spot": "spot" in t.get("SupportedUsageClasses", []),
            "od": "on-demand" in t.get("SupportedUsageClasses", []),
            "nvme_gb": (t.get("InstanceStorageInfo", {}) or {}).get("TotalSizeInGB", 0)
                       if (t.get("InstanceStorageInfo", {}) or {}).get("Disks", [{}])[0].get("Type") == "ssd"
                       else 0,
            "manufacturer": ("intel" if "intel" in manuf else
                             "amd" if "amd" in manuf else
                             "aws" if "amazon" in manuf or "aws" in manuf else manuf),
            "bandwidth_mbps": _parse_bandwidth(t.get("NetworkInfo", {}).get("NetworkPerformance", "")),
            # accelerator presence, for matching AMI requirements like
            # "instance-gpu-count Exists" that gate GPU/inference AMIs
            "gpu_count": sum(g.get("Count", 0)
                             for g in (t.get("GpuInfo", {}) or {}).get("Gpus", [])),
            "accelerator_count": sum(a.get("Count", 0)
                                     for a in (t.get("InferenceAcceleratorInfo", {}) or {}).get("Accelerators", [])),
        }
    with open(cache, "w") as f:
        json.dump(cat, f)
    return cat

def match_requirement(info, req):
    key, op = req["key"], req.get("operator", "In")
    vals = req.get("values", [])
    actual = None
    if key == "kubernetes.io/arch":
        actual = info["arch"]
    elif key == "kubernetes.io/os":
        return True  # linux catalog only
    elif key == "karpenter.sh/capacity-type":
        return (("spot" in vals and info["spot"]) or
                ("on-demand" in vals and info["od"])) if op == "In" else True
    elif key == "karpenter.k8s.aws/instance-category":
        actual = info["category"][0]
    elif key == "karpenter.k8s.aws/instance-family":
        actual = info["family"]
    elif key == "karpenter.k8s.aws/instance-generation":
        actual = info["generation"]
    elif key == "karpenter.k8s.aws/instance-cpu":
        actual = info["cpu"]
    elif key == "karpenter.k8s.aws/instance-memory":
        actual = info["memory_mib"]
    elif key == "karpenter.k8s.aws/instance-size":
        actual = info["size"]
    elif key == "karpenter.k8s.aws/instance-local-nvme":
        if op == "DoesNotExist":
            return info["nvme_gb"] == 0
        if op == "Exists":
            return info["nvme_gb"] > 0
        actual = info["nvme_gb"]
    elif key == "karpenter.k8s.aws/instance-cpu-manufacturer":
        actual = info["manufacturer"]
    elif key == "karpenter.k8s.aws/instance-network-bandwidth":
        actual = info["bandwidth_mbps"]
    elif key in ("karpenter.k8s.aws/instance-gpu-count",
                 "karpenter.k8s.aws/instance-accelerator-count"):
        n = info.get("gpu_count", 0) if "gpu" in key else info.get("accelerator_count", 0)
        if op == "DoesNotExist":
            return n == 0
        if op == "Exists":
            return n > 0
        actual = n
    elif key == "node.kubernetes.io/instance-type":
        actual = None  # set by caller via name
    else:
        return True  # unknown key: don't filter
    if actual is None:
        return True
    if op == "In":
        return str(actual) in vals or actual in vals
    if op == "NotIn":
        return str(actual) not in vals
    try:
        if op == "Gt":
            return float(actual) > float(vals[0])
        if op == "Lt":
            return float(actual) < float(vals[0])
        if op == "Gte":
            return float(actual) >= float(vals[0])
        if op == "Lte":
            return float(actual) <= float(vals[0])
    except (ValueError, TypeError):
        return True
    if op == "Exists":
        return True
    return True

def parse_quantity(q):
    """k8s quantity → (cpu millicores | memory bytes) float, unit-agnostic."""
    q = str(q)
    units = {"m": 1e-3, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12,
             "Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40}
    m = re.match(r"^([\d.]+)([A-Za-z]*)$", q)
    if not m:
        return 0.0
    val, unit = float(m.group(1)), m.group(2)
    return val * units.get(unit, 1.0)

def cmd_plan(store, args):
    # read pod spec (yaml via kubectl's converter to avoid a yaml dependency)
    with open(args.file) as f:
        content = f.read()
    if content.lstrip().startswith("{"):
        pod = json.loads(content)
    else:
        # convert yaml → json without PyYAML: use kubectl create --dry-run
        p = subprocess.run(
            f"kubectl create --dry-run=client -o json -f {args.file}",
            shell=True, capture_output=True, text=True)
        if p.returncode != 0:
            sys.exit(f"error: could not parse {args.file}: {p.stderr.strip()}")
        pod = json.loads(p.stdout)
    if pod.get("kind") == "List":
        pod = pod["items"][0]
    spec = pod.get("spec", {})
    if pod.get("kind") in ("Deployment", "ReplicaSet", "Job", "StatefulSet", "DaemonSet"):
        spec = spec.get("template", {}).get("spec", {})

    # pod requirements
    cpu_req = mem_req = 0.0
    for ctr in spec.get("containers", []):
        r = ctr.get("resources", {}).get("requests", {})
        cpu_req += parse_quantity(r.get("cpu", 0))
        mem_req += parse_quantity(r.get("memory", 0))
    pod_reqs = []
    for k, v in (spec.get("nodeSelector") or {}).items():
        pod_reqs.append({"key": k, "operator": "In", "values": [v]})
    aff = ((spec.get("affinity") or {}).get("nodeAffinity") or {}) \
        .get("requiredDuringSchedulingIgnoredDuringExecution") or {}
    for term in aff.get("nodeSelectorTerms", [])[:1]:
        pod_reqs.extend(term.get("matchExpressions", []))

    cat = ec2_catalog(store)
    pools = store.objects("nodepools")
    if not pools:
        sync(store, quiet=True)
        pools = store.objects("nodepools")

    print(bold(f"\nPLAN for {pod.get('kind', 'Pod')}/"
               f"{pod.get('metadata', {}).get('name', '?')}"
               f"  (requests: cpu={cpu_req or '?'}, mem={int(mem_req / 2**20) if mem_req else '?'}Mi)"))
    for pname, pool in sorted(pools.items()):
        reqs = pool["spec"]["template"]["spec"].get("requirements", []) + pod_reqs
        # explicit instance-type pinning
        pinned = None
        for r in reqs:
            if r["key"] == "node.kubernetes.io/instance-type" and r.get("operator") == "In":
                pinned = set(r["values"])
        matches = []
        for name, info in cat.items():
            if pinned and name not in pinned:
                continue
            if all(match_requirement(info, r) for r in reqs):
                # must also fit the pod (leave ~10% headroom for daemonsets/system)
                if cpu_req and info["cpu"] * 1000 * CPU_ALLOCATABLE_RATIO < cpu_req * 1000:
                    continue
                if mem_req and info["memory_mib"] * 2**20 * MEM_ALLOCATABLE_RATIO < mem_req:
                    continue
                matches.append((name, info))
        # pod-level conflict check
        conflict = None
        for r in pod_reqs:
            pool_reqs = pool["spec"]["template"]["spec"].get("requirements", [])
            for pr in pool_reqs:
                if pr["key"] == r["key"] and pr.get("operator") == "In" and \
                   r.get("operator") == "In" and not set(pr["values"]) & set(r["values"]):
                    conflict = f'{r["key"]}: pod wants {r["values"]}, pool allows {pr["values"]}'
        print(f'\n  NodePool {bold(pname)}: ', end="")
        if conflict:
            print(red(f"INCOMPATIBLE: {conflict}"))
            continue
        if not matches:
            print(red("0 instance types fit"))
            continue
        matches.sort(key=lambda m: (m[1]["cpu"], m[1]["memory_mib"]))
        print(green(f"{len(matches)} candidate instance types"))
        cap_vals = ["spot", "on-demand"]
        for r in reqs:
            if r["key"] == "karpenter.sh/capacity-type" and r.get("operator") == "In":
                cap_vals = r["values"]
        print(dim(f'     capacity types: {", ".join(cap_vals)}; smallest that fit '
                  f'(Karpenter will pick cheapest via CreateFleet):'))
        for name, info in matches[:args.top]:
            print(f'     {name:<20} {info["cpu"]:>3} vCPU  '
                  f'{info["memory_mib"] / 1024:>7.1f} GiB  {info["arch"]}'
                  f'{"  spot✓" if info["spot"] and "spot" in cap_vals else ""}')
        if len(matches) > args.top:
            print(dim(f'     … and {len(matches) - args.top} more'))
    print()

# ---------------------------------------------------------------- wizard

def ask(prompt, options=None, default=None):
    """Numbered-menu or free-text prompt. Ctrl-C/Ctrl-D exits cleanly."""
    try:
        if options:
            print()
            for i, (label, _) in enumerate(options, 1):
                print(f'  {bold(str(i))}. {label}')
            while True:
                raw = input(f'\n{cyan(prompt)} [1-{len(options)}'
                            f'{", q to quit" if True else ""}]: ').strip().lower()
                if raw in ("q", "quit", "exit"):
                    sys.exit(0)
                if not raw and default is not None:
                    return options[default][1]
                if raw.isdigit() and 1 <= int(raw) <= len(options):
                    return options[int(raw) - 1][1]
                print(dim("  pick a number from the list, or q to quit"))
        else:
            raw = input(f'{cyan(prompt)}: ').strip()
            if raw.lower() in ("q", "quit", "exit"):
                sys.exit(0)
            return raw
    except (KeyboardInterrupt, EOFError):
        print()
        sys.exit(0)

def pick_node(stories, live_only=False, prompt="Which node?"):
    """Menu of known nodes, most recent first. Returns a NodeStory or None."""
    live = live_nodeclaims()
    items = sorted(stories.values(),
                   key=lambda s: s.t_created or datetime.min.replace(tzinfo=timezone.utc),
                   reverse=True)
    if live_only:
        items = [s for s in items if s.name in live]
    if not items:
        return None
    opts = []
    for s in items[:12]:
        state = "RUNNING" if s.name in live else ("disrupted" if s.disruption else "gone")
        detail = f'({s.instance_type or "?"}, {s.capacity_type or "?"}, {state})'
        opts.append((f'{s.node or s.name}  {dim(detail)}', s))
    return ask(prompt, opts)

def cmd_wizard(store, args):
    if not sys.stdin.isatty():
        sys.exit("wizard needs an interactive terminal. Agents should use the "
                 "direct commands instead: kexplain nodes/history/explain "
                 "(see AGENTS.md)")

    # health check first, so investigations start from a known-good setup
    ok, checks = run_checks(progress=status)
    status()
    if ok:
        soft = [c for c in checks if not c["ok"]]
        line = green("  ✓ setup looks good")
        if soft:
            line += yellow(f'  ({", ".join(c["check"] for c in soft)} missing, '
                           f'some detail will be reduced)')
        print(line + "\n")
    else:
        render_checks(ok, checks)
        cont = ask("Setup has problems. Continue anyway?", [
            ("Fix things first, exit the wizard", False),
            ("Continue with reduced functionality", True),
        ], default=0)
        if not cont:
            sys.exit(3)

    stories = build_stories(store)

    while True:
        goal = ask("What do you want to investigate?", [
            ("Why did I get this specific node / instance type?", "why_node"),
            ("Why did Karpenter pick an expensive / weird instance?", "expensive"),
            ("Why was a type I expected NOT chosen?", "why_not"),
            ("What happened in my cluster recently?", "history"),
            ("What would a new deployment get? (before the fact)", "plan"),
            ("Check my setup (doctor)", "doctor"),
        ])

        if goal == "why_node":
            s = pick_node(stories)
            if not s:
                print(yellow("\nno karpenter nodes found yet. Deploy something "
                             "unschedulable and re-run, or check kexplain doctor."))
                continue
            ns = argparse.Namespace(target=s.name, json=False, no_prices=False,
                                    no_funnel=False, why_not=None)
            cmd_explain(store, ns)

        elif goal == "expensive":
            s = pick_node(stories, prompt="Which node looks too expensive?")
            if not s:
                print(yellow("\nno karpenter nodes found yet."))
                continue
            print(dim("\nreading the trace top to bottom: CONSTRAINTS shows who "
                      "narrowed the choice, FEATURE ATTRIBUTION shows whether "
                      "premium features were asked for or just picked."))
            ns = argparse.Namespace(target=s.name, json=False, no_prices=False,
                                    no_funnel=False, why_not=None)
            cmd_explain(store, ns)
            follow = ask("Dig further?", [
                ("Compare against a type you expected instead", "why_not"),
                ("Back to the main menu", "menu"),
            ], default=1)
            if follow == "why_not":
                t = ask("Which instance type did you expect (e.g. m6g.large)?")
                if t:
                    pool = store.objects("nodepools").get(s.nodepool or "")
                    cmd_why_not(store, s, pool, t)

        elif goal == "why_not":
            s = pick_node(stories, prompt="For which node?")
            if not s:
                print(yellow("\nno karpenter nodes found yet."))
                continue
            t = ask("Which instance type should have been chosen (e.g. c5.large)?")
            if t:
                pool = store.objects("nodepools").get(s.nodepool or "")
                cmd_why_not(store, s, pool, t)

        elif goal == "history":
            hours = ask("How far back? ", [
                ("Last hour", 1.0), ("Last 24 hours", 24.0),
                ("Last week", 168.0), ("Everything in the store", None),
            ], default=1)
            cmd_history(store, argparse.Namespace(since=hours))

        elif goal == "plan":
            path = ask("Path to your pod/deployment yaml")
            if not path:
                continue
            if not os.path.exists(path):
                print(red(f"  file not found: {path}"))
                continue
            cmd_plan(store, argparse.Namespace(file=path, top=10))

        elif goal == "doctor":
            try:
                cmd_doctor(store, argparse.Namespace(json=False))
            except SystemExit:
                pass  # stay in the wizard

        again = ask("\nAnything else?", [
            ("Yes, back to the menu", True),
            ("No, done", False),
        ], default=0)
        if not again:
            print(dim("bye\n"))
            return

# ---------------------------------------------------------------- doctor

def preflight():
    """Fast basic checks before any real command. Returns a problem string,
    or None when good to go. Kept cheap: two kubectl calls, no AWS."""
    try:
        sh("kubectl config current-context", timeout=10)
    except Exception:
        return "no kubectl context (is kubectl installed and kubeconfig set?)"
    try:
        sh("kubectl get --raw /readyz", timeout=15)
    except Exception:
        return "cluster unreachable with current kubeconfig"
    try:
        pods = kubectl_json(f"get pods -n {KARPENTER_NS} "
                            f"-l app.kubernetes.io/name=karpenter")["items"]
        if not pods:
            return (f"no karpenter pods in namespace {KARPENTER_NS} "
                    f"(set KARPENTER_NAMESPACE if it runs elsewhere)")
    except Exception:
        return "cannot list pods (RBAC?)"
    return None

OPTIONAL_CHECKS = ("aws ec2 access", "debug logging")

def run_checks(progress=None):
    """Gather every prerequisite check. Returns (ok, checks) where ok ignores
    the optional ones. Pure data, no printing, no exiting. `progress` is an
    optional callable(str) invoked before each slow check."""
    checks = []

    def check(name, ok, detail, fix=None):
        checks.append({"check": name, "ok": bool(ok), "detail": detail,
                       **({"fix": fix} if fix and not ok else {})})

    def step(msg):
        if progress:
            progress(msg)

    # kubectl present
    step("checking kubectl...")
    try:
        v = sh("kubectl version --client -o json", timeout=15)
        check("kubectl", True, json.loads(v)["clientVersion"]["gitVersion"])
    except Exception as ex:
        check("kubectl", False, str(ex)[:120],
              "install kubectl and put it on PATH")

    # cluster reachable
    step("checking cluster access...")
    cluster = None
    try:
        ctx = sh("kubectl config current-context", timeout=15).strip()
        sh("kubectl get --raw /readyz", timeout=20)
        cluster = current_cluster()
        check("cluster access", True, f"context {ctx}")
    except Exception as ex:
        check("cluster access", False, str(ex)[:120],
              "aws eks update-kubeconfig --name <cluster> --region <region>")

    # karpenter installed, and is debug logging on
    if cluster:
        step("checking karpenter...")
        try:
            pods = kubectl_json(f"get pods -n {KARPENTER_NS} "
                                f"-l app.kubernetes.io/name=karpenter")["items"]
            if pods:
                ready = sum(1 for p in pods
                            if all(c.get("ready") for c in
                                   p.get("status", {}).get("containerStatuses", [])))
                check("karpenter", ready > 0,
                      f"{ready}/{len(pods)} controller pod(s) ready in {KARPENTER_NS}",
                      "kubectl describe the karpenter pods; see README Requirements")
            else:
                check("karpenter", False, f"no karpenter pods in {KARPENTER_NS}",
                      "export KARPENTER_NAMESPACE=<ns> if it runs elsewhere, "
                      "or install karpenter (see infra/create-cluster.sh)")
        except Exception as ex:
            check("karpenter", False, str(ex)[:120])

        step("checking nodepools...")
        try:
            pools = kubectl_json("get nodepools")["items"]
            check("nodepools", len(pools) > 0,
                  f'{len(pools)} nodepool(s): '
                  f'{", ".join(p["metadata"]["name"] for p in pools[:5])}' if pools
                  else "none found",
                  "apply a NodePool + EC2NodeClass; see infra/create-cluster.sh step 5")
        except Exception as ex:
            check("nodepools", False, str(ex)[:120],
                  "karpenter CRDs missing? is karpenter v1.x installed?")

        step("checking log level...")
        try:
            logs = sh(f"kubectl logs -n {KARPENTER_NS} "
                      f"-l app.kubernetes.io/name=karpenter --tail={KARPENTER_LOG_TAIL}", timeout=30)
            has_debug = '"DEBUG"' in logs
            check("debug logging", has_debug,
                  "DEBUG lines present" if has_debug else
                  "no DEBUG lines in recent logs (candidate lists will be missing)",
                  "helm upgrade karpenter ... --set logLevel=debug")
        except Exception:
            pass

    # aws cli + read-only EC2 access (optional but recommended)
    step("checking aws access...")
    try:
        sh("aws sts get-caller-identity", timeout=20)
        try:
            sh("aws ec2 describe-instance-types --max-items 1", timeout=30)
            check("aws ec2 access", True,
                  "DescribeInstanceTypes works (prices, funnel, plan, why-not enabled)")
        except Exception:
            check("aws ec2 access", False,
                  "credentials exist but ec2:DescribeInstanceTypes denied",
                  "grant read-only EC2 describe permissions; tool still works without")
    except Exception:
        check("aws ec2 access", False, "no aws credentials",
              "optional: configure aws CLI for prices/funnel/plan/why-not")

    # store writable
    try:
        if cluster:
            s = Store(cluster)
            probe = os.path.join(s.dir, ".probe")
            open(probe, "w").close()
            os.remove(probe)
            check("local store", True, s.dir)
    except Exception as ex:
        check("local store", False, str(ex)[:120],
              "set KEXPLAIN_STORE to a writable directory")

    ok = all(c["ok"] for c in checks if c["check"] not in OPTIONAL_CHECKS)
    return ok, checks

def render_checks(ok, checks):
    print()
    for c in checks:
        mark = green("✓") if c["ok"] else \
               (yellow("!") if c["check"] in OPTIONAL_CHECKS else red("✗"))
        print(f'  {mark} {bold(c["check"].ljust(16))} {c["detail"]}')
        if not c["ok"] and c.get("fix"):
            print(f'      {dim("fix: " + c["fix"])}')
    print()
    print(green("ready to explain") if ok else
          red("fix the failing checks above (aws / debug-logging are optional)"))

def cmd_doctor(store, args):
    """Check every prerequisite and report what works, what doesn't, and how
    to fix it. --json gives agents a machine-readable version."""
    ok, checks = run_checks()
    if args.json:
        print(json.dumps({"ok": ok, "checks": checks}, indent=2))
    else:
        render_checks(ok, checks)
    sys.exit(0 if ok else 3)

# ---------------------------------------------------------------- main

def main():
    global USE_COLOR
    ap = argparse.ArgumentParser(prog="kexplain",
                                 description="EXPLAIN plan for Karpenter decisions")
    ap.add_argument("--version", action="version",
                    version=f"kexplain {__version__} (alpha)")
    ap.add_argument("--no-color", action="store_true")
    ap.add_argument("--no-sync", action="store_true",
                    help="skip harvesting cluster state before running")
    sub = ap.add_subparsers(dest="cmd")

    sub.add_parser("sync", help="harvest logs/events/objects into local store")

    p = sub.add_parser("nodes", help="list karpenter nodes (live + historical)")
    p.add_argument("--live", action="store_true", help="only currently-running")
    p.add_argument("--json", action="store_true", help="machine-readable output")

    p = sub.add_parser("history", help="timeline of provisioning/disruption decisions")
    p.add_argument("--since", type=float, metavar="HOURS", help="only last N hours")
    p.add_argument("--json", action="store_true", help="machine-readable output")

    p = sub.add_parser("explain", help="decision trace for one node")
    p.add_argument("target", help="node name, nodeclaim name, or instance id")
    p.add_argument("--json", action="store_true")
    p.add_argument("--no-prices", action="store_true", help="skip spot price lookup")
    p.add_argument("--no-funnel", action="store_true", help="skip the constraint funnel view")
    p.add_argument("--why-not", metavar="INSTANCE_TYPE",
                   help="explain why this instance type was not chosen for the node")

    p = sub.add_parser("plan", help="before-the-fact: which instance types could a pod get")
    p.add_argument("-f", "--file", required=True, help="pod/deployment yaml or json")
    p.add_argument("--top", type=int, default=10)

    p = sub.add_parser("doctor", help="check prerequisites and cluster readiness")
    p.add_argument("--json", action="store_true", help="machine-readable output")

    sub.add_parser("wizard", help="interactive guided investigation")

    args = ap.parse_args()
    if args.no_color:
        USE_COLOR = False

    # bare `kexplain` on a terminal drops into the wizard; otherwise show help
    if not args.cmd:
        if sys.stdin.isatty() and sys.stdout.isatty():
            args.cmd = "wizard"
        else:
            ap.print_help()
            sys.exit(1)

    # doctor must run even with no kubeconfig at all
    if args.cmd == "doctor":
        print_logo("doctor")
        cmd_doctor(None, args)
        return

    # show the banner before any slow work so the tool never looks stuck
    print_logo(args.cmd if args.cmd != "wizard" else "interactive investigation")

    # preflight: if the basics are broken, fall through to doctor instead of
    # failing with a stack trace mid-command
    status("connecting to cluster...")
    problem = preflight()
    if problem:
        status()
        print(red(f"\n  preflight failed: {problem}"))
        print(dim("  running kexplain doctor for the full picture:\n"))
        cmd_doctor(None, argparse.Namespace(json=False))
        return  # doctor sys.exits with its own code

    store = Store(current_cluster())
    if args.cmd not in ("sync",) and not args.no_sync:
        status("harvesting karpenter logs and events...")
        try:
            sync(store, quiet=True)
        except Exception as ex:
            print(dim(f"  (sync skipped: {ex})"), file=sys.stderr)
    status()

    if args.cmd == "wizard":
        cmd_wizard(store, args)
        return

    {"sync": cmd_sync, "nodes": cmd_nodes, "history": cmd_history,
     "explain": cmd_explain, "plan": cmd_plan}[args.cmd](store, args)

if __name__ == "__main__":
    main()
