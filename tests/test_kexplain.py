"""Unit tests for kexplain. Run with:  python3 -m unittest discover tests

No cluster or AWS access needed; everything runs against fixtures captured
from a real Karpenter v1.13 cluster.
"""
import importlib.machinery
import importlib.util
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
TOOL = os.path.join(HERE, "..", "kexplain.py")

loader = importlib.machinery.SourceFileLoader("kexplain", TOOL)
spec = importlib.util.spec_from_loader("kexplain", loader)
kx = importlib.util.module_from_spec(spec)
loader.exec_module(kx)


# Real log lines captured from Karpenter v1.13 (identifiers anonymized).
FIXTURE_LOGS = [
    {"level": "INFO", "time": "2026-07-09T09:43:09.986Z",
     "message": "found provisionable pod(s)", "controller": "provisioner",
     "Pods": "default/inflate-aaa, default/inflate-bbb",
     "duration": "112.207066ms"},
    {"level": "INFO", "time": "2026-07-09T09:43:10.009Z",
     "message": "created nodeclaim", "controller": "provisioner",
     "NodePool": {"name": "default"}, "NodeClaim": {"name": "default-test1"},
     "requests": {"cpu": "5350m", "memory": "5376Mi", "pods": "9"},
     "instance-types": "c3.2xlarge, c3.4xlarge, c3.8xlarge and 595 other(s)"},
    {"level": "INFO", "time": "2026-07-09T09:43:14.159Z",
     "message": "launched nodeclaim", "NodeClaim": {"name": "default-test1"},
     "provider-id": "aws:///us-east-1b/i-0123456789abcdef0",
     "instance-type": "c8g.2xlarge", "zone": "us-east-1b",
     "capacity-type": "spot",
     "allocatable": {"cpu": "7910m", "memory": "14103Mi"}},
    {"level": "INFO", "time": "2026-07-09T09:43:29.000Z",
     "message": "registered nodeclaim", "NodeClaim": {"name": "default-test1"},
     "Node": {"name": "ip-10-0-0-1.ec2.internal"}},
    {"level": "INFO", "time": "2026-07-09T09:43:45.000Z",
     "message": "initialized nodeclaim", "NodeClaim": {"name": "default-test1"}},
    {"level": "INFO", "time": "2026-07-09T09:47:52.301Z",
     "message": "disrupting node(s)", "controller": "disruption",
     "command": "Empty/fecd7346: delete: nodepools=[default]: "
                "[ip-10-0-0-1.ec2.internal] (savings: $0.27)",
     "decision": "delete", "disrupted-node-count": 1,
     "replacement-node-count": 0, "pod-count": 0,
     "disrupted-nodes": [{"Node": {"name": "ip-10-0-0-1.ec2.internal"},
                          "NodeClaim": {"name": "default-test1"}}]},
    {"level": "INFO", "time": "2026-07-09T09:49:01.584Z",
     "message": "deleted nodeclaim", "NodeClaim": {"name": "default-test1"},
     "Node": {"name": "ip-10-0-0-1.ec2.internal"}},
]

FAKE_CATALOG = {
    "c5.large":    {"family": "c5", "category": "c", "generation": 5,
                    "size": "large", "arch": "amd64", "cpu": 2,
                    "memory_mib": 4096, "spot": True, "od": True,
                    "nvme_gb": 0, "manufacturer": "intel", "bandwidth_mbps": 10000},
    "c5d.large":   {"family": "c5d", "category": "c", "generation": 5,
                    "size": "large", "arch": "amd64", "cpu": 2,
                    "memory_mib": 4096, "spot": True, "od": True,
                    "nvme_gb": 50, "manufacturer": "intel", "bandwidth_mbps": 10000},
    "m6g.xlarge":  {"family": "m6g", "category": "m", "generation": 6,
                    "size": "xlarge", "arch": "arm64", "cpu": 4,
                    "memory_mib": 16384, "spot": True, "od": True,
                    "nvme_gb": 0, "manufacturer": "aws", "bandwidth_mbps": 10000},
    "t3.micro":    {"family": "t3", "category": "t", "generation": 3,
                    "size": "micro", "arch": "amd64", "cpu": 2,
                    "memory_mib": 1024, "spot": True, "od": True,
                    "nvme_gb": 0, "manufacturer": "intel", "bandwidth_mbps": 5000},
}


def make_store(logs=None):
    """A Store on a temp dir, optionally pre-loaded with log rows."""
    tmp = tempfile.mkdtemp(prefix="kexplain-test-")
    old = kx.STORE_ROOT
    kx.STORE_ROOT = tmp
    store = kx.Store("testcluster")
    kx.STORE_ROOT = old
    if logs:
        store.add_logs(logs)
    return store


class TestParsing(unittest.TestCase):
    def test_parse_ts_zulu(self):
        dt = kx.parse_ts("2026-07-09T09:43:09.986Z")
        self.assertEqual(dt.tzinfo, timezone.utc)
        self.assertEqual((dt.hour, dt.minute, dt.second), (9, 43, 9))

    def test_parse_ts_nanoseconds(self):
        self.assertIsNotNone(kx.parse_ts("2026-07-09T09:43:09.986123456Z"))

    def test_parse_ts_garbage(self):
        self.assertIsNone(kx.parse_ts("not-a-time"))
        self.assertIsNone(kx.parse_ts(""))
        self.assertIsNone(kx.parse_ts(None))

    def test_fmt_dur(self):
        self.assertEqual(kx.fmt_dur(45), "45s")
        self.assertEqual(kx.fmt_dur(150), "2m30s")
        self.assertEqual(kx.fmt_dur(7300), "2h01m")
        self.assertEqual(kx.fmt_dur(None), "?")

    def test_parse_quantity(self):
        self.assertEqual(kx.parse_quantity("500m"), 0.5)
        self.assertEqual(kx.parse_quantity("2"), 2.0)
        self.assertEqual(kx.parse_quantity("1Gi"), 2**30)
        self.assertEqual(kx.parse_quantity("512Mi"), 512 * 2**20)
        self.assertEqual(kx.parse_quantity("junk..."), 0.0)

    def test_parse_bandwidth(self):
        self.assertEqual(kx._parse_bandwidth("25 Gigabit"), 25000)
        self.assertEqual(kx._parse_bandwidth("Up to 10 Gigabit"), 10000)
        self.assertEqual(kx._parse_bandwidth(""), 0)

    def test_instance_features(self):
        f = kx.instance_features("c5d.2xlarge")
        self.assertEqual((f["family"], f["generation"]), ("c5d", 5))
        self.assertIn("d", f["suffix"])
        f = kx.instance_features("m8g.large")
        self.assertEqual(f["generation"], 8)
        self.assertNotIn("d", f["suffix"])

    def test_instance_id(self):
        self.assertEqual(kx.instance_id("aws:///us-east-1b/i-0abc"), "i-0abc")
        self.assertIsNone(kx.instance_id(None))

    def test_requirement_str(self):
        self.assertEqual(
            kx.requirement_str({"key": "k", "operator": "In", "values": ["a"]}),
            "k = a")
        self.assertEqual(
            kx.requirement_str({"key": "k", "operator": "In", "values": ["a", "b"]}),
            "k in [a, b]")
        self.assertEqual(
            kx.requirement_str({"key": "k", "operator": "Gt", "values": ["2"]}),
            "k > 2")
        self.assertEqual(
            kx.requirement_str({"key": "k", "operator": "Gte", "values": ["3"]}),
            "k >= 3")


class TestMatchRequirement(unittest.TestCase):
    def req(self, key, op, *vals):
        return {"key": key, "operator": op, "values": list(vals)}

    def test_arch(self):
        info = FAKE_CATALOG["m6g.xlarge"]
        self.assertTrue(kx.match_requirement(
            info, self.req("kubernetes.io/arch", "In", "arm64")))
        self.assertFalse(kx.match_requirement(
            info, self.req("kubernetes.io/arch", "In", "amd64")))

    def test_category(self):
        info = FAKE_CATALOG["t3.micro"]
        self.assertFalse(kx.match_requirement(
            info, self.req("karpenter.k8s.aws/instance-category", "In", "c", "m", "r")))

    def test_generation_gt_and_gte(self):
        info = FAKE_CATALOG["c5.large"]
        self.assertTrue(kx.match_requirement(
            info, self.req("karpenter.k8s.aws/instance-generation", "Gt", "2")))
        self.assertTrue(kx.match_requirement(
            info, self.req("karpenter.k8s.aws/instance-generation", "Gte", "5")))
        self.assertFalse(kx.match_requirement(
            info, self.req("karpenter.k8s.aws/instance-generation", "Gt", "5")))

    def test_local_nvme(self):
        with_nvme = FAKE_CATALOG["c5d.large"]
        without = FAKE_CATALOG["c5.large"]
        r = self.req("karpenter.k8s.aws/instance-local-nvme", "Gte", "10")
        self.assertTrue(kx.match_requirement(with_nvme, r))
        self.assertFalse(kx.match_requirement(without, r))
        r = self.req("karpenter.k8s.aws/instance-local-nvme", "DoesNotExist")
        self.assertFalse(kx.match_requirement(with_nvme, r))
        self.assertTrue(kx.match_requirement(without, r))

    def test_cpu_manufacturer(self):
        self.assertFalse(kx.match_requirement(
            FAKE_CATALOG["m6g.xlarge"],
            self.req("karpenter.k8s.aws/instance-cpu-manufacturer", "In", "intel")))

    def test_unknown_key_passes(self):
        self.assertTrue(kx.match_requirement(
            FAKE_CATALOG["c5.large"], self.req("some/unknown-label", "In", "x")))


class TestStore(unittest.TestCase):
    def test_log_dedup(self):
        store = make_store()
        self.assertEqual(store.add_logs(FIXTURE_LOGS), len(FIXTURE_LOGS))
        # same rows again: nothing new
        self.assertEqual(store.add_logs(FIXTURE_LOGS), 0)
        self.assertEqual(len(store.logs()), len(FIXTURE_LOGS))

    def test_logs_sorted_by_time(self):
        store = make_store(list(reversed(FIXTURE_LOGS)))
        times = [r["time"] for r in store.logs()]
        self.assertEqual(times, sorted(times))

    def test_snapshot_roundtrip(self):
        store = make_store()
        store.snapshot("nodepools", {"metadata": {"name": "default"},
                                     "spec": {"limits": {"cpu": 100}}})
        objs = store.objects("nodepools")
        self.assertEqual(objs["default"]["spec"]["limits"]["cpu"], 100)


class TestBuildStories(unittest.TestCase):
    def setUp(self):
        self.store = make_store(FIXTURE_LOGS)
        self.stories = kx.build_stories(self.store)
        self.s = self.stories["default-test1"]

    def test_full_lifecycle_parsed(self):
        s = self.s
        self.assertIsNotNone(s.t_created)
        self.assertIsNotNone(s.t_launched)
        self.assertIsNotNone(s.t_registered)
        self.assertIsNotNone(s.t_initialized)
        self.assertIsNotNone(s.t_deleted)
        self.assertLess(s.t_created, s.t_launched)

    def test_launch_details(self):
        s = self.s
        self.assertEqual(s.instance_type, "c8g.2xlarge")
        self.assertEqual(s.capacity_type, "spot")
        self.assertEqual(s.zone, "us-east-1b")
        self.assertEqual(s.node, "ip-10-0-0-1.ec2.internal")

    def test_trigger_pods_linked(self):
        self.assertIn("default/inflate-aaa", self.s.trigger_pods)

    def test_candidate_count_includes_others(self):
        # "c3.2xlarge, c3.4xlarge, c3.8xlarge and 595 other(s)" -> 598
        self.assertEqual(self.s.candidate_count, 598)

    def test_disruption_parsed_from_command(self):
        d = self.s.disruption
        self.assertEqual(d["reason"], "Empty")
        self.assertEqual(d["decision"], "delete")
        self.assertEqual(d["savings"], 0.27)
        self.assertEqual(d["replacements"], 0)

    def test_resolve_target(self):
        by_claim = kx.resolve_target(self.stories, "default-test1")
        by_node = kx.resolve_target(self.stories, "ip-10-0-0-1.ec2.internal")
        by_iid = kx.resolve_target(self.stories, "i-0123456789abcdef0")
        by_prefix = kx.resolve_target(self.stories, "default-te")
        self.assertTrue(by_claim is by_node is by_iid is by_prefix)
        self.assertIsNone(kx.resolve_target(self.stories, "nope"))


class TestFunnel(unittest.TestCase):
    def test_stages_shrink(self):
        store = make_store(FIXTURE_LOGS)
        # pre-seed the catalog cache so no AWS call happens
        with open(os.path.join(store.dir, "ec2-catalog-v2.json"), "w") as f:
            json.dump(FAKE_CATALOG, f)
        pool = {"metadata": {"name": "default"},
                "spec": {"template": {"spec": {"requirements": [
                    {"key": "karpenter.k8s.aws/instance-category",
                     "operator": "In", "values": ["c", "m", "r"]},
                    {"key": "karpenter.k8s.aws/instance-generation",
                     "operator": "Gt", "values": ["4"]},
                ]}}}}
        s = kx.build_stories(store)["default-test1"]
        stages = kx.build_funnel(store, s, pool)
        self.assertEqual(stages[0][0], "EC2 instance types in region")
        self.assertEqual(len(stages[0][1]), 4)
        counts = [len(remaining) for _, remaining, _ in stages]
        self.assertEqual(counts, sorted(counts, reverse=True),
                         "funnel must only ever shrink")
        # category drops t3, generation Gt 4 drops m6g (gen 6 stays), t3 gone
        final = stages[-1][1]
        self.assertNotIn("t3.micro", final)


class TestReplacementLinking(unittest.TestCase):
    def test_replacement_nodeclaim_links_to_disruption(self):
        logs = list(FIXTURE_LOGS)
        logs[5] = dict(logs[5])  # disrupting node(s) row
        logs[5]["command"] = ("Underutilized/aaa: replace: nodepools=[default]: "
                              "[ip-10-0-0-1.ec2.internal] (savings: $0.23)")
        logs[5]["decision"] = "replace"
        logs[5]["replacement-node-count"] = 1
        logs.append({"level": "INFO", "time": "2026-07-09T09:47:55.000Z",
                     "message": "created nodeclaim",
                     "NodePool": {"name": "default"},
                     "NodeClaim": {"name": "default-repl"},
                     "requests": {"cpu": "1350m"},
                     "instance-types": "c5.large, m6g.xlarge"})
        stories = kx.build_stories(make_store(logs))
        repl = stories["default-repl"]
        self.assertIsNotNone(repl.replaces)
        self.assertIn("default-test1", repl.replaces["nodes"])
        self.assertEqual(repl.replaces["reason"], "Underutilized")


if __name__ == "__main__":
    unittest.main()
