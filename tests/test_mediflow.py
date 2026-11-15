"""CPU fixtures exercise durable cursors, immutable references and public privacy."""
from contextlib import contextmanager
import errno
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import threading
import unittest
from unittest import mock

from mediflow import MediflowBridge, SCHEMA, DATA_SCHEMA, PREFIX
from simulator import DashboardError


def dump(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def manifest(**extra):
    return {"schema_version": SCHEMA, "campaign_id": "campaign-1", "job_id": "v6-first-seed-T1",
            "run_id": "run-1", "attempt_id": "attempt-1", "source": "mediflow_live", "producer": "mediflow.campaign",
            "model": "example/model", "task": "text", "seed": 42, "data_hash": "a" * 64, "scientific_recipe_hash": "b" * 64,
            "code_sha": "c" * 40, "telemetry_version": SCHEMA, "eval_protocol": "native-v1", "precision": "bf16",
            "total_updates": 2000, "samples_total": 7997, **extra}


def step(seq=1, m=None, **extra):
    m = m or manifest()
    return {**{k: m[k] for k in ("schema_version", "campaign_id", "job_id", "run_id", "attempt_id", "source", "producer", "model", "task")},
            "seq": seq, "emitted_at": "2026-11-15T00:00:00+00:00", "observed_at": "2026-11-15T00:00:00+00:00",
            "monotonic_ns": 1, "phase": "training", "provenance": "live_measured", "source_artifact": None,
            "optimizer_step": seq, "batch_examples": 13, "samples_processed": seq * 13, "train_loss": 1 / seq,
            "loss_objective": "example_mean_of_target_token_mean", "lr_used": 1e-4, "lr_next": 9e-5,
            "gradient_norm_before_clip": 1.2, "supervised_tokens": 90, "sequence_tokens": None,
            "step_wall_seconds": 0.4, "step_time_scope": "update_host_wall_no_extra_cuda_sync", "finite": True, **extra}


class MediflowTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.base = Path(self.tmp.name)
        self.root = self.base / "telemetry"
        self.root.mkdir()
        self.dbpath = self.base / "cache" / "read.sqlite3"
        self.attempt = self.root / "campaign" / "run" / "attempt"
        self.attempt.mkdir(parents=True)
        self.m = manifest()
        dump(self.attempt / "manifest.json", self.m)
        dump(self.attempt / "status.json", {"schema_version": SCHEMA, "status": "running", "phase": "training", "telemetry_complete": True})
        self.bridge = MediflowBridge(self.root, self.dbpath)

    def tearDown(self):
        self.bridge.close()
        self.tmp.cleanup()

    def get(self, path, **query):
        return self.bridge.dispatch("GET", PREFIX + path, query)

    def rid(self):
        return self.get("/runs")["runs"][0]["id"]

    def append(self, rows, stream="steps", newline=True):
        with (self.attempt / (stream + ".jsonl")).open("a", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + ("\n" if newline else ""))

    def assert_error(self, code, fn, status=None):
        with self.assertRaises(DashboardError) as context:
            fn()
        self.assertEqual(context.exception.code, "mediflow_" + code)
        if status:
            self.assertEqual(context.exception.status, status)
        self.assertNotIn(str(self.base), str(context.exception))

    def dataset(self, count=1205, with_index=True, expected_hash=True):
        directory = self.root / "datasets" / "frozen-text"
        directory.mkdir(parents=True, exist_ok=True)
        raw = b"".join((json.dumps({"sample_id": "sample-" + str(i), "source_id": "approved-source", "source_revision": "r1",
                                  "group_ids": ["group-" + str(i // 3)], "split": "train", "task_type": "text", "language": "ko",
                                  "freeze_id": "freeze-1", "license_ref": "license-1", "restrictions": ["research-only"],
                                  "derivation": {"method": "canonical", "version": "1", "raw_answer": "PRIVATE-DERIVATION"},
                                  "review_status": "reviewed", "image_sha256": "c" * 64,
                                  "input": "PRIVATE-PROMPT", "supervision": "PRIVATE-ANSWER", "path": "/home/private/image.png"}) + "\n").encode() for i in range(count))
        (directory / "records.metadata.jsonl").write_bytes(raw)
        m = {"schema_version": DATA_SCHEMA, "dataset_id": "frozen-text", "name": "Frozen text reference", "freeze_id": "freeze-1",
             "data_hash": "d" * 64, "record_count": count, "split_counts": {"train": count, "validation": 0, "test": 0},
             "sources": [{"source_id": "approved-source", "source_revision": "r1", "license_ref": "license-1", "restrictions": ["research-only"]}],
             "source_artifacts": [{"artifact_id": "canonical-source", "sha256": "e" * 64, "path": "/private"}],
             "frozen": True, "status": "verified", "provenance_preserved": True,
             "metadata_index": "records.metadata.jsonl" if with_index else None,
             "metadata_sha256": hashlib.sha256(raw).hexdigest() if expected_hash else None,
             "raw_path": "/home/private/raw.jsonl"}
        dump(directory / "manifest.json", m)
        return directory, m

    def test_mode_capabilities_and_no_training_dependencies(self):
        before = {str(p.relative_to(self.root)) for p in self.root.rglob("*")}
        health = self.get("/health")
        self.assertEqual(health["mode"], "mediflow")
        self.assertTrue(health["connected"])
        self.assertFalse(health["capabilities"]["resume"])
        self.assertEqual(before, {str(p.relative_to(self.root)) for p in self.root.rglob("*")})
        self.assert_error("read_only", lambda: self.bridge.dispatch("POST", PREFIX + "/runs/new/start", {}, {}), 405)
        self.assert_error("read_only", lambda: self.bridge.dispatch("DELETE", PREFIX + "/runs"), 405)

    def test_identity_mapping_and_manifest_allowlist(self):
        self.m.update(raw_prompt="PRIVATE-PROMPT", checkpoint_path="/home/private", artifacts=[{"artifact_id": "adapter-final", "kind": "checkpoint", "exists": True, "verified": True, "reload_verified": False, "selection": "final", "path": "/private"}])
        dump(self.attempt / "manifest.json", self.m)
        rid = self.rid()
        identity = [self.m[k] for k in ("campaign_id", "job_id", "run_id", "attempt_id")]
        expected = "mf-" + hashlib.sha256(json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()[:24]
        self.assertEqual(rid, expected)
        detail = self.get("/runs/" + rid)
        self.assertEqual(detail["manifest"]["scientific_recipe_hash"], "b" * 64)
        self.assertEqual(detail["artifacts"][0]["selection"], "final")
        serialized = json.dumps(detail)
        self.assertNotIn("PRIVATE", serialized)
        self.assertNotIn("/private", serialized)
        self.assertNotIn("checkpoint_path", serialized)

    def test_large_stream_paginated_no_1000_row_or_128k_source_limit(self):
        self.append([step(i) for i in range(1, 1506)])
        self.assertGreater((self.attempt / "steps.jsonl").stat().st_size, 128 * 1024)
        rid = self.rid()
        after, epoch, seen = 0, None, []
        while True:
            q = {"after_seq": str(after), "limit": "400"}
            if epoch:
                q["stream_epoch"] = epoch
            page = self.get("/runs/" + rid + "/steps", **q)
            seen.extend(x["seq"] for x in page["items"])
            after, epoch = page["next_after_seq"], page["stream_epoch"]
            if not page["has_more"]:
                break
        self.assertEqual(seen, list(range(1, 1506)))
        self.assertEqual(self.get("/runs/" + rid)["run"]["train_loss"], 1 / 1505)
        self.assertNotIn("steps", self.get("/runs/" + rid + "/export"))

    def test_partial_line_dedup_conflict_and_incomplete(self):
        first, second = step(1), step(2)
        self.append([first, first])
        encoded = json.dumps(second)
        with (self.attempt / "steps.jsonl").open("a") as f:
            f.write(encoded[:80])
        rid = self.rid()
        page = self.get("/runs/" + rid + "/steps")
        self.assertEqual([r["seq"] for r in page["items"]], [1])
        with (self.attempt / "steps.jsonl").open("a") as f:
            f.write(encoded[80:] + "\n")
        page = self.get("/runs/" + rid + "/steps", after_seq="1", stream_epoch=page["stream_epoch"])
        self.assertEqual(page["items"][0]["seq"], 2)
        self.append([step(2, train_loss=99)])
        page = self.get("/runs/" + rid + "/steps")
        self.assertEqual(page["items"][1]["train_loss"], 0.5)
        self.assertFalse(page["telemetry_complete"])
        self.assertIn("conflicting_sequence", self.get("/health")["errors"])

    def test_restart_offset_and_epoch_are_durable(self):
        self.append([step(1), step(2)])
        rid = self.rid()
        before = self.get("/runs/" + rid + "/steps", limit="1")
        self.bridge.close()
        self.bridge = MediflowBridge(self.root, self.dbpath)
        self.append([step(3)])
        after = self.get("/runs/" + rid + "/steps", after_seq="1", stream_epoch=before["stream_epoch"])
        self.assertEqual([x["seq"] for x in after["items"]], [2, 3])
        self.assertEqual(after["stream_epoch"], before["stream_epoch"])
        self.assertEqual(self.bridge._db.execute("SELECT count(*) FROM mf_records").fetchone()[0], 3)

    def test_rotation_reset_and_truncate_same_inode_detection(self):
        self.append([step(1), step(2)])
        rid = self.rid()
        before = self.get("/runs/" + rid + "/steps")
        source = self.attempt / "steps.jsonl"
        source.rename(source.with_suffix(".old"))
        self.append([step(1, train_loss=9)])
        self.assert_error("cursor_reset", lambda: self.get("/runs/" + rid + "/steps", after_seq="2", stream_epoch=before["stream_epoch"]), 409)
        reset = self.get("/runs/" + rid + "/steps")
        self.assertNotEqual(reset["stream_epoch"], before["stream_epoch"])
        self.assertEqual(reset["items"][0]["train_loss"], 9)
        # Rewritten same-inode file can already be larger before the next poll.
        source.write_text(json.dumps(step(1, train_loss=88, availability={"x": {"reason": "a" * 100}})) + "\n")
        self.assert_error("cursor_reset", lambda: self.get("/runs/" + rid + "/steps", stream_epoch=reset["stream_epoch"]), 409)

    def test_missing_stream_invalidates_cache(self):
        self.append([step(1)])
        rid = self.rid()
        old = self.get("/runs/" + rid + "/steps")
        (self.attempt / "steps.jsonl").unlink()
        self.assert_error("cursor_reset", lambda: self.get("/runs/" + rid + "/steps", stream_epoch=old["stream_epoch"]), 409)
        self.assertEqual(self.get("/runs/" + rid + "/steps")["items"], [])

    def test_separate_stream_cursors_and_producer_integrity(self):
        self.append([step(1)])
        event = step(1, event="checkpoint_saved", message="password=PRIVATE /home/private <script>", details={"raw_answer": "PRIVATE", "optimizer_step": 1, "path": "/private"})
        self.append([event], "events")
        resource = step(1, attribution="device_shared", tool_status="unavailable", gpu_utilization_percent=None,
                        availability={"gpu_utilization_percent": {"available": False, "reason": "nvml_unavailable", "provenance": "unmeasured"}})
        self.append([resource], "resources")
        rid = self.rid()
        a, b = self.get("/runs/" + rid + "/steps"), self.get("/runs/" + rid + "/events")
        self.assertNotEqual(a["stream_epoch"], b["stream_epoch"])
        self.assertEqual(b["items"][0]["message"], "checkpoint saved")
        self.assertEqual(b["items"][0]["details"], {"optimizer_step": 1})
        r = self.get("/runs/" + rid + "/resources")["items"][0]
        self.assertIsNone(r["gpu_utilization_percent"])
        self.assertEqual(r["availability"]["gpu_utilization_percent"]["reason"], "nvml_unavailable")
        self.append([step(2, producer="different-producer")])
        self.assertEqual(len(self.get("/runs/" + rid + "/steps")["items"]), 1)
        self.assertIn("producer_mismatch", self.get("/health")["errors"])

    def test_bad_envelope_seq_gap_and_nonfinite(self):
        self.append([step(1, run_id="another-run"), step(2, train_loss=float("nan"), lr_used=float("inf"))])
        rid = self.rid()
        page = self.get("/runs/" + rid + "/steps")
        self.assertEqual(len(page["items"]), 1)
        self.assertIsNone(page["items"][0]["train_loss"])
        self.assertEqual(page["items"][0]["availability"]["train_loss"]["reason"], "nonfinite_or_invalid")
        self.assertFalse(page["telemetry_complete"])
        json.dumps(page, allow_nan=False)

    def test_duplicate_json_keys_are_rejected(self):
        text = json.dumps(step(1))[:-1] + ',"seq":2}\n'
        (self.attempt / "steps.jsonl").write_text(text)
        rid = self.rid()
        self.assertEqual(self.get("/runs/" + rid + "/steps")["items"], [])
        self.assertIn("invalid_stream_record", self.get("/health")["errors"])

    def test_future_manifest_is_discovered_without_restart(self):
        self.assertEqual(self.get("/runs")["total"], 1)
        other = self.root / "future" / "run" / "attempt"
        dump(other / "manifest.json", manifest(campaign_id="future-campaign", run_id="future-run"))
        result = self.get("/runs", campaign_id="future-campaign")
        self.assertEqual(result["total"], 1)
        self.assertEqual(result["runs"][0]["run_id"], "future-run")
        (other / "manifest.json").unlink()
        self.assertEqual(self.get("/runs")["total"], 1)

    def test_historical_and_terminal_not_stale(self):
        self.assertTrue(self.get("/runs")["runs"][0]["stale"] is False)
        dump(self.attempt / "status.json", {"status": "running", "phase": "training", "last_emitted_at": "2001-01-01T00:00:00Z"})
        self.assertTrue(self.get("/runs")["runs"][0]["stale"])
        self.m["source"] = "mediflow_historical"
        dump(self.attempt / "manifest.json", self.m)
        self.assertFalse(self.get("/runs")["runs"][0]["stale"])
        dump(self.attempt / "status.json", {"status": "succeeded", "phase": "succeeded", "last_emitted_at": "2001-01-01T00:00:00Z"})
        self.assertFalse(self.get("/runs")["runs"][0]["stale"])

    def test_campaign_first_seed_distinct_verified_jobs(self):
        for name, job, training, reload in [("a", "v6-first-seed-T1", True, True), ("b", "v6-first-seed-T1", True, True), ("c", "v6-first-seed-T2", True, False), ("d", "v6-smoke-T3", True, True)]:
            target = self.root / "c" / name / "a"
            dump(target / "manifest.json", manifest(job_id=job, run_id=name))
            dump(target / "status.json", {"status": "succeeded", "training_succeeded": training, "reload_succeeded": reload})
        campaign = self.get("/campaigns")["campaigns"][0]
        self.assertEqual(campaign["first_seed_completed"], 1)
        self.assertEqual(campaign["first_seed_expected"], 10)

    def test_pagination_and_logical_id_validation(self):
        for q in ({"limit": "0"}, {"limit": "501"}, {"offset": "-1"}, {"limit": "NaN"}, {"limit": True}, {"offset": ["0"]}):
            self.assert_error("invalid_request", lambda q=q: self.get("/runs", **q))
        for path in ("/runs/../../private", "/datasets/%2e%2e/records", "/datasets/foo\\bar/records", "/runs//foo"):
            self.assert_error("invalid_request", lambda path=path: self.get(path))
        self.assert_error("not_found", lambda: self.get("/runs/mf-" + "a" * 24), 404)
        rid = self.rid()
        self.assert_error("invalid_request", lambda: self.get("/runs/" + rid + "/steps", limit="1001"))
        self.assert_error("cursor_reset", lambda: self.get("/runs/" + rid + "/steps", after_seq="1"), 409)

    def test_reference_registration_does_not_scan_metadata(self):
        directory, m = self.dataset()
        with mock.patch.object(self.bridge, "_metadata_page", side_effect=AssertionError("should not scan")):
            page = self.get("/datasets")
            result = self.bridge.dispatch("POST", PREFIX + "/datasets/register", {}, {"manifest_id": "frozen-text"})
        self.assertEqual(page["datasets"][0]["record_count"], 1205)
        self.assertTrue(result["registered"])
        self.assertEqual(self.bridge._db.execute("SELECT count(*) FROM mf_metadata").fetchone()[0], 0)
        for payload in ({"manifest_id": "../private"}, {"manifest_id": "/tmp/raw"}, {"manifest_id": "frozen-text", "path": "/private"}, {"manifest_id": "frozen-text", "split": "test"}, {"manifest_id": ["frozen-text"]}):
            self.assert_error("invalid_request", lambda payload=payload: self.bridge.dispatch("POST", PREFIX + "/datasets/register", {}, payload))

    def test_large_metadata_index_pages_restart_and_privacy(self):
        directory, m = self.dataset(2207)
        self.assertGreater((directory / "records.metadata.jsonl").stat().st_size, 128 * 1024)
        page = self.get("/datasets/frozen-text/records", offset="1000", limit="500")
        self.assertTrue(page["availability"]["available"])
        self.assertEqual(page["total"], 2207)
        self.assertEqual(page["records"][0]["sample_id"], "sample-1000")
        self.assertEqual(page["records"][-1]["sample_id"], "sample-1499")
        self.assertEqual(page["records"][0]["derivation"], {"method": "canonical", "version": "1"})
        self.assertNotIn("PRIVATE", json.dumps(page))
        self.bridge.close()
        self.bridge = MediflowBridge(self.root, self.dbpath)
        with mock.patch.object(self.bridge, "_file_hash", side_effect=AssertionError("do not rehash unchanged index")):
            next_page = self.get("/datasets/frozen-text/records", offset="2000", limit="500")
        self.assertEqual(len(next_page["records"]), 207)
        self.assertFalse(next_page["has_more"])
        conn = sqlite3.connect(self.dbpath)
        # SQLite cache itself also contains sanitized values only.
        for table, col in (("mf_files", "value"), ("mf_datasets", "manifest")):
            self.assertNotIn("PRIVATE", str(conn.execute("SELECT " + col + " FROM " + table).fetchall()))
        conn.close()

    def test_reference_registration_persists_separate_from_verification(self):
        self.dataset(5)
        self.assertFalse(self.get("/datasets")["datasets"][0]["registered"])
        self.bridge.dispatch("POST", "/api/mediflow/datasets/register", payload={"manifest_id":"frozen-text"})
        self.assertTrue(self.get("/datasets")["datasets"][0]["registered"])
        self.bridge.close()
        self.bridge = MediflowBridge(self.root, self.dbpath)
        self.assertTrue(self.get("/datasets")["datasets"][0]["registered"])

    def test_metadata_hash_count_and_mutation_are_rejected(self):
        directory, m = self.dataset(5)
        m["metadata_sha256"] = "f" * 64
        dump(directory / "manifest.json", m)
        p = self.get("/datasets/frozen-text/records")
        self.assertFalse(p["availability"]["available"])
        self.assertEqual(p["availability"]["reason"], "metadata_hash_mismatch")
        self.assertEqual(p["records"], [])

    def test_metadata_count_mismatch(self):
        directory, m = self.dataset(5)
        m["record_count"] = 6
        dump(directory / "manifest.json", m)
        self.assertEqual(self.get("/datasets/frozen-text/records")["availability"]["reason"], "metadata_count_mismatch")

    def test_immutable_metadata_mutation(self):
        directory, _ = self.dataset(5)
        self.assertTrue(self.get("/datasets/frozen-text/records")["availability"]["available"])
        with (directory / "records.metadata.jsonl").open("a") as f:
            f.write('{}\n')
        p = self.get("/datasets/frozen-text/records")
        self.assertEqual(p["availability"]["reason"], "immutable_metadata_changed")
        self.assertEqual(p["records"], [])

    def test_reference_without_index_and_empty_index(self):
        self.dataset(1205, with_index=False)
        p = self.get("/datasets/frozen-text/records")
        self.assertFalse(p["availability"]["available"])
        self.assertEqual(p["total"], 1205)
        directory, m = self.dataset(0)
        dump(directory / "manifest.json", m)
        p = self.get("/datasets/frozen-text/records")
        self.assertTrue(p["availability"]["available"])
        self.assertEqual(p["records"], [])

    def test_dataset_symlink_manifest_and_index_escape(self):
        directory, _ = self.dataset(2)
        outside = self.base / "private.json"
        outside.write_text('{"raw_prompt":"PRIVATE"}')
        (directory / "records.metadata.jsonl").unlink()
        (directory / "records.metadata.jsonl").symlink_to(outside)
        self.assertFalse(self.get("/datasets/frozen-text/records")["availability"]["available"])
        (directory / "manifest.json").unlink()
        (directory / "manifest.json").symlink_to(outside)
        self.assertEqual(self.get("/datasets")["datasets"], [])
        escaped = self.root / "datasets" / "escape"
        escaped.symlink_to(self.base, target_is_directory=True)
        self.assertEqual(self.get("/datasets")["datasets"], [])

    def test_attempt_symlink_and_stream_escape(self):
        self.append([step(1)])
        rid = self.rid()
        self.assertEqual(len(self.get("/runs/" + rid + "/steps")["items"]), 1)
        outside = self.base / "private-stream.jsonl"
        outside.write_text(json.dumps(step(2, train_loss=999)) + "\n")
        (self.attempt / "steps.jsonl").unlink()
        (self.attempt / "steps.jsonl").symlink_to(outside)
        page = self.get("/runs/" + rid + "/steps")
        self.assertEqual(page["items"], [])
        self.assertFalse(page["telemetry_complete"])
        (self.attempt / "manifest.json").unlink()
        (self.attempt / "manifest.json").symlink_to(self.base / "missing")
        self.assertEqual(self.get("/runs")["runs"], [])

    def test_evaluation_aggregate_allowlist_and_bounded_pages(self):
        ev = {"evaluation_id": "evaluation-1", "run_id": "run-1", "variant": "tuned", "dataset_id": "frozen-text", "task": "vqa", "split": "test",
              "protocol": "native", "protocol_version": "1", "image_condition": "no_image", "renderer_version": "native-v1", "data_hash": "d" * 64, "model": "example/model", "precision": "bf16", "cohort_id": "cohort-1",
              "status": "partial", "scheduled": 10, "attempted": 8, "valid": 6, "invalid": 2, "failed": 0, "unattempted": 2,
              "metrics": {"coverage": 0.8, "answered_accuracy": 0.5, "clinical_accuracy": None, "raw_predictions": ["PRIVATE"],
                          "e2e_latency_seconds": {"n": 8, "median": 0.4, "scope": "generation_e2e", "raw_text": "PRIVATE"},
                          "statuses": {"valid": 6, "invalid": 2}, "ci95": [0.1, 0.8]},
              "subgroups": [{"label": "OPEN", "metrics": {"lexical_token_f1": 0.2, "prompt": "PRIVATE"}}],
              "source_artifact": {"artifact_id": "aggregate-evaluation", "sha256": "e" * 64, "path": "/private"},
              "provenance": "historical_reconstructed", "receipt_verified": None, "predictions": ["PRIVATE"]}
        dump(self.attempt / "evaluations.json", {"evaluations": [{**ev, "evaluation_id": "evaluation-" + str(i)} for i in range(502)]})
        rid = self.rid()
        result = self.get("/runs/" + rid + "/evaluations", limit="100", offset="400")
        self.assertEqual(len(result["evaluations"]), 100)
        self.assertEqual(result["total"], 502)
        self.assertTrue(result["has_more"])
        first = result["evaluations"][0]
        self.assertEqual(first["attempted"], 8)
        self.assertEqual(first["image_condition"], "no_image")
        self.assertEqual(first["renderer_version"], "native-v1")
        self.assertEqual(first["metrics"]["answered_accuracy"], 0.5)
        self.assertEqual(first["subgroups"][0]["label"], "OPEN")
        self.assertNotIn("PRIVATE", json.dumps(result))
        export = self.get("/runs/" + rid + "/export", format="csv")
        self.assertFalse(export["synthetic"])
        self.assertEqual(export["evaluations"][0]["receipt_verified"], None)
        json.dumps(export, allow_nan=False)
        # Repeated requests use sanitized cache without losing numeric metrics.
        self.assertEqual(self.get("/runs/" + rid + "/evaluations")["evaluations"][0]["metrics"]["coverage"], 0.8)

    def test_status_summary_and_diagnostics_redaction(self):
        dump(self.attempt / "status.json", {"status": "failed", "phase": "failed", "telemetry_error": "password=PRIVATE /home/private", "traceback": "PRIVATE", "gaps": [{"stream": "steps", "from_seq": 1, "to_seq": 3, "reason": "/private"}]})
        dump(self.attempt / "summary.json", {"train_loss_mean": 0.4, "wall_seconds": 55, "wall_time_scope": "legacy_attempt_wall", "peak_allocated_bytes": 1234, "raw_losses": [9], "reconciliation": {"status": "passed", "checks": {"count": True, "raw_prompt": "PRIVATE"}}})
        result = self.get("/runs/" + self.rid())
        self.assertEqual(result["status"]["telemetry_error"], "unavailable_or_redacted")
        self.assertEqual(result["summary"]["wall_time_scope"], "legacy_attempt_wall")
        self.assertNotIn("PRIVATE", json.dumps(result))
        self.assertIsNone(result["run"]["train_loss"])

    def test_canonical_provenance_dicts_and_public_license_references(self):
        directory, m = self.dataset(1)
        raw = {"sample_id": "source/revision/id", "source_id": "source", "source_revision": "revision",
               "group_ids": {"patient": None, "study": "study-1", "document": "https://example.org/document/1", "image": "image-hash", "parent": "parent-1", "duplicate_cluster": "cluster-1", "raw_answer": "PRIVATE"},
               "restrictions": {"raw_redistribution": False, "research_only": True, "sft_allowed": True, "dataset_license": "CC-BY-4.0", "attribution_required": True, "raw_answer": "PRIVATE"},
               "license_ref": "https://example.org/LICENSE", "derivation": "original", "review_status": "machine_checked"}
        payload = (json.dumps(raw) + "\n").encode()
        (directory / "records.metadata.jsonl").write_bytes(payload)
        m["metadata_sha256"] = hashlib.sha256(payload).hexdigest()
        m["sources"][0].update(license_ref=raw["license_ref"], restrictions=raw["restrictions"])
        dump(directory / "manifest.json", m)
        result = self.get("/datasets/frozen-text/records")["records"][0]
        self.assertEqual(result["group_ids"]["study"], "study-1")
        self.assertEqual(result["group_ids"]["document"], "https://example.org/document/1")
        self.assertIsNone(result["group_ids"]["patient"])
        self.assertTrue(result["restrictions"]["sft_allowed"])
        self.assertEqual(result["license_ref"], raw["license_ref"])
        self.assertEqual(result["derivation"], "original")
        source = self.get("/datasets")["datasets"][0]["sources"][0]
        self.assertEqual(source["license_ref"], raw["license_ref"])
        self.assertEqual(source["restrictions"]["dataset_license"], "CC-BY-4.0")
        self.assertNotIn("PRIVATE", json.dumps(result))

    def test_evaluator_dimension_and_value_preserved_for_ui_label(self):
        dump(self.attempt / "evaluations.json", {"evaluations": [{"evaluation_id": "ev-1", "status": "partial",
             "subgroups": [{"source_id": "slake", "dimension": "answer_type", "value": "OPEN", "metrics": {"lexical_token_f1": 0.8}}]}]})
        subgroup = self.get("/runs/" + self.rid() + "/evaluations")["evaluations"][0]["subgroups"][0]
        self.assertEqual(subgroup["label"], "OPEN")
        self.assertEqual(subgroup["dimension"], "answer_type")
        self.assertEqual(subgroup["value"], "OPEN")
        self.assertEqual(subgroup["source_id"], "slake")

    def test_metadata_index_incremental_and_restart(self):
        self.dataset(80)
        with mock.patch("mediflow.INGEST_BYTES", 2048):
            first = self.get("/datasets/frozen-text/records", offset="70")
            self.assertEqual(first["availability"]["reason"], "metadata_indexing_in_progress")
            indexed = self.bridge._db.execute("SELECT count FROM mf_metadata_state").fetchone()[0]
            self.assertGreater(indexed, 0)
            self.assertLess(indexed, 80)
            self.bridge.close()
            self.bridge = MediflowBridge(self.root, self.dbpath)
            for _ in range(80):
                page = self.get("/datasets/frozen-text/records", offset="70")
                if page["availability"]["available"]:
                    break
            self.assertTrue(page["availability"]["available"])
            self.assertEqual(len(page["records"]), 10)
            self.assertEqual(page["records"][0]["sample_id"], "sample-70")
            self.assertEqual(self.bridge._db.execute("SELECT count(*) FROM mf_metadata").fetchone()[0], 80)

    def test_invalid_metadata_after_valid_prefix_stays_unavailable(self):
        directory, m = self.dataset(2, expected_hash=False)
        (directory / "records.metadata.jsonl").write_text('{"sample_id":"valid"}\nnot-json\n')
        first = self.get("/datasets/frozen-text/records")
        second = self.get("/datasets/frozen-text/records")
        self.assertEqual(first["records"], [])
        self.assertEqual(second["availability"]["reason"], "invalid_metadata_index")

    def test_partial_truncation_detected_before_committed_offset_changes(self):
        self.append([step(1)])
        with (self.attempt / "steps.jsonl").open("a") as f:
            f.write(" " * 100)
        rid = self.rid()
        before = self.get("/runs/" + rid + "/steps")
        source = self.attempt / "steps.jsonl"
        with source.open("r+b") as f:
            f.truncate(source.stat().st_size - 50)
        self.assert_error("cursor_reset", lambda: self.get("/runs/" + rid + "/steps", stream_epoch=before["stream_epoch"]), 409)

    def test_oversize_stream_and_snapshot_fail_closed(self):
        with mock.patch("mediflow.MAX_LINE_BYTES", 100):
            self.append([step(1)])
            rid = self.rid()
            self.assertFalse(self.get("/runs/" + rid + "/steps")["telemetry_complete"])
            self.assertIn("stream_line_limit", self.get("/health")["errors"])
        # A large unrelated field cannot bypass the snapshot safety budget.
        dump(self.attempt / "summary.json", {"private_raw": "X" * 300})
        with mock.patch("mediflow.MAX_SNAPSHOT_BYTES", 250):
            self.assertIn("snapshot_unavailable", self.get("/health")["errors"])

    def test_live_steps_advance_beyond_lifecycle_zero_snapshot(self):
        from datetime import datetime, timezone
        current = datetime.now(timezone.utc).isoformat()
        dump(self.attempt / "status.json", {"status": "running", "phase": "training", "optimizer_step": 0,
             "samples_processed": 0, "last_emitted_at": "2001-01-01T00:00:00Z"})
        self.append([step(7, emitted_at=current, observed_at=current)])
        # An old lifecycle event is ingested after the steps stream in a refresh.
        self.append([step(1, event="training_started", emitted_at="2001-01-01T00:00:00Z", observed_at="2001-01-01T00:00:00Z")], "events")
        detail = self.get("/runs/" + self.rid())
        self.assertEqual(detail["run"]["optimizer_step"], 7)
        self.assertEqual(detail["run"]["samples_processed"], 91)
        self.assertEqual(detail["run"]["last_emitted_at"], current)
        self.assertEqual(detail["run"]["observed_at"], current)
        self.assertFalse(detail["run"]["stale"])
        self.assertEqual(detail["status"]["optimizer_step"], 7)
        self.assertEqual(detail["status"]["last_emitted_at"], current)

    def test_transient_permission_error_resets_cursor_and_restores_full_replay(self):
        self.append([step(1), step(2)])
        rid = self.rid()
        before = self.get("/runs/" + rid + "/steps")
        original_open = self.bridge._open
        @contextmanager
        def unavailable(relative):
            if relative.endswith("steps.jsonl"):
                raise PermissionError(errno.EACCES, "private rejected path")
            with original_open(relative) as stream:
                yield stream
        with mock.patch.object(self.bridge, "_open", unavailable):
            unavailable_page = self.get("/runs/" + rid + "/steps")
            self.assertEqual(unavailable_page["items"], [])
            self.assertFalse(unavailable_page["telemetry_complete"])
        restored = self.get("/runs/" + rid + "/steps")
        self.assertEqual([r["seq"] for r in restored["items"]], [1, 2])
        self.assertNotEqual(restored["stream_epoch"], before["stream_epoch"])
        self.assert_error("cursor_reset", lambda: self.get("/runs/" + rid + "/steps", after_seq="2", stream_epoch=before["stream_epoch"]), 409)

    def test_missing_source_restore_replays_every_original_record_after_restart(self):
        self.append([step(1), step(2)])
        rid = self.rid()
        before = self.get("/runs/" + rid + "/steps")
        source = self.attempt / "steps.jsonl"
        backup = self.attempt / "steps.private-backup"
        source.rename(backup)
        self.get("/health")
        self.bridge.close()
        self.bridge = MediflowBridge(self.root, self.dbpath)
        backup.rename(source)
        restored = self.get("/runs/" + rid + "/steps")
        self.assertEqual([r["seq"] for r in restored["items"]], [1, 2])
        self.assertNotEqual(restored["stream_epoch"], before["stream_epoch"])

    def test_mid_read_io_error_discards_partial_transaction_and_restores_replay(self):
        self.append([step(1)])
        rid = self.rid()
        before = self.get("/runs/" + rid + "/steps")
        self.append([step(2), step(3)])
        original_open = self.bridge._open
        class FailingRead:
            def __init__(self, stream):
                self.stream = stream
                self.reads = 0
            def __getattr__(self, name):
                return getattr(self.stream, name)
            def readline(self, size):
                self.reads += 1
                if self.reads > 1:
                    raise OSError(errno.EIO, "private I/O diagnostic")
                return self.stream.readline(size)
        @contextmanager
        def failing(relative):
            with original_open(relative) as stream:
                yield FailingRead(stream) if relative.endswith("steps.jsonl") else stream
        with mock.patch.object(self.bridge, "_open", failing):
            self.get("/health")
        state = self.bridge._db.execute("SELECT * FROM mf_streams WHERE run_id=? AND stream='steps'", (rid,)).fetchone()
        self.assertEqual(state["offset"], 0)
        self.assertEqual(state["max_seq"], 0)
        self.assertIsNone(state["inode"])
        self.assertEqual(self.bridge._db.execute("SELECT count(*) FROM mf_records WHERE run_id=? AND stream='steps'", (rid,)).fetchone()[0], 0)
        self.bridge.close()
        self.bridge = MediflowBridge(self.root, self.dbpath)
        restored = self.get("/runs/" + rid + "/steps")
        self.assertEqual([r["seq"] for r in restored["items"]], [1, 2, 3])
        self.assertNotEqual(restored["stream_epoch"], before["stream_epoch"])

    def test_symlink_rejection_then_same_inode_restore_replays_full_epoch(self):
        self.append([step(1), step(2)])
        rid = self.rid()
        before = self.get("/runs/" + rid + "/steps")
        source = self.attempt / "steps.jsonl"
        original_inode = source.stat().st_ino
        backup = self.attempt / "steps.private-backup"
        source.rename(backup)
        outside = self.base / "private.jsonl"
        outside.write_text(json.dumps(step(3, train_loss=999)) + "\n")
        source.symlink_to(outside)
        blocked = self.get("/runs/" + rid + "/steps")
        self.assertEqual(blocked["items"], [])
        self.assertNotEqual(blocked["stream_epoch"], before["stream_epoch"])
        repeated = self.get("/runs/" + rid + "/steps")
        self.assertEqual(repeated["stream_epoch"], blocked["stream_epoch"])
        source.unlink()
        backup.rename(source)
        self.assertEqual(source.stat().st_ino, original_inode)
        self.bridge.close()
        self.bridge = MediflowBridge(self.root, self.dbpath)
        restored = self.get("/runs/" + rid + "/steps", stream_epoch=blocked["stream_epoch"])
        self.assertEqual([r["seq"] for r in restored["items"]], [1, 2])
        self.assertEqual(restored["stream_epoch"], blocked["stream_epoch"])
        self.assertTrue(restored["telemetry_complete"])
        self.assertNotIn(999, [r["train_loss"] for r in restored["items"]])

    def test_threaded_read_model_requests(self):
        self.append([step(i) for i in range(1, 25)])
        failures = []
        def read():
            try:
                for _ in range(3):
                    self.assertEqual(self.get("/runs")["total"], 1)
            except Exception as e:
                failures.append(e)
        threads = [threading.Thread(target=read) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(failures, [])
        self.assertEqual(self.bridge._db.execute("SELECT count(*) FROM mf_records").fetchone()[0], 24)


if __name__ == "__main__":
    unittest.main()
