"""CPU-only, read-only Mediflow telemetry adapter for Forge.

HTTP authorization belongs to DashboardServer. This facade accepts logical IDs,
never user paths or execution commands. Only its separate SQLite read model is
written; close() has no relationship to a training process.
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import stat
import threading
import time
from urllib.parse import urlsplit

from simulator import DashboardError

SCHEMA = "mediflow.telemetry.v1"
DATA_SCHEMA = "mediflow.dataset-reference.v1"
PREFIX = "/api/mediflow"
SAFE_ID = re.compile(r"[A-Za-z0-9_-]{1,100}\Z")
RUN_ID = re.compile(r"mf-[0-9a-f]{24}\Z")
COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,159}\Z")
HASH = re.compile(r"[0-9a-f]{64}\Z")
PHASES = set("loading training saving reloading evaluating succeeded failed held recovering pending unknown".split())
STATUSES = set("pending running held succeeded failed unknown".split())
PROVENANCE = {"live_measured", "historical_reconstructed", "derived"}
STREAMS = ("steps", "resources", "events")
MAX_LINE_BYTES = 1024 * 1024
MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024
INGEST_BYTES = 8 * 1024 * 1024
MAX_COLLECTION = 100
STALE_SECONDS = 30


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _now():
    return datetime.now(timezone.utc).isoformat()


def _error(code, status=400):
    # Never interpolate rejected paths, values, parser errors, or SQLite errors.
    return DashboardError("mediflow_" + code, {
        "cursor_reset": "The telemetry stream changed. Reset this stream's cursor and reload.",
        "not_found": "The requested Mediflow record does not exist.",
        "invalid_request": "Invalid Mediflow request or pagination parameters.",
        "dataset_unavailable": "The approved dataset reference is unavailable.",
        "read_only": "Mediflow execution and frozen datasets are owned by the training supervisor.",
    }.get(code, "Mediflow data is unavailable or failed integrity validation."), status)


def _label(value, limit=200):
    if not isinstance(value, str) or not value or len(value) > limit:
        return None
    if any(ord(c) < 32 for c in value) or any(c in value for c in "<>\\\x7f"):
        return None
    if value.startswith(("/", "~", ".")) or ".." in value or "://" in value:
        return None
    if re.search(r"(?i)(?:/home/|/tmp/|/mnt/|/root/|[a-z]:[/\\]|bearer\s|(?:token|password|secret|api_key)\s*[:=])", value):
        return None
    return value


def _reference(value):
    """Public license/identifier URLs are provenance, not local file paths."""
    label = _label(value, 2048)
    if label is not None:
        return label
    if not isinstance(value, str) or len(value) > 2048 or any(ord(c) < 32 for c in value) or any(c in value for c in "<>\\"):
        return None
    try:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
            return None
        host = parsed.hostname.lower()
        if host in {"localhost", "127.0.0.1", "::1"} or host.endswith((".local", ".internal")):
            return None
        if re.search(r"(?i)(?:password|token|api_key|secret)=|%0[ad]", value):
            return None
        return value
    except ValueError:
        return None


def _groups(value):
    if isinstance(value, dict):
        return {k: _reference(value[k]) for k in ("patient", "study", "document", "image", "parent", "duplicate_cluster") if k in value}
    return _labels(value)


def _restrictions(value):
    if isinstance(value, dict):
        keys = ("raw_redistribution", "research_only", "sft_allowed", "dataset_license", "attribution_required")
        return {k: value[k] if isinstance(value[k], bool) else _label(value[k]) for k in keys if k in value}
    return _labels(value) if isinstance(value, list) else _label(value)


def _reason(value):
    # Free-form diagnostics may contain raw data or paths. Publish reason codes only.
    if isinstance(value, str) and re.fullmatch(r"[a-z][a-z0-9_]{0,99}", value) and not any(s in value for s in ("secret", "password", "token_value")):
        return value
    return "unavailable_or_redacted" if value is not None else None


def _number(value, integer=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        if not math.isfinite(value):
            return None
    except (OverflowError, TypeError):
        return None
    if integer and (not isinstance(value, int) or value < 0 or value > 9223372036854775807):
        return None
    return value


def _bool(value):
    return value if isinstance(value, bool) else None


def _timestamp(value):
    if not isinstance(value, str) or len(value) > 40:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset().total_seconds() != 0:
            return None
        return value
    except (ValueError, TypeError, OverflowError):
        return None


def _hash(value):
    return value if isinstance(value, str) and HASH.fullmatch(value) else None


def _artifact(value, full=False):
    if not isinstance(value, dict):
        return None
    aid = _label(value.get("artifact_id"))
    digest = _hash(value.get("sha256"))
    if not aid:
        return None
    result = {"artifact_id": aid, "sha256": digest}
    if full:
        result.update(kind=_label(value.get("kind")), exists=_bool(value.get("exists")),
                      verified=_bool(value.get("verified")), reload_verified=_bool(value.get("reload_verified")),
                      selection=_label(value.get("selection")))
    return result


def _availability(value):
    if not isinstance(value, dict):
        return {}
    result = {}
    for key, val in list(value.items())[:100]:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,79}", key) or not isinstance(val, dict):
            continue
        result[key] = {"available": _bool(val.get("available")), "reason": _reason(val.get("reason")),
                       "provenance": val.get("provenance") if val.get("provenance") in PROVENANCE | {"unmeasured"} else "unmeasured"}
    return result


def _objects(value, convert, limit=MAX_COLLECTION):
    return [x for v in value[:limit] if (x := convert(v)) is not None] if isinstance(value, list) else []


def _labels(value):
    return [v for x in value[:100] if (v := _label(x)) is not None] if isinstance(value, list) else []


MANIFEST_LABELS = "campaign_id job_id run_id attempt_id producer model task eval_protocol precision parent_run_id freeze_id cohort_id".split()
STEP_INTS = "optimizer_step total_updates batch_examples samples_processed samples_total supervised_tokens supervised_tokens_cumulative sequence_tokens sequence_tokens_cumulative cuda_allocated_bytes cuda_reserved_bytes cuda_peak_allocated_bytes cuda_peak_reserved_bytes".split()
STEP_FLOATS = "train_loss lr_used lr_next gradient_norm_before_clip step_wall_seconds samples_per_second supervised_tokens_per_second sequence_tokens_per_second".split()
RESOURCE_INTS = "gpu_index process_id gpu_memory_used_bytes gpu_memory_total_bytes gpu_sm_clock_mhz gpu_memory_clock_mhz host_ram_used_bytes host_ram_total_bytes process_rss_bytes".split()
RESOURCE_FLOATS = "sample_interval_seconds sample_gap_seconds gpu_utilization_percent gpu_temperature_celsius gpu_power_watts cpu_percent".split()
EVENTS = set("model_loading training_started optimizer_update checkpoint_saved reload_started reload_succeeded evaluation_started evaluation_completed succeeded failed recovery hold telemetry_gap telemetry_error".split())
EVENT_MESSAGES = {name: name.replace("_", " ") for name in EVENTS}
DETAIL_NUMBERS = set("optimizer_step total_updates samples_processed samples_total dropped_records from_seq to_seq duration_seconds count".split())


def _manifest(value):
    if not isinstance(value, dict) or value.get("schema_version") != SCHEMA:
        raise _error("invalid_manifest")
    out = {key: _label(value.get(key)) for key in MANIFEST_LABELS}
    if any(out[k] is None for k in ("campaign_id", "job_id", "run_id", "attempt_id", "producer")):
        raise _error("invalid_manifest")
    source = value.get("source")
    if source not in {"mediflow_live", "mediflow_historical"}:
        raise _error("invalid_manifest")
    out.update(schema_version=SCHEMA, telemetry_version=SCHEMA, source=source,
               historical=source == "mediflow_historical", seed=_number(value.get("seed"), True),
               data_hash=_hash(value.get("data_hash")), scientific_recipe_hash=_hash(value.get("scientific_recipe_hash")),
               code_sha=value.get("code_sha") if isinstance(value.get("code_sha"), str) and re.fullmatch(r"[0-9a-f]{40,64}", value["code_sha"]) else None,
               total_updates=_number(value.get("total_updates"), True), samples_total=_number(value.get("samples_total"), True),
               created_at=_timestamp(value.get("created_at")), dataset_ids=_labels(value.get("dataset_ids")),
               artifacts=_objects(value.get("artifacts"), lambda v: _artifact(v, True)),
               source_artifacts=_objects(value.get("source_artifacts"), _artifact))
    return out


def _envelope(row, manifest):
    if not isinstance(row, dict) or row.get("schema_version") != SCHEMA:
        raise _error("invalid_record")
    for k in ("campaign_id", "job_id", "run_id", "attempt_id", "source"):
        if row.get(k) != manifest[k]:
            raise _error("identity_mismatch")
    seq = _number(row.get("seq"), True)
    producer = _label(row.get("producer"))
    if not seq or not producer:
        raise _error("invalid_record")
    out = {k: manifest[k] for k in ("campaign_id", "job_id", "run_id", "attempt_id", "source")}
    out.update(schema_version=SCHEMA, seq=seq, producer=producer, emitted_at=_timestamp(row.get("emitted_at")),
               observed_at=_timestamp(row.get("observed_at")), monotonic_ns=_number(row.get("monotonic_ns"), True),
               model=_label(row.get("model")), task=_label(row.get("task")),
               phase=row.get("phase") if row.get("phase") in PHASES else "unknown",
               provenance=row.get("provenance") if row.get("provenance") in PROVENANCE else None,
               source_artifact=_artifact(row.get("source_artifact")))
    return out


def _record(row, manifest, stream):
    out = _envelope(row, manifest)
    if stream == "steps":
        for key in STEP_INTS:
            out[key] = _number(row.get(key), True)
        for key in STEP_FLOATS:
            out[key] = _number(row.get(key))
        if not out["optimizer_step"] or not out["batch_examples"]:
            raise _error("invalid_record")
        out.update(loss_objective=row.get("loss_objective") if row.get("loss_objective") == "example_mean_of_target_token_mean" else None,
                   step_time_scope=_label(row.get("step_time_scope")), finite=_bool(row.get("finite")))
    elif stream == "resources":
        for key in RESOURCE_INTS:
            out[key] = _number(row.get(key), True)
        for key in RESOURCE_FLOATS:
            out[key] = _number(row.get(key))
        out.update(gpu_uuid=_label(row.get("gpu_uuid")),
                   attribution=row.get("attribution") if row.get("attribution") in {"device_shared", "process", "unavailable"} else "unavailable",
                   tool_status=row.get("tool_status") if row.get("tool_status") in {"ok", "unavailable", "error", "backoff"} else "unavailable")
    else:
        event = row.get("event")
        if event not in EVENTS:
            raise _error("invalid_record")
        details = row.get("details") if isinstance(row.get("details"), dict) else {}
        out.update(event=event, message=EVENT_MESSAGES[event], optimizer_step=_number(row.get("optimizer_step"), True),
                   details={k: _number(details[k]) for k in DETAIL_NUMBERS if k in details})
    out["availability"] = _availability(row.get("availability"))
    for key in STEP_FLOATS if stream == "steps" else RESOURCE_FLOATS if stream == "resources" else []:
        if row.get(key) is not None and out[key] is None:
            out["availability"][key] = {"available": False, "reason": "nonfinite_or_invalid", "provenance": "unmeasured"}
    return out


def _status(value):
    value = value if isinstance(value, dict) else {}
    out = {"schema_version": SCHEMA, "status": value.get("status") if value.get("status") in STATUSES else "unknown",
           "phase": value.get("phase") if value.get("phase") in PHASES else "unknown"}
    for key in ("optimizer_step", "total_updates", "samples_processed", "samples_total", "dropped_records"):
        out[key] = _number(value.get(key), True)
    out["dropped_records"] = out["dropped_records"] or 0
    for key in ("training_succeeded", "reload_succeeded", "evaluation_completed", "telemetry_complete"):
        out[key] = _bool(value.get(key))
    out.update(last_emitted_at=_timestamp(value.get("last_emitted_at")), telemetry_error=_reason(value.get("telemetry_error")),
               writer_health=value.get("writer_health") if value.get("writer_health") in {"ok", "degraded", "failed", "unknown"} else "unknown")
    def gap(v):
        if not isinstance(v, dict) or v.get("stream") not in STREAMS:
            return None
        return {"stream": v["stream"], "from_seq": _number(v.get("from_seq"), True), "to_seq": _number(v.get("to_seq"), True), "reason": _reason(v.get("reason"))}
    out["gaps"] = _objects(value.get("gaps"), gap)
    return out


def _summary(value):
    if not isinstance(value, dict):
        return None
    out = {k: _number(value.get(k)) for k in ("train_loss_mean", "wall_seconds")}
    out.update({k: _number(value.get(k), True) for k in ("optimizer_steps", "samples_processed", "supervised_tokens", "sequence_tokens", "peak_allocated_bytes", "peak_reserved_bytes")})
    r = value.get("reconciliation") if isinstance(value.get("reconciliation"), dict) else {}
    checks = r.get("checks") if isinstance(r.get("checks"), dict) else {}
    out.update(telemetry_complete=_bool(value.get("telemetry_complete")),
               wall_time_scope=value.get("wall_time_scope") if value.get("wall_time_scope") in {"model_load_audit_training_verify_save_and_final_sync", "legacy_attempt_wall", "unknown"} else "unknown",
               reconciliation={"status": r.get("status") if r.get("status") in {"passed", "failed", "unavailable"} else "unavailable",
                               "checks": {k: _bool(v) for k, v in list(checks.items())[:50] if re.fullmatch(r"[a-z][a-z0-9_]{0,79}", k)}})
    return out


METRICS = set("coverage answered_accuracy end_to_end_correct_fraction answered_lexical_exact_match end_to_end_lexical_exact_match lexical_token_f1 macro_f1 e2e_latency_seconds completed valid_answers correct exact_matches invalid_format_rate not_attempted_count technical_failure_rate truncated_count clinical_accuracy likelihood_accuracy likelihood_coverage likelihood_nll likelihood_loss likelihood_perplexity latency_seconds ttft_seconds decode_tokens_per_second peak_allocated_bytes peak_reserved_bytes model_load_seconds delta group_count bootstrap_resamples training_seed_uncertainty_measured".split())
STRUCTURED_METRICS = {"e2e_latency_seconds", "latency_seconds", "ttft_seconds", "paired_ci", "confidence_interval", "ci95", "likelihood", "human_review"}
METRIC_LEAVES = set("n count median mean min max p50 p90 p95 p99 lower upper low high delta group_count bootstrap_resamples training_seed_uncertainty_measured correct total accuracy coverage loss perplexity available completed valid required".split())


def _metrics(value):
    if not isinstance(value, dict):
        return {}
    out = {}
    for key, v in value.items():
        if key in METRICS and not isinstance(v, (dict, list)):
            out[key] = v if isinstance(v, bool) else _number(v)
        elif key in STRUCTURED_METRICS:
            if isinstance(v, list) and key == "ci95":
                out[key] = [_number(x) for x in v[:2]]
            elif isinstance(v, dict):
                entry = {k: x if isinstance(x, bool) else _number(x) for k, x in v.items() if k in METRIC_LEAVES}
                for k in ("scope", "bootstrap_unit", "metric"):
                    if k in v:
                        entry[k] = _label(v[k], 100)
                out[key] = entry
        elif key in {"bootstrap_unit", "metric"}:
            out[key] = _label(v, 100)
        elif key in {"statuses", "label_support"} and isinstance(v, dict):
            # Labels are bounded, no arbitrary nested objects or prediction text.
            out[key] = {k: _number(x, True) for k, x in list(v.items())[:30]
                        if isinstance(k, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,32}", k)}
    return out


def _evaluation(value):
    if not isinstance(value, dict) or not _label(value.get("evaluation_id")):
        return None
    out = {k: _label(value.get(k)) for k in ("evaluation_id", "run_id", "dataset_id", "source_id", "language", "task", "protocol", "protocol_version", "model", "precision", "cohort_id", "image_condition", "renderer_version")}
    out.update(variant=value.get("variant") if value.get("variant") in {"base", "tuned"} else None,
               split=value.get("split") if value.get("split") in {"train", "validation", "test"} else None,
               data_hash=_hash(value.get("data_hash")),
               status=value.get("status") if value.get("status") in {"not_run", "partial", "final", "failed"} else "not_run",
               metrics=_metrics(value.get("metrics")),
               provenance=value.get("provenance") if value.get("provenance") in PROVENANCE else None,
               source_artifact=_artifact(value.get("source_artifact")), receipt_verified=_bool(value.get("receipt_verified")))
    for k in ("scheduled", "attempted", "valid", "invalid", "failed", "unattempted"):
        out[k] = _number(value.get(k), True)
    def subgroup(v):
        if not isinstance(v, dict):
            return None
        r = {k: _label(v.get(k)) for k in ("label", "group", "subgroup", "modality", "image_ablation", "task", "name", "source_id", "dimension", "value") if k in v}
        if not r.get("label") and r.get("value"):
            r["label"] = r["value"]
        r["metrics"] = _metrics(v.get("metrics", v))
        for k in ("scheduled", "attempted", "valid", "invalid", "failed", "unattempted"):
            if k in v:
                r[k] = _number(v[k], True)
        return r
    out["subgroups"] = _objects(value.get("subgroups"), subgroup)
    return out


def _dataset(value, catalog_id):
    if not isinstance(value, dict) or value.get("schema_version") != DATA_SCHEMA or value.get("dataset_id") != catalog_id:
        raise _error("invalid_dataset")
    if value.get("frozen") is not True or value.get("provenance_preserved") is not True or not _hash(value.get("data_hash")):
        raise _error("invalid_dataset")
    count = _number(value.get("record_count"), True)
    if count is None:
        raise _error("invalid_dataset")
    index = value.get("metadata_index")
    if index not in (None, "records.metadata.jsonl"):
        raise _error("invalid_dataset")
    def source(v):
        if not isinstance(v, dict):
            return None
        return {"source_id": _label(v.get("source_id")), "source_revision": _label(v.get("source_revision")),
                "license_ref": _reference(v.get("license_ref")), "restrictions": _restrictions(v.get("restrictions"))}
    splits = value.get("split_counts") if isinstance(value.get("split_counts"), dict) else {}
    return {"schema_version": DATA_SCHEMA, "dataset_id": catalog_id, "name": _label(value.get("name")),
            "freeze_id": _label(value.get("freeze_id")), "data_hash": value["data_hash"], "record_count": count,
            "split_counts": {k: _number(splits.get(k), True) for k in ("train", "validation", "test")},
            "sources": _objects(value.get("sources"), source), "source_artifacts": _objects(value.get("source_artifacts"), _artifact),
            "frozen": True, "status": value.get("status") if value.get("status") in {"registered", "verified"} else "registered",
            "metadata_index": index, "metadata_sha256": _hash(value.get("metadata_sha256")), "provenance_preserved": True}


def _metadata(value):
    if not isinstance(value, dict):
        raise _error("invalid_metadata")
    out = {k: _label(value.get(k)) for k in ("sample_id", "source_id", "source_revision", "task_type", "language", "license_ref", "review_status", "freeze_id")}
    out["license_ref"] = _reference(value.get("license_ref"))
    out.update(group_ids=_groups(value.get("group_ids")), split=value.get("split") if value.get("split") in {"train", "validation", "test"} else None,
               image_sha256=_hash(value.get("image_sha256")),
               restrictions=_restrictions(value.get("restrictions")))
    # Derivation is structured provenance, never arbitrary raw fields.
    d = value.get("derivation")
    out["derivation"] = {k: _label(d.get(k)) for k in ("method", "version", "parent_sample_id", "review_status") if k in d} if isinstance(d, dict) else _label(d)
    return out


def _unique(pairs):
    result = {}
    for k, v in pairs:
        if k in result:
            raise ValueError("duplicate_key")
        result[k] = v
    return result


def _parse(raw):
    return json.loads(raw, object_pairs_hook=_unique)


class MediflowBridge:
    """Durable indexed read model. All public operations are serialized by a lock."""

    def __init__(self, root, db_path=None):
        self.root = Path(root).expanduser().resolve()
        if db_path is None:
            self.root.mkdir(parents=True, exist_ok=True)
            db_path = self.root / ".forge-mediflow.sqlite3"
        if str(db_path) != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._db = sqlite3.connect(str(db_path), check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.executescript("""
            CREATE TABLE IF NOT EXISTS mf_runs(id TEXT PRIMARY KEY, path TEXT UNIQUE, manifest TEXT NOT NULL,
                status TEXT NOT NULL, summary TEXT, evaluations TEXT NOT NULL DEFAULT '[]', activity REAL NOT NULL);
            CREATE TABLE IF NOT EXISTS mf_files(path TEXT PRIMARY KEY, signature TEXT NOT NULL, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS mf_streams(run_id TEXT, stream TEXT, producer TEXT NOT NULL, epoch TEXT NOT NULL,
                generation INTEGER NOT NULL DEFAULT 0, dev INTEGER, inode INTEGER, offset INTEGER NOT NULL DEFAULT 0,
                size INTEGER NOT NULL DEFAULT 0, anchor TEXT, max_seq INTEGER NOT NULL DEFAULT 0, integrity INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY(run_id,stream));
            CREATE TABLE IF NOT EXISTS mf_records(run_id TEXT,stream TEXT,seq INTEGER,digest TEXT NOT NULL,value TEXT NOT NULL,
                PRIMARY KEY(run_id,stream,seq));
            CREATE TABLE IF NOT EXISTS mf_datasets(id TEXT PRIMARY KEY,path TEXT NOT NULL,manifest TEXT NOT NULL,registered INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS mf_metadata_state(id TEXT PRIMARY KEY,signature TEXT NOT NULL,offset INTEGER NOT NULL DEFAULT 0,
                count INTEGER NOT NULL DEFAULT 0,complete INTEGER NOT NULL DEFAULT 0,verified INTEGER NOT NULL DEFAULT 0,error TEXT);
            CREATE TABLE IF NOT EXISTS mf_metadata(id TEXT,rownum INTEGER,offset INTEGER,length INTEGER,PRIMARY KEY(id,rownum));
            CREATE TABLE IF NOT EXISTS mf_errors(code TEXT PRIMARY KEY);
            CREATE TABLE IF NOT EXISTS mf_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
        """)
        self._db.commit()
        self._closed = False
        self._snapshot_signatures = {}

    def capabilities(self):
        return {"read": True, "register_dataset_reference": True, "start": False, "pause": False,
                "resume": False, "cancel": False, "retry": False,
                "reason": "Execution is owned by the Mediflow supervisor; partial optimizer/RNG resume is unsupported."}

    def close(self):
        with self._lock:
            if not self._closed:
                self._db.close()
                self._closed = True

    @contextmanager
    def _open(self, relative):
        """Open relative files through O_NOFOLLOW directory descriptors (race safe)."""
        parts = Path(relative).parts
        if not parts or Path(relative).is_absolute() or any(p in {"", ".", ".."} for p in parts):
            raise _error("unsafe_file")
        descriptors = []
        try:
            fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            descriptors.append(fd)
            for part in parts[:-1]:
                fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
                descriptors.append(fd)
            fd = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            descriptors.append(fd)
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise _error("unsafe_file")
            with os.fdopen(descriptors.pop(), "rb") as f:
                yield f
        finally:
            for fd in reversed(descriptors):
                os.close(fd)

    def _note(self, code):
        self._db.execute("INSERT OR IGNORE INTO mf_errors(code) VALUES(?)", (code,))

    def _snapshot(self, path):
        try:
            with self._open(path) as f:
                s = os.fstat(f.fileno())
                signature = _json([s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns])
                self._snapshot_signatures[path] = signature
                cached = self._db.execute("SELECT * FROM mf_files WHERE path=?", (path,)).fetchone()
                if cached and cached["signature"] == signature:
                    return _parse(cached["value"]), False
                raw = f.read(MAX_SNAPSHOT_BYTES + 1)
                if len(raw) > MAX_SNAPSHOT_BYTES:
                    raise ValueError("snapshot_limit")
                value = _parse(raw)
                # Store sanitized snapshots in specific tables; generic cache only
                # retains parsed source in memory through this call, never private raw data.
                return value, True
        except FileNotFoundError:
            return None, False
        except (OSError, ValueError, UnicodeError, DashboardError, RecursionError):
            self._note("snapshot_unavailable")
            return None, False

    def _cache(self, path, value):
        # Signature belongs to the same opened descriptor as the parsed bytes.
        # Atomic replacement after the read is detected on the next request.
        signature = self._snapshot_signatures.get(path)
        if signature:
            self._db.execute("INSERT OR REPLACE INTO mf_files VALUES(?,?,?)", (path, signature, _json(value)))

    def _dirs(self, directory):
        try:
            return sorted(e.name for e in os.scandir(directory) if e.is_dir(follow_symlinks=False) and COMPONENT.fullmatch(e.name))
        except OSError:
            return []

    def _discover(self):
        found_runs, found_datasets = set(), set()
        for campaign in self._dirs(self.root):
            if campaign == "datasets":
                continue
            for run in self._dirs(self.root / campaign):
                for attempt in self._dirs(self.root / campaign / run):
                    relative = str(Path(campaign, run, attempt))
                    path = str(Path(relative, "manifest.json"))
                    value, changed = self._snapshot(path)
                    if value is None:
                        continue
                    try:
                        m = _manifest(value)
                    except DashboardError:
                        self._note("invalid_manifest")
                        continue
                    identity = [m[k] for k in ("campaign_id", "job_id", "run_id", "attempt_id")]
                    rid = "mf-" + hashlib.sha256(_json(identity).encode()).hexdigest()[:24]
                    existing = self._db.execute("SELECT path FROM mf_runs WHERE id=?", (rid,)).fetchone()
                    if existing and existing["path"] != relative:
                        self._note("duplicate_attempt_identity")
                        continue
                    found_runs.add(rid)
                    self._db.execute("INSERT INTO mf_runs(id,path,manifest,status,activity) VALUES(?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET manifest=excluded.manifest", (rid, relative, _json(m), _json(_status(None)), time.time()))
                    if changed:
                        self._cache(path, m)
                    for filename, converter, column in (("status.json", _status, "status"), ("summary.json", _summary, "summary"), ("evaluations.json", lambda v: {"evaluations": _objects(v.get("evaluations"), _evaluation, len(v.get("evaluations", [])) if isinstance(v.get("evaluations"), list) else 0)} if isinstance(v, dict) else {"evaluations": []}, "evaluations")):
                        snap_path = str(Path(relative, filename))
                        raw, snap_changed = self._snapshot(snap_path)
                        if raw is None:
                            clean = _status(None) if column == "status" else None if column == "summary" else []
                        else:
                            clean = converter(raw)
                            if snap_changed:
                                self._cache(snap_path, clean)
                            if column == "evaluations":
                                clean = clean["evaluations"]
                        self._db.execute("UPDATE mf_runs SET " + column + "=? WHERE id=?", (_json(clean), rid))
                        if snap_changed:
                            self._db.execute("UPDATE mf_runs SET activity=? WHERE id=?", (time.time(), rid))
                    for stream in STREAMS:
                        self._ingest(rid, relative, m, stream)
        for did in self._dirs(self.root / "datasets"):
            if not SAFE_ID.fullmatch(did):
                continue
            path = str(Path("datasets", did, "manifest.json"))
            value, changed = self._snapshot(path)
            if value is None:
                continue
            try:
                manifest = _dataset(value, did)
            except DashboardError:
                self._note("invalid_dataset")
                continue
            found_datasets.add(did)
            self._db.execute("INSERT INTO mf_datasets(id,path,manifest) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET path=excluded.path,manifest=excluded.manifest", (did, str(Path("datasets", did)), _json(manifest)))
            if changed:
                self._cache(path, manifest)
        # Missing sources must not remain accessible via cached manifests.
        for row in self._db.execute("SELECT id FROM mf_runs").fetchall():
            if row["id"] not in found_runs:
                self._db.execute("DELETE FROM mf_runs WHERE id=?", (row["id"],))
        for row in self._db.execute("SELECT id FROM mf_datasets").fetchall():
            if row["id"] not in found_datasets:
                self._db.execute("DELETE FROM mf_datasets WHERE id=?", (row["id"],))

    def _epoch(self, manifest, stream, producer, generation=0):
        identity = [manifest[k] for k in ("campaign_id", "job_id", "run_id", "attempt_id")]
        return hashlib.sha256(_json(identity + [stream, producer, generation]).encode()).hexdigest()

    def _ingest(self, rid, relative, m, stream):
        path = str(Path(relative, stream + ".jsonl"))
        state = self._db.execute("SELECT * FROM mf_streams WHERE run_id=? AND stream=?", (rid, stream)).fetchone()
        if state is None:
            self._db.execute("INSERT INTO mf_streams(run_id,stream,producer,epoch) VALUES(?,?,?,?)", (rid, stream, m["producer"], self._epoch(m, stream, m["producer"])))
            state = self._db.execute("SELECT * FROM mf_streams WHERE run_id=? AND stream=?", (rid, stream)).fetchone()
        try:
            with self._open(path) as f:
                s = os.fstat(f.fileno())
                changed = state["inode"] is not None and (s.st_ino != state["inode"] or s.st_dev != state["dev"] or s.st_size < state["size"])
                if not changed and state["offset"] and state["anchor"]:
                    f.seek(max(0, state["offset"] - 256))
                    changed = hashlib.sha256(f.read(min(256, state["offset"]))).hexdigest() != state["anchor"]
                if changed:
                    generation = state["generation"] + 1
                    self._db.execute("DELETE FROM mf_records WHERE run_id=? AND stream=?", (rid, stream))
                    self._db.execute("UPDATE mf_streams SET epoch=?,generation=?,offset=0,max_seq=0,anchor=NULL,integrity=1 WHERE run_id=? AND stream=?", (self._epoch(m, stream, m["producer"], generation), generation, rid, stream))
                    self._note("stream_rotated")
                    state = self._db.execute("SELECT * FROM mf_streams WHERE run_id=? AND stream=?", (rid, stream)).fetchone()
                offset, highest, producer = state["offset"], state["max_seq"], state["producer"]
                integrity = state["integrity"]
                f.seek(offset)
                start = offset
                while f.tell() - start < INGEST_BYTES:
                    line = f.readline(MAX_LINE_BYTES + 1)
                    if not line or not line.endswith(b"\n"):
                        if len(line) > MAX_LINE_BYTES:
                            self._note("stream_line_limit")
                            integrity = 0
                        break
                    if len(line) > MAX_LINE_BYTES:
                        self._note("stream_line_limit")
                        integrity = 0
                        break
                    offset = f.tell()
                    if not line.strip():
                        continue
                    try:
                        raw = _parse(line)
                        clean = _record(raw, m, stream)
                        seq = clean["seq"]
                        # Semantic equality tolerates JSON key order and whitespace.
                        digest = hashlib.sha256(json.dumps(raw, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                        old = self._db.execute("SELECT digest FROM mf_records WHERE run_id=? AND stream=? AND seq=?", (rid, stream, seq)).fetchone()
                        if old:
                            if old["digest"] != digest:
                                self._note("conflicting_sequence")
                                integrity = 0
                            continue
                        if seq <= highest:
                            self._note("sequence_regression")
                            integrity = 0
                            continue
                        if highest == 0:
                            # The stream can legitimately use a producer distinct from
                            # the manifest coordinator, but it becomes immutable now.
                            producer = clean["producer"]
                        if clean["producer"] != producer:
                            self._note("producer_mismatch")
                            integrity = 0
                            continue
                        if seq != highest + 1:
                            self._note("sequence_gap")
                            integrity = 0
                        self._db.execute("INSERT INTO mf_records VALUES(?,?,?,?,?)", (rid, stream, seq, digest, _json(clean)))
                        highest = seq
                        self._db.execute("UPDATE mf_runs SET activity=? WHERE id=?", (time.time(), rid))
                        self._db.execute("INSERT OR REPLACE INTO mf_meta VALUES('last_ingested_at',?)", (_now(),))
                    except (ValueError, UnicodeError, TypeError, DashboardError, RecursionError):
                        self._note("invalid_stream_record")
                        integrity = 0
                f.seek(max(0, offset - 256))
                anchor = hashlib.sha256(f.read(min(256, offset))).hexdigest() if offset else None
                self._db.execute("UPDATE mf_streams SET producer=?,epoch=?,dev=?,inode=?,offset=?,size=?,anchor=?,max_seq=?,integrity=? WHERE run_id=? AND stream=?", (producer, self._epoch(m, stream, producer, state["generation"]), s.st_dev, s.st_ino, offset, s.st_size, anchor, highest, integrity, rid, stream))
        except FileNotFoundError:
            # A vanished stream is a reset; do not serve stale cached records.
            if state["inode"] is not None:
                self._db.execute("DELETE FROM mf_records WHERE run_id=? AND stream=?", (rid, stream))
                generation = state["generation"] + 1
                self._db.execute("UPDATE mf_streams SET inode=NULL,dev=NULL,offset=0,max_seq=0,anchor=NULL,generation=?,epoch=?,integrity=0 WHERE run_id=? AND stream=?", (generation, self._epoch(m, stream, state["producer"], generation), rid, stream))
                self._note("stream_missing")
        except (OSError, DashboardError):
            self._db.execute("DELETE FROM mf_records WHERE run_id=? AND stream=?", (rid, stream))
            self._db.execute("UPDATE mf_streams SET integrity=0 WHERE run_id=? AND stream=?", (rid, stream))
            self._note("stream_unavailable")

    def _get(self, rid):
        if not RUN_ID.fullmatch(rid):
            raise _error("invalid_request")
        row = self._db.execute("SELECT * FROM mf_runs WHERE id=?", (rid,)).fetchone()
        if not row:
            raise _error("not_found", 404)
        return row

    def _run(self, row):
        m, status = _parse(row["manifest"]), _parse(row["status"])
        latest = self._db.execute("SELECT value FROM mf_records WHERE run_id=? AND stream='steps' ORDER BY seq DESC LIMIT 1", (row["id"],)).fetchone()
        step = _parse(latest["value"]) if latest else {}
        # The lifecycle owner updates snapshots on phase changes. Its initial
        # counters and timestamp must not hide newer child step/resource records.
        tails = []
        for stream in STREAMS:
            tail = self._db.execute("SELECT value FROM mf_records WHERE run_id=? AND stream=? ORDER BY seq DESC LIMIT 1", (row["id"], stream)).fetchone()
            if tail:
                tails.append(_parse(tail["value"]))
        def newest(values):
            values = [v for v in values if v]
            return max(values, key=lambda v: datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp()) if values else None
        out = {k: m.get(k) for k in ("campaign_id", "job_id", "run_id", "attempt_id", "source", "model", "task", "seed", "historical", "data_hash", "scientific_recipe_hash", "code_sha", "eval_protocol", "precision", "cohort_id")}
        out.update(id=row["id"], status=status["status"], phase=status["phase"], train_loss=step.get("train_loss"))
        for k in ("optimizer_step", "samples_processed"):
            values = [v for v in (status.get(k), step.get(k)) if v is not None]
            out[k] = max(values) if values else None
        for k in ("total_updates", "samples_total"):
            out[k] = status.get(k) if status.get(k) is not None else step.get(k) if step.get(k) is not None else m.get(k)
        for k in ("training_succeeded", "reload_succeeded", "evaluation_completed", "telemetry_complete"):
            out[k] = status.get(k)
        bad = self._db.execute("SELECT 1 FROM mf_streams WHERE run_id=? AND integrity=0 LIMIT 1", (row["id"],)).fetchone()
        if bad:
            out["telemetry_complete"] = False
        out["last_emitted_at"] = newest([status.get("last_emitted_at")] + [r.get("emitted_at") for r in tails])
        out["observed_at"] = newest([r.get("observed_at") for r in tails])
        age_from = row["activity"]
        if out["last_emitted_at"]:
            age_from = datetime.fromisoformat(out["last_emitted_at"].replace("Z", "+00:00")).timestamp()
        out["stale"] = out["status"] == "running" and not out["historical"] and time.time() - age_from > STALE_SECONDS
        return out

    def _page(self, records, offset, limit, key):
        total = len(records)
        return {key: records[offset:offset + limit], "offset": offset, "limit": limit, "total": total, "has_more": offset + limit < total}

    def _int_query(self, query, name, default, maximum=None):
        value = query.get(name, str(default))
        if isinstance(value, int) and not isinstance(value, bool):
            number = value
        elif isinstance(value, str) and re.fullmatch(r"[0-9]{1,12}", value):
            number = int(value)
        else:
            raise _error("invalid_request")
        if number < 0 or (name == "limit" and number == 0) or (maximum is not None and number > maximum):
            raise _error("invalid_request")
        return number

    def dispatch(self, method, path, query=None, payload=None):
        query = query or {}
        if not isinstance(path, str) or not path.startswith(PREFIX + "/") or not isinstance(query, dict):
            raise _error("invalid_request")
        route = path[len(PREFIX):]
        # No URL decoding, filesystem interpolation, or arbitrary query paths.
        if "%" in route or ".." in route or "\\" in route or "//" in route:
            raise _error("invalid_request")
        if method not in {"GET", "POST"}:
            raise _error("read_only", 405)
        if method == "POST" and route != "/datasets/register":
            raise _error("read_only", 405)
        with self._lock:
            if self._closed:
                raise _error("unavailable", 503)
            try:
                self._discover()
                result = self._dispatch(method, route, query, payload)
                self._db.commit()
                return result
            except DashboardError:
                # Cursor/reset/integrity observations are durable even on HTTP409.
                self._db.commit()
                raise
            except (sqlite3.Error, OSError, ValueError, TypeError, RecursionError):
                self._db.rollback()
                raise _error("unavailable", 503) from None

    def _dispatch(self, method, route, q, payload):
        if route == "/health" and method == "GET":
            last = self._db.execute("SELECT value FROM mf_meta WHERE key='last_ingested_at'").fetchone()
            return {"schema_version": SCHEMA, "mode": "mediflow", "source": "external_mediflow", "capabilities": self.capabilities(),
                    "connected": self.root.is_dir(), "last_ingested_at": last["value"] if last else None,
                    "stale_after_seconds": STALE_SECONDS, "errors": [r["code"] for r in self._db.execute("SELECT code FROM mf_errors ORDER BY code")]}
        offset = self._int_query(q, "offset", 0)
        limit = self._int_query(q, "limit", 100, 1000 if re.fullmatch(r"/runs/[^/]+/(steps|resources|events)", route) else 500)
        if method == "GET" and route in {"/runs", "/campaigns"}:
            rows = self._db.execute("SELECT * FROM mf_runs ORDER BY id").fetchall()
            runs = [self._run(r) for r in rows]
            if route == "/runs":
                campaign = q.get("campaign_id")
                if campaign is not None:
                    if _label(campaign) is None:
                        raise _error("invalid_request")
                    runs = [r for r in runs if r["campaign_id"] == campaign]
                return self._page(runs, offset, limit, "runs")
            campaigns = []
            for cid in sorted({r["campaign_id"] for r in runs}):
                subset = [r for r in runs if r["campaign_id"] == cid]
                # Count unique successful first-seed jobs, not attempts or smoke/eval.
                done = {r["job_id"] for r in subset if re.fullmatch(r"v6-first-seed-(?:T[1-6]|V[1-4])", r["job_id"]) and r["training_succeeded"] is True and r["reload_succeeded"] is True}
                campaigns.append({"campaign_id": cid, "run_count": len(subset), "status_counts": dict(Counter(r["status"] for r in subset)),
                                  "first_seed_completed": len(done), "first_seed_expected": 10 if any(re.fullmatch(r"v6-first-seed-(?:T[1-6]|V[1-4])", r["job_id"]) for r in subset) else None,
                                  "last_emitted_at": max((r["last_emitted_at"] for r in subset if r["last_emitted_at"]), default=None)})
            return self._page(campaigns, offset, limit, "campaigns")
        if route == "/datasets" and method == "GET":
            rows = self._db.execute("SELECT manifest,registered FROM mf_datasets ORDER BY id").fetchall()
            return self._page([{**_parse(r["manifest"]), "registered": bool(r["registered"])} for r in rows], offset, limit, "datasets")
        if route == "/datasets/register" and method == "POST":
            if not isinstance(payload, dict) or set(payload) != {"manifest_id"} or not isinstance(payload["manifest_id"], str) or not SAFE_ID.fullmatch(payload["manifest_id"]):
                raise _error("invalid_request")
            row = self._db.execute("SELECT * FROM mf_datasets WHERE id=?", (payload["manifest_id"],)).fetchone()
            if not row:
                raise _error("dataset_unavailable", 404)
            self._db.execute("UPDATE mf_datasets SET registered=1 WHERE id=?", (payload["manifest_id"],))
            return {"dataset": {**_parse(row["manifest"]), "registered": True}, "registered": True}
        match = re.fullmatch(r"/datasets/([A-Za-z0-9_-]{1,100})/records", route)
        if match and method == "GET":
            return self._metadata_page(match[1], offset, limit)
        match = re.fullmatch(r"/runs/(mf-[0-9a-f]{24})(?:/(steps|resources|events|evaluations|export))?", route)
        if match and method == "GET":
            rid, action = match.groups()
            row = self._get(rid)
            run = self._run(row)
            m, status, summary = _parse(row["manifest"]), _parse(row["status"]), _parse(row["summary"]) if row["summary"] else None
            for key in ("optimizer_step", "samples_processed", "total_updates", "samples_total", "last_emitted_at", "telemetry_complete"):
                status[key] = run[key]
            if action is None:
                return {"run": run, "manifest": m, "status": status, "summary": summary, "artifacts": m["artifacts"]}
            if action == "evaluations":
                return self._page(_parse(row["evaluations"]), offset, limit, "evaluations")
            if action == "export":
                if q.get("format", "json") not in {"json", "csv"}:
                    raise _error("invalid_request")
                return {"schema_version": SCHEMA, "mode": "mediflow", "synthetic": False, "run": run, "manifest": m,
                        "status": status, "summary": summary, "artifacts": m["artifacts"],
                        **self._page(_parse(row["evaluations"]), offset, limit, "evaluations")}
            after = self._int_query(q, "after_seq", 0)
            if "limit" not in q:
                limit = 500
            stream = self._db.execute("SELECT * FROM mf_streams WHERE run_id=? AND stream=?", (rid, action)).fetchone()
            if (q.get("stream_epoch") is not None and q["stream_epoch"] != stream["epoch"]) or (after > 0 and not q.get("stream_epoch") and stream["generation"]):
                raise _error("cursor_reset", 409)
            if after > stream["max_seq"]:
                raise _error("cursor_reset", 409)
            records = self._db.execute("SELECT value FROM mf_records WHERE run_id=? AND stream=? AND seq>? ORDER BY seq LIMIT ?", (rid, action, after, limit + 1)).fetchall()
            items = [_parse(r["value"]) for r in records[:limit]]
            return {"items": items, "next_after_seq": items[-1]["seq"] if items else after, "has_more": len(records) > limit,
                    "stream_epoch": stream["epoch"], "telemetry_complete": run["telemetry_complete"]}
        raise _error("not_found", 404)

    def _metadata_page(self, did, offset, limit):
        row = self._db.execute("SELECT * FROM mf_datasets WHERE id=?", (did,)).fetchone()
        if not row:
            raise _error("not_found", 404)
        m = _parse(row["manifest"])
        out = {"records": [], "offset": offset, "limit": limit, "total": m["record_count"], "has_more": offset + limit < m["record_count"],
               "availability": {"available": False, "reason": "metadata_index_not_provided"}}
        if not m["metadata_index"]:
            return out
        try:
            with self._open(str(Path(row["path"], "records.metadata.jsonl"))) as f:
                s = os.fstat(f.fileno())
                signature = _json([s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, m["metadata_sha256"], m["data_hash"]])
                state = self._db.execute("SELECT * FROM mf_metadata_state WHERE id=?", (did,)).fetchone()
                if state and state["signature"] != signature:
                    self._db.execute("DELETE FROM mf_metadata WHERE id=?", (did,))
                    self._db.execute("UPDATE mf_metadata_state SET error='immutable_metadata_changed' WHERE id=?", (did,))
                    self._note("immutable_metadata_changed")
                    out["availability"]["reason"] = "immutable_metadata_changed"
                    return out
                if not state:
                    self._db.execute("INSERT INTO mf_metadata_state(id,signature) VALUES(?,?)", (did, signature))
                    state = self._db.execute("SELECT * FROM mf_metadata_state WHERE id=?", (did,)).fetchone()
                if state["error"]:
                    out["availability"]["reason"] = state["error"]
                    return out
                if not state["complete"]:
                    f.seek(state["offset"])
                    start, count, end = state["offset"], state["count"], state["offset"]
                    complete, error = False, None
                    while f.tell() - start < INGEST_BYTES:
                        pos = f.tell()
                        line = f.readline(MAX_LINE_BYTES + 1)
                        if not line:
                            complete = True
                            break
                        if len(line) > MAX_LINE_BYTES or not line.endswith(b"\n"):
                            error = "invalid_metadata_index"
                            break
                        end = f.tell()
                        if not line.strip():
                            continue
                        # Validate the row once; index stores offsets, not private contents.
                        try:
                            _metadata(_parse(line))
                        except (ValueError, UnicodeError, TypeError, DashboardError, RecursionError):
                            error = "invalid_metadata_index"
                            break
                        self._db.execute("INSERT INTO mf_metadata VALUES(?,?,?,?)", (did, count, pos, len(line)))
                        count += 1
                    if complete and count != m["record_count"]:
                        error = "metadata_count_mismatch"
                    self._db.execute("UPDATE mf_metadata_state SET offset=?,count=?,complete=?,error=? WHERE id=?", (end, count, int(complete), error, did))
                    state = self._db.execute("SELECT * FROM mf_metadata_state WHERE id=?", (did,)).fetchone()
                if state["complete"] and not state["verified"] and not state["error"]:
                    f.seek(0)
                    digest = hashlib.file_digest(f, "sha256").hexdigest() if hasattr(hashlib, "file_digest") else self._file_hash(f)
                    if m["metadata_sha256"] and digest != m["metadata_sha256"]:
                        self._db.execute("UPDATE mf_metadata_state SET error='metadata_hash_mismatch' WHERE id=?", (did,))
                        out["availability"]["reason"] = "metadata_hash_mismatch"
                        self._note("metadata_hash_mismatch")
                        return out
                    self._db.execute("UPDATE mf_metadata_state SET verified=1 WHERE id=?", (did,))
                elif state["error"] or not state["complete"]:
                    out["availability"]["reason"] = state["error"] or "metadata_indexing_in_progress"
                    return out
                indexes = self._db.execute("SELECT offset,length FROM mf_metadata WHERE id=? AND rownum>=? ORDER BY rownum LIMIT ?", (did, offset, limit)).fetchall()
                records = []
                for item in indexes:
                    f.seek(item["offset"])
                    records.append(_metadata(_parse(f.read(item["length"]))))
                out.update(records=records, availability={"available": True, "reason": None})
                return out
        except (OSError, ValueError, UnicodeError, DashboardError, TypeError, RecursionError):
            self._note("metadata_unavailable")
            out["availability"]["reason"] = "metadata_unavailable"
            return out

    @staticmethod
    def _file_hash(f):
        h = hashlib.sha256()
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
        return h.hexdigest()
