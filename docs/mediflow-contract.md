# Mediflow bridge contract v1

`MediflowBridge(root, db_path=None)` is a standard-library-only read model for
`mediflow.telemetry.v1` and `mediflow.dataset-reference.v1`. It does not import a
trainer, initialize CUDA, start subprocesses, acquire a GPU lease, signal a worker,
change a schedule, or rewrite engine artifacts. `close()` closes SQLite only.

The server supplies an administrator-approved telemetry directory and a separate
private SQLite path. An explicit `db_path` permits a read-only telemetry root. If
omitted, the cache defaults to `root/.forge-mediflow.sqlite3`. Keep this cache and
all telemetry outside public static directories and Git. The database contains
sanitized API records, source digests, private relative locations, and indexing
state; it is not an HTTP artifact.

## HTTP facade and ownership

`dispatch(method, path, query=None, payload=None)` receives a full
`/api/mediflow/...` path and a string-valued query dictionary and returns a JSON
object. Errors use the existing `simulator.DashboardError`. DashboardServer owns
Host/Origin validation, authentication, `remote_view`, session/CSRF checks and
`remote_control`. Never expose the facade through an unauthenticated route.

Supported reads are `/health`, `/campaigns`, `/runs`, `/runs/<id>`,
`/runs/<id>/{steps,resources,events,evaluations,export}`, `/datasets`, and
`/datasets/<id>/records`. The sole write is `POST /datasets/register` with exactly
`{"manifest_id":"approved-catalog-id"}`; it records reference registration in the
cache. It cannot upload, transform, split, or preprocess a frozen dataset.

The server returns HTTP201 for registration. Both export formats return the same
sanitized aggregate JSON object from the bridge; the server serializes CSV when
requested. Export is paginated like evaluations; use `offset`, `limit`, `total`,
and `has_more` to retrieve all aggregates. It never includes step dumps, prompts,
answers, images, model generations, arbitrary files or raw predictions.

`capabilities()` advertises read and reference registration. Execution controls
start/pause/resume/cancel/retry are false: Mediflow's supervisor remains the
execution owner and partial optimizer/RNG resume is unsupported.

## Discovery, identity and replay

Attempt files are discovered at `<root>/<campaign>/<run>/<attempt>/manifest.json`.
Dataset manifests are discovered at `<root>/datasets/<safe-id>/manifest.json`.
Discovery runs on polling requests, so future manifests appear without a bridge
restart. Snapshot stat signatures cache unchanged sanitized JSON. Missing or
unsafe manifests immediately remove cached listings. IDs never interpolate a
browser-supplied path. `mf-` IDs are the first 24 hex characters of SHA256 over
canonical JSON `[campaign_id,job_id,run_id,attempt_id]`.

Each stream has a separate durable offset, producer, sequence index and epoch.
Only LF-terminated JSONL records are committed; partial trailing lines are held
for the next poll. Repeated identical sequence/content is ignored. Conflicting
content, cross-attempt envelopes, producer changes, gaps and sequence regressions
are recorded as integrity errors and make telemetry completeness false. Existing
points are never silently overwritten. Nonfinite metrics become null with an
availability reason.

The client saves separate `(after_seq,stream_epoch)` values for steps, resources
and events. Epochs survive a normal bridge restart. Inode replacement, truncation,
source removal, or a changed suffix at the committed boundary invalidate the
stream cache. An incompatible epoch or cursor beyond the stream returns
`mediflow_cursor_reset` HTTP409. Clear only that chart/cursor and request from 0
without an epoch. Raw source files are untouched. Same-inode rewrites that change
only an old prefix while preserving size and the committed boundary are outside
append-only producer semantics; producers must rotate files for replacement.

Only a running live attempt becomes stale after 30 seconds without a timestamped
update; terminal/historical records do not become stale merely with age. Missing
or unknown loss stays null, and training/reload/evaluation success remain separate.
Campaign completion counts distinct verified first-seed condition job IDs rather
than attempts, smoke jobs or evaluation jobs.

## Bounded resources and immutable references

Ordinary pages default to 100 records, maximum 500. Stream pages default to 500,
maximum 1000. These bound response sizes, not source row counts. The bridge handles
sources larger than 1000 rows and 128KiB without passing them through Forge's old
JSONL text import. Stream ingestion processes at most 8MiB per stream/request;
remaining rows continue on subsequent polling requests. Individual source lines
are limited to 1MiB and JSON snapshots to 8MiB as explicit disk/memory safety
limits. Oversize or invalid input yields sanitized health/integrity diagnostics.
No medical input or exception traceback is echoed.

Canonical group dictionaries, restriction flags and license URLs remain intact.
Dataset listing and registration use manifest counts and hashes without scanning
canonical data or a metadata index. Optional metadata must use the fixed filename
`records.metadata.jsonl`; every directory/file open uses no-follow descriptors.
Symlinks, traversal and arbitrary browser paths are rejected, including concurrent
symlink replacement at open time.

The first metadata request incrementally builds a persisted row-offset index,
reading at most 8MiB per request. Until indexing is complete it returns empty rows
with `availability.reason=metadata_indexing_in_progress`; retry the same request.
Before serving rows, the bridge verifies row count and the declared metadata hash
when present. Subsequent pages seek directly using SQLite offsets; unchanged
indexes are neither rescanned nor rehashed after restart. A changed immutable
index fails closed, requiring a new dataset ID/version. References without a
metadata index still expose their manifest count/hash, with unavailable metadata.
Canonical source transformation, dataset resplitting and native image processing
remain Mediflow-owned.

Metadata pages expose only source/revision, sample/group IDs, split, task/language,
image hash, license/restrictions, bounded derivation/review provenance and freeze
ID. Stream/status/summary/artifact/evaluation output uses explicit field
allowlists. Event messages are fixed safe labels and arbitrary details are
excluded. Diagnostic reasons publish bounded reason codes. Evaluation aggregates
retain evaluator names and denominators, including OPEN/CLOSED subgroup labels,
latency scope and supported confidence interval fields; scoring is never
recomputed. Unknown raw fields are discarded before caching or export.

## CPU verification

```bash
python3 -m unittest discover -s tests -p test_mediflow.py -v
```

Tests generate safe fixtures in temporary directories. They cover 1505 stream
rows and 2207 metadata rows, sources over 128KiB, durable restart cursors, partial
lines, idempotency/conflicts, file replacement/truncation/removal, unavailable
sensors, finite/null semantics, namespace isolation, stale/historical status,
first-seed counts, future discovery, pagination, frozen reference registration,
metadata hash/count/mutation checks, no-follow path security, evaluation and
cache sanitization, aggregate export, and concurrent reads. These tests do not
claim GPU probes or real browser/live telemetry acceptance.
