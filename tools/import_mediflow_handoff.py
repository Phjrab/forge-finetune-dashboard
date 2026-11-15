#!/usr/bin/env python3
"""Read an owner handoff into a private, status-only catalog; never dispatch jobs."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile

SCHEMA = 'mediflow.telemetry.v1'


def read_json(path, root):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts or path.is_symlink():
        raise ValueError('UNSAFE_SOURCE_PATH')
    resolved = path.resolve(strict=True)
    resolved.relative_to(root)
    current = path
    while current != root:
        if current.is_symlink():
            raise ValueError('SYMLINK_SOURCE_FORBIDDEN')
        current = current.parent
    if resolved.stat().st_size > 32 * 1024 * 1024:
        raise ValueError('SOURCE_TOO_LARGE')
    raw = resolved.read_bytes()
    return json.loads(raw), hashlib.sha256(raw).hexdigest()


def atomic(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix='.handoff-')
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(value, stream, ensure_ascii=False, allow_nan=False)
            stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def import_handoff(handoff_path, data_root, telemetry_root):
    data_root = Path(data_root).resolve(strict=True)
    handoff, digest = read_json(handoff_path, data_root)
    if handoff.get('writer') != 'TRAINING_OWNER':
        raise ValueError('TRAINING_OWNER_HANDOFF_REQUIRED')
    old = handoff['old_campaign']
    campaign = old['campaign_id']
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', campaign):
        raise ValueError('UNSAFE_CAMPAIGN_ID')
    manifest, _ = read_json(old['manifest_path'], data_root)
    completed = handoff.get('completed_first_seed', {})
    active = {row['job_id']: row for row in handoff.get('active_workers', [])}
    held = old.get('unstarted_jobs', {})
    root = Path(telemetry_root).resolve()
    source_ref = {'artifact_id': 'training_handoff.json', 'sha256': digest}
    imported = []
    for job in manifest['jobs']:
        job_id = job['job_id']
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}', job_id):
            raise ValueError('UNSAFE_JOB_ID')
        # Only the ten first-seed cells and owner's explicit migration holds are
        # surfaced. Preparatory jobs with private source payloads stay private.
        if job_id not in completed and job_id not in active and job_id not in held:
            continue
        progress = active.get(job_id, {}).get('progress') or {}
        progress_path = active.get(job_id, {}).get('attempt_dir')
        if progress_path:
            candidate = Path(progress_path) / 'progress.json'
            if candidate.exists():
                fresh, _ = read_json(candidate, data_root)
                if fresh.get('job_id') == job_id:
                    progress = fresh
        proof = completed.get(job_id, {})
        done = proof.get('final_checkpoint_verified') is True and proof.get('fresh_process_reload_verified') is True
        if done:
            receipt, receipt_hash = read_json(proof['receipt'], data_root)
            if receipt_hash != proof.get('receipt_sha256') or receipt.get('status') != 'SUCCEEDED':
                raise ValueError('COMPLETED_RECEIPT_HASH_MISMATCH')
            progress = {**progress, 'optimizer_step': receipt.get('optimizer_steps'),
                        'total_updates': receipt.get('optimizer_steps'),
                        'samples_processed': receipt.get('samples_processed'),
                        'total_examples': receipt.get('samples_processed')}
        status = 'succeeded' if done else 'running' if job_id in active else 'held'
        stage = str(progress.get('stage') or '')
        phase = ('succeeded' if done else 'held' if status == 'held' else
                 'reloading' if 'reload' in stage else 'saving' if 'save' in stage else 'training')
        spec = {'schema_version': SCHEMA, 'campaign_id': campaign, 'job_id': job_id,
                'run_id': job.get('run_id', job_id), 'attempt_id': 'coordination',
                'source': 'mediflow_historical', 'producer': 'mediflow.handoff',
                'model': job.get('model_id'), 'task': job.get('task'), 'seed': None,
                'data_hash': job.get('data_hash'), 'scientific_recipe_hash': None,
                'code_sha': handoff.get('repository_head'), 'telemetry_version': SCHEMA,
                'eval_protocol': None, 'precision': None, 'parent_run_id': None,
                'freeze_id': None, 'dataset_ids': [], 'total_updates': progress.get('total_updates'),
                'samples_total': progress.get('total_examples'), 'created_at': None,
                'historical': True, 'artifacts': [], 'source_artifacts': [source_ref], 'cohort_id': None}
        if done:
            spec['artifacts'] = [{'artifact_id': 'final-checkpoint-receipt', 'kind': 'receipt',
                                  'sha256': proof.get('receipt_sha256'), 'exists': True, 'verified': True,
                                  'reload_verified': True, 'selection': 'ONE_PASS_FINAL_NOT_MINIMUM_LOSS'}]
        state = {'schema_version': SCHEMA, 'status': status, 'phase': phase,
                 'optimizer_step': progress.get('optimizer_step'), 'total_updates': progress.get('total_updates'),
                 'samples_processed': progress.get('samples_processed'), 'samples_total': progress.get('total_examples'),
                 'last_emitted_at': proof.get('finished_at') if done else progress.get('last_progress_at'),
                 'training_succeeded': True if done else None, 'reload_succeeded': True if done else None,
                 'evaluation_completed': None, 'telemetry_complete': False,
                 'telemetry_error': 'legacy_worker_has_no_live_loss_stream', 'dropped_records': 0,
                 'gaps': [], 'writer_health': 'unknown'}
        directory = root / campaign / job_id / 'coordination'
        atomic(directory / 'status.json', state)
        atomic(directory / 'manifest.json', spec)
        imported.append(job_id)
    return {'campaign_id': campaign, 'status_only_runs': len(imported), 'job_ids': imported,
            'handoff_sha256': digest, 'gpu_started': False, 'originals_modified': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--handoff', required=True, type=Path)
    parser.add_argument('--data-root', required=True, type=Path)
    parser.add_argument('--telemetry-root', required=True, type=Path)
    args = parser.parse_args()
    print(json.dumps(import_handoff(args.handoff, args.data_root, args.telemetry_root), indent=2))
