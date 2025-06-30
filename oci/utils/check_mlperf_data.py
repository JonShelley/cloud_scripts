#!/usr/bin/env python3
import json
import re
from pathlib import Path


def process_log_file(log_path: Path):
    """
    Process a single SLURM log file:
    - Extract MLLOG run_start and run_stop times (time_ms)
    - Extract number_of_nodes from :::SYSJSON line
    - Calculate duration in minutes
    - Pull compliance-invoked log filename and learning rate
    Returns a dict of extracted values or None if entries missing.
    """
    lines = log_path.read_text().splitlines()

    # Extract MLLOG start and stop lines
    start_line = next((l for l in lines if 'run_start' in l), None)
    stop_line = next((l for l in lines if 'run_stop' in l), None)
    if not start_line or not stop_line:
        return None

    def extract_time_ms(line: str) -> int:
        json_part = line.split('MLLOG', 1)[1].strip()
        return json.loads(json_part).get('time_ms', 0)

    start_ms = extract_time_ms(start_line)
    stop_ms = extract_time_ms(stop_line)
    duration_min = (stop_ms - start_ms) / 60000.0

    # number_of_nodes from SYSJSON
    num_nodes = None
    for l in lines:
        if l.startswith(':::SYSJSON'):
            try:
                json_part = l.split(':::SYSJSON', 1)[1].strip()
                data = json.loads(json_part)
                num_nodes = int(data.get('number_of_nodes', 0))
            except (json.JSONDecodeError, ValueError):
                num_nodes = None
            break

    # compliance-invoked log filename (basename only)
    logf = None
    for l in lines:
        if 'INFO - Running compliance on file:' in l:
            logf = Path(l.split('file:')[-1].strip()).name
            break

    # learning rate
    lr = None
    for l in lines:
        if 'opt_base_learning_rate' in l:
            try:
                json_part = l.split('MLLOG', 1)[1].strip()
                lr = json.loads(json_part).get('value')
            except json.JSONDecodeError:
                lr = None
            break

    return {
        'filename': log_path.name,
        'num_nodes': num_nodes,
        'logf': logf,
        'learning_rate': lr,
        'start_ms': start_ms,
        'stop_ms': stop_ms,
        'duration_min': duration_min,
    }


def main():
    # Locate compliance files in current directory
    compliance_files = sorted(Path('.').glob('compliance_*.out'))
    results = []

    for comp in compliance_files:
        lines = comp.read_text().splitlines()
        if any('INFO - SUCCESS' in l for l in lines):
            # First line contains log filename; basename only
            basename = Path(lines[0].strip()).name
            log_path = comp.parent / basename
            if log_path.exists():
                result = process_log_file(log_path)
                if result:
                    results.append(result)
            else:
                print(f"Warning: log file {basename} for {comp.name} not found.")

    # Sort results by number of nodes
    results_sorted = sorted(results, key=lambda x: (x['num_nodes'] is None, x['num_nodes']))

    # Print header
    print("filename\tnum_nodes\tlogf\tlearning_rate\tstart_ms\tstop_ms\tduration_min")
    for r in results_sorted:
        print("\t".join(str(r[k]) for k in [
            'filename', 'num_nodes', 'logf', 'learning_rate',
            'start_ms', 'stop_ms', 'duration_min'
        ]))

if __name__ == '__main__':
    main()

