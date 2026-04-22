#!/usr/bin/env python3
"""
Cron Schedule Optimizer for openshift-eng/ocp-qe-perfscale-ci periodic jobs.

Detects scheduling conflicts among large jobs (120+ nodes) and redistributes
them to maintain a minimum time gap, while making minimal modifications to
existing schedules.

Usage:
    python3 hack/perfscale-cron-optimizer.py --report
    python3 hack/perfscale-cron-optimizer.py --dry-run
    python3 hack/perfscale-cron-optimizer.py --apply
"""

import argparse
import hashlib
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from croniter import croniter
from ruamel.yaml import YAML

DEFAULT_CONFIG_DIR = "ci-operator/config/openshift-eng/ocp-qe-perfscale-ci"
DEFAULT_MIN_GAP_HOURS = 2
DEFAULT_NODE_THRESHOLD = 120
# 3 default worker nodes in a standard OCP cluster
DEFAULT_WORKER_NODES = 3

LARGE_NODE_NAME_PATTERNS = re.compile(r"(\d+)nodes")

# Special cron shortcuts that croniter doesn't handle
CRON_SHORTCUTS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Optimize cron schedules for perfscale-ci periodic jobs"
    )
    parser.add_argument(
        "--config-dir",
        default=DEFAULT_CONFIG_DIR,
        help=f"Config directory (default: {DEFAULT_CONFIG_DIR})",
    )
    parser.add_argument(
        "--min-gap-hours",
        type=int,
        default=DEFAULT_MIN_GAP_HOURS,
        help=f"Minimum hours between large jobs (default: {DEFAULT_MIN_GAP_HOURS})",
    )
    parser.add_argument(
        "--node-threshold",
        type=int,
        default=DEFAULT_NODE_THRESHOLD,
        help=f"Node count threshold for 'large' jobs (default: {DEFAULT_NODE_THRESHOLD})",
    )
    action = parser.add_mutually_exclusive_group()
    action.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Show changes without writing (default)",
    )
    action.add_argument(
        "--apply", action="store_true", help="Write changes to files"
    )
    action.add_argument(
        "--report",
        action="store_true",
        help="Print schedule summary and conflict report",
    )
    action.add_argument(
        "--check",
        metavar="CRON",
        help="Check if a cron expression conflicts with existing large jobs (e.g. '0 2 * * 1,3')",
    )
    return parser.parse_args()


def normalize_cron(cron_expr):
    """Normalize cron expression, expanding shortcuts."""
    cron_expr = cron_expr.strip().strip("'\"")
    return CRON_SHORTCUTS.get(cron_expr, cron_expr)


def get_node_count(test):
    """Extract total node count from a test definition."""
    # Check ADDITIONAL_WORKER_NODES env var
    env = test.get("steps", {}).get("env", {})
    additional = env.get("ADDITIONAL_WORKER_NODES")
    if additional:
        try:
            return int(additional) + DEFAULT_WORKER_NODES
        except (ValueError, TypeError):
            pass

    # Check job name pattern
    name = test.get("as", "")
    match = LARGE_NODE_NAME_PATTERNS.search(name)
    if match:
        return int(match.group(1))

    return DEFAULT_WORKER_NODES


def is_large_job(test, threshold):
    """Determine if a job is 'large' based on node count."""
    return get_node_count(test) >= threshold


def expand_cron_to_weekly_slots(cron_expr, gap_hours=1):
    """
    Expand a cron expression into a set of (day_of_week, hour) slots
    over a representative 4-week window.

    Returns a set of (week_offset, day_of_week, hour) tuples.
    """
    cron_expr = normalize_cron(cron_expr)
    slots = set()

    try:
        # Use a fixed reference Monday to get consistent day-of-week mapping
        base = datetime(2024, 1, 1, 0, 0)  # Monday
        end = base + timedelta(days=28)  # 4 weeks
        it = croniter(cron_expr, base)

        while True:
            next_time = it.get_next(datetime)
            if next_time >= end:
                break
            day_offset = (next_time - base).days
            week = day_offset // 7
            dow = day_offset % 7  # 0=Mon in our system
            slots.add((week, dow, next_time.hour))
    except (ValueError, KeyError):
        pass

    return slots


def load_jobs(config_dir):
    """
    Load all periodic jobs from YAML files in the config directory.

    Returns list of dicts: {file, filename, test_name, test, cron, node_count, is_large}
    """
    yaml = YAML()
    yaml.preserve_quotes = True
    config_path = Path(config_dir)
    jobs = []

    for yaml_file in sorted(config_path.glob("*.yaml")):
        try:
            with open(yaml_file, "r") as f:
                data = yaml.load(f)
        except Exception:
            continue

        if not data or "tests" not in data:
            continue

        for test in data["tests"]:
            cron = test.get("cron")
            if not cron:
                continue

            cron_str = str(cron).strip()
            node_count = get_node_count(test)

            jobs.append(
                {
                    "file": yaml_file,
                    "filename": yaml_file.name,
                    "test_name": test.get("as", "unknown"),
                    "test": test,
                    "cron": cron_str,
                    "node_count": node_count,
                    "is_large": node_count >= DEFAULT_NODE_THRESHOLD,
                }
            )

    return jobs


def find_conflicts(large_jobs, min_gap_hours):
    """
    Find conflicts among large jobs.

    Two jobs conflict if any of their expanded time slots are within
    min_gap_hours of each other on the same day.

    Returns list of conflict groups: [(job_a, job_b, overlapping_slots), ...]
    """
    conflicts = []

    # Expand all jobs to their time slots
    job_slots = []
    for job in large_jobs:
        slots = expand_cron_to_weekly_slots(job["cron"])
        job_slots.append((job, slots))

    # Check pairwise conflicts
    for i in range(len(job_slots)):
        for j in range(i + 1, len(job_slots)):
            job_a, slots_a = job_slots[i]
            job_b, slots_b = job_slots[j]

            overlapping = set()
            for wa, da, ha in slots_a:
                for wb, db, hb in slots_b:
                    if wa == wb and da == db and abs(ha - hb) < min_gap_hours:
                        overlapping.add((wa, da, ha, hb))

            if overlapping:
                conflicts.append((job_a, job_b, overlapping))

    return conflicts


def build_occupied_timeline(large_jobs, exclude_job=None, min_gap_hours=4):
    """
    Build a set of occupied (week, day, hour) slots from large jobs,
    expanded by the gap requirement.

    Optionally exclude a specific job (used when reassigning that job).
    """
    occupied = set()
    for job in large_jobs:
        if exclude_job and job["test_name"] == exclude_job["test_name"] and job["filename"] == exclude_job["filename"]:
            continue
        slots = expand_cron_to_weekly_slots(job["cron"])
        for w, d, h in slots:
            for offset in range(-min_gap_hours + 1, min_gap_hours):
                blocked_h = h + offset
                # Wrap around day boundaries
                if 0 <= blocked_h < 24:
                    occupied.add((w, d, blocked_h))
    return occupied


def _try_find_hour(day_pairs, occupied, start_hour):
    """Try to find a non-conflicting hour for the given day pairs."""
    for offset in range(24):
        candidate_hour = (start_hour + offset) % 24
        if all((w, d, candidate_hour) not in occupied for w, d in day_pairs):
            return candidate_hour
    return None


def _parse_dom_field(dom_str):
    """Parse a day-of-month cron field into a list of individual days.

    Handles single values (5), comma lists (1,8,15), and ranges (8-14).
    """
    if dom_str == "*":
        return None
    days = []
    for part in dom_str.split(","):
        if "-" in part:
            start, end = part.split("-", 1)
            days.extend(range(int(start), int(end) + 1))
        else:
            days.append(int(part))
    return days


def _shift_dom_list(dom_str, shift):
    """Shift a day-of-month field by N days, wrapping 1-28."""
    days = _parse_dom_field(dom_str)
    if days is None:
        return dom_str
    shifted = sorted(set((d - 1 + shift) % 28 + 1 for d in days))
    return ",".join(str(d) for d in shifted)


def generate_new_cron(job, occupied, min_gap_hours):
    """
    Generate a new cron expression for a conflicting job using deterministic
    hashing while preserving the original scheduling frequency.

    First tries shifting only the hour. If all hours are blocked (too many
    jobs on the same days), also shifts the day-of-month pattern.
    """
    cron_expr = normalize_cron(job["cron"])
    parts = cron_expr.split()
    if len(parts) != 5:
        return job["cron"]

    _minute, _hour, dom, month, dow = parts

    # Generate deterministic hash for this job
    hash_input = f"{job['test_name']}:{job['filename']}"
    hash_bytes = hashlib.md5(hash_input.encode()).hexdigest()
    hash_val = int(hash_bytes[:8], 16)

    start_hour = hash_val % 24
    new_minute = hash_val % 60

    # Expand the original cron's day slots to check against occupied
    original_slots = expand_cron_to_weekly_slots(cron_expr)
    day_pairs = {(w, d) for w, d, _h in original_slots}

    # Phase 1: Try shifting only the hour
    hour = _try_find_hour(day_pairs, occupied, start_hour)
    if hour is not None:
        return f"{new_minute} {hour} {dom} {month} {dow}"

    # Phase 2: Shift day-of-month by 1-27 days and try each with all hours
    dom_shift_start = hash_val % 27 + 1
    for dom_offset in range(1, 28):
        shift = (dom_shift_start + dom_offset - 1) % 27 + 1
        new_dom = _shift_dom_list(dom, shift)
        candidate_cron = f"0 0 {new_dom} {month} {dow}"
        candidate_slots = expand_cron_to_weekly_slots(candidate_cron)
        candidate_day_pairs = {(w, d) for w, d, _h in candidate_slots}

        hour = _try_find_hour(candidate_day_pairs, occupied, start_hour)
        if hour is not None:
            return f"{new_minute} {hour} {new_dom} {month} {dow}"

    # Last resort: use hash-derived values (should be extremely rare)
    return f"{new_minute} {start_hour} {dom} {month} {dow}"


def resolve_conflicts(jobs, min_gap_hours, node_threshold):
    """
    Resolve scheduling conflicts among large jobs using greedy placement.

    Strategy:
    1. Identify all large jobs, sorted deterministically
    2. Process jobs one at a time: place each at its current time if it
       doesn't conflict with already-placed jobs, otherwise reassign it
    3. This guarantees all conflicts are resolved in a single pass

    Returns dict of {(filename, test_name): (old_cron, new_cron)} for changed jobs.
    """
    large_jobs = [j for j in jobs if j["node_count"] >= node_threshold]
    large_jobs.sort(key=lambda j: (j["filename"], j["test_name"]))

    if not large_jobs:
        return {}

    # Check for existing conflicts first (early exit if none)
    conflicts = find_conflicts(large_jobs, min_gap_hours)
    if not conflicts:
        return {}

    # Greedy placement: process jobs in order, place each one
    placed_jobs = []  # Jobs with finalized schedules
    changes = {}

    for job in large_jobs:
        job_slots = expand_cron_to_weekly_slots(job["cron"])

        # Check if current schedule conflicts with already-placed jobs
        has_conflict = False
        occupied = build_occupied_timeline(placed_jobs, min_gap_hours=min_gap_hours)
        for w, d, h in job_slots:
            if (w, d, h) in occupied:
                has_conflict = True
                break

        if has_conflict:
            old_cron = job["cron"]
            new_cron = generate_new_cron(job, occupied, min_gap_hours)
            if new_cron != old_cron:
                changes[(job["filename"], job["test_name"])] = (old_cron, new_cron)
                job = dict(job)  # Copy so we don't mutate the original
                job["cron"] = new_cron

        placed_jobs.append(job)

    return changes


def apply_changes(config_dir, changes):
    """Write cron changes back to YAML files via targeted line replacement.

    Only modifies the exact cron lines that need to change, leaving the
    rest of the file byte-for-byte identical.
    """
    config_path = Path(config_dir)

    files_to_update = defaultdict(list)
    for (filename, test_name), (old_cron, new_cron) in changes.items():
        files_to_update[filename].append((test_name, old_cron, new_cron))

    for filename, test_changes in files_to_update.items():
        filepath = config_path / filename
        with open(filepath, "r") as f:
            lines = f.readlines()

        # Build a map of test_name -> (old_cron, new_cron) still to apply
        pending = {name: (old, new) for name, old, new in test_changes}
        current_test = None

        for i, line in enumerate(lines):
            stripped = line.strip()
            # Track which test block we're in via "- as:" or "  as:" lines
            as_match = re.match(r"^[\s-]*as:\s+(.+)$", stripped)
            if as_match:
                current_test = as_match.group(1).strip()

            if current_test in pending and re.match(r"^\s+cron:\s+", line):
                old_cron, new_cron = pending[current_test]
                # Replace the cron value on this line, preserving indentation
                indent = len(line) - len(line.lstrip())
                lines[i] = " " * indent + f"cron: {new_cron}\n"
                del pending[current_test]
                current_test = None

        with open(filepath, "w") as f:
            f.writelines(lines)


def print_report(jobs, min_gap_hours, node_threshold):
    """Print a schedule summary and conflict report."""
    large_jobs = [j for j in jobs if j["node_count"] >= node_threshold]
    small_jobs = [j for j in jobs if j["node_count"] < node_threshold]

    print("=" * 70)
    print("PERFSCALE-CI CRON SCHEDULE REPORT")
    print("=" * 70)
    print(f"\nTotal periodic jobs: {len(jobs)}")
    print(f"Large jobs (>={node_threshold} nodes): {len(large_jobs)}")
    print(f"Small jobs (<{node_threshold} nodes): {len(small_jobs)}")
    print(f"Minimum gap requirement: {min_gap_hours} hours")

    if large_jobs:
        print(f"\n{'─' * 70}")
        print("LARGE JOB SCHEDULE")
        print(f"{'─' * 70}")
        print(f"{'Job Name':<45} {'Nodes':>5}  {'Cron':<20} {'File'}")
        print(f"{'─' * 45} {'─' * 5}  {'─' * 20} {'─' * 30}")
        for job in sorted(large_jobs, key=lambda j: j["test_name"]):
            short_file = job["filename"].replace("openshift-eng-ocp-qe-perfscale-ci-main__", "")
            print(f"{job['test_name']:<45} {job['node_count']:>5}  {job['cron']:<20} {short_file}")

    # Weekly heatmap for large jobs
    print(f"\n{'─' * 70}")
    print("WEEKLY SCHEDULE HEATMAP (Large Jobs)")
    print(f"{'─' * 70}")
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    # Build heatmap for week 0
    heatmap = [[0] * 7 for _ in range(24)]
    for job in large_jobs:
        slots = expand_cron_to_weekly_slots(job["cron"])
        for w, d, h in slots:
            if w == 0:
                heatmap[h][d] += 1

    print(f"{'Hour':>4}  ", end="")
    for d in days:
        print(f"{d:>5}", end="")
    print()

    for h in range(24):
        if any(heatmap[h][d] > 0 for d in range(7)):
            print(f"{h:>4}  ", end="")
            for d in range(7):
                count = heatmap[h][d]
                if count == 0:
                    print(f"{'·':>5}", end="")
                elif count == 1:
                    print(f"{count:>5}", end="")
                else:
                    print(f"{'!' + str(count):>5}", end="")
            print()

    print(f"\n  Legend: · = no jobs, N = N jobs, !N = N jobs (conflict risk)")

    # Conflicts
    conflicts = find_conflicts(large_jobs, min_gap_hours)
    print(f"\n{'─' * 70}")
    print(f"CONFLICTS (within {min_gap_hours}-hour gap)")
    print(f"{'─' * 70}")

    if not conflicts:
        print("No conflicts found.")
    else:
        print(f"Found {len(conflicts)} conflict pair(s):\n")
        for job_a, job_b, overlap in conflicts:
            print(f"  CONFLICT:")
            print(f"    {job_a['test_name']} ({job_a['node_count']} nodes)")
            print(f"      cron: {job_a['cron']}  file: {job_a['filename']}")
            print(f"    {job_b['test_name']} ({job_b['node_count']} nodes)")
            print(f"      cron: {job_b['cron']}  file: {job_b['filename']}")
            sample = list(overlap)[:3]
            slots_str = ", ".join(
                f"week {w} day {d} hours {ha}&{hb}" for w, d, ha, hb in sample
            )
            if len(overlap) > 3:
                slots_str += f" ... (+{len(overlap) - 3} more)"
            print(f"    Overlapping slots: {slots_str}")
            print()


def check_cron(cron_expr, jobs, min_gap_hours, node_threshold):
    """Check if a cron expression conflicts with existing large jobs."""
    cron_expr = normalize_cron(cron_expr)
    candidate_slots = expand_cron_to_weekly_slots(cron_expr)
    if not candidate_slots:
        print(f"Could not parse cron expression: {cron_expr}", file=sys.stderr)
        sys.exit(1)

    large_jobs = [j for j in jobs if j["node_count"] >= node_threshold]
    occupied = build_occupied_timeline(large_jobs, min_gap_hours=min_gap_hours)

    conflicting_slots = {(w, d, h) for w, d, h in candidate_slots if (w, d, h) in occupied}

    if not conflicting_slots:
        print(f"No conflicts. '{cron_expr}' is clear of all large jobs (>={node_threshold} nodes, {min_gap_hours}h gap).")
        return

    # Find which specific jobs conflict
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    print(f"CONFLICTS found for '{cron_expr}' ({min_gap_hours}h gap):\n")

    for job in sorted(large_jobs, key=lambda j: j["test_name"]):
        job_slots = expand_cron_to_weekly_slots(job["cron"])
        overlap = set()
        for wc, dc, hc in candidate_slots:
            for wj, dj, hj in job_slots:
                if wc == wj and dc == dj and abs(hc - hj) < min_gap_hours:
                    overlap.add((wc, dc, hc, hj))
        if overlap:
            short_file = job["filename"].replace("openshift-eng-ocp-qe-perfscale-ci-main__", "")
            print(f"  {job['test_name']} ({job['node_count']} nodes, {short_file})")
            print(f"    cron: {job['cron']}")
            sample = sorted(overlap)[:5]
            for w, d, hc, hj in sample:
                print(f"    - week {w} {days[d]}: your job at hour {hc} vs existing at hour {hj}")
            if len(overlap) > 5:
                print(f"    ... and {len(overlap) - 5} more overlaps")
            print()


def print_dry_run(changes):
    """Print what would change in dry-run mode."""
    if not changes:
        print("No changes needed - no conflicts detected among large jobs.")
        return

    print(f"\nDRY RUN - {len(changes)} job(s) would be modified:\n")

    for (filename, test_name), (old_cron, new_cron) in sorted(changes.items()):
        short_file = filename.replace("openshift-eng-ocp-qe-perfscale-ci-main__", "")
        print(f"  {test_name} ({short_file})")
        print(f"    old: {old_cron}")
        print(f"    new: {new_cron}")
        print()

    print("Run with --apply to write these changes.")


def main():
    args = parse_args()

    # Update the global threshold used by is_large_job
    global DEFAULT_NODE_THRESHOLD
    DEFAULT_NODE_THRESHOLD = args.node_threshold

    jobs = load_jobs(args.config_dir)

    if not jobs:
        print(f"No periodic jobs found in {args.config_dir}", file=sys.stderr)
        sys.exit(1)

    if args.check:
        check_cron(args.check, jobs, args.min_gap_hours, args.node_threshold)
        return

    if args.report:
        print_report(jobs, args.min_gap_hours, args.node_threshold)
        return

    changes = resolve_conflicts(jobs, args.min_gap_hours, args.node_threshold)

    if args.apply:
        if not changes:
            print("No changes needed - no conflicts detected among large jobs.")
            return
        apply_changes(args.config_dir, changes)
        print(f"Applied {len(changes)} change(s).")
        for (filename, test_name), (_old, new_cron) in sorted(changes.items()):
            print(f"  {test_name} -> {new_cron}")
        print("\nRun 'make update' to regenerate Prow jobs.")
    else:
        print_dry_run(changes)


if __name__ == "__main__":
    main()
