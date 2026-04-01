#!/usr/bin/env bash
set -euo pipefail

if [[ "${BASH_VERSINFO[0]}" -lt 5 ]]; then
    echo "Error: bash 5+ required. You have bash ${BASH_VERSION}." >&2
    echo "  On macOS: brew install bash && /opt/homebrew/bin/bash $0 $*" >&2
    exit 1
fi

MIN_RUNS=20
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${1:-./ci-metrics-data}"
GNUPLOT_SCRIPT="$SCRIPT_DIR/plot-pipeline-duration.gp"

if ! command -v gnuplot &>/dev/null; then
    echo "Error: 'gnuplot' is required but not found in PATH." >&2
    exit 1
fi

if [[ ! -d "$DATA_DIR" ]]; then
    echo "Error: Data directory '$DATA_DIR' not found. Run collect-workflow-data.sh first." >&2
    exit 1
fi

# --- Combined overview plot from all-runs.csv ---
if [[ -f "$DATA_DIR/all-runs.csv" ]]; then
    echo "Generating combined overview plot..." >&2
    gnuplot -e "datafile='$DATA_DIR/all-runs.csv'; outfile='$DATA_DIR/all-runs.png'; gtitle='All CI Runs — Execution Time (excl. queue)'; scriptdir='$SCRIPT_DIR'" \
        "$GNUPLOT_SCRIPT"
    echo "  -> $DATA_DIR/all-runs.png" >&2
fi

# --- Per-group plots ---
for csv in "$DATA_DIR"/*.csv; do
    [[ "$(basename "$csv")" == "all-runs.csv" ]] && continue

    group=$(basename "$csv" .csv)
    png="$DATA_DIR/${group}.png"

    # Skip groups with fewer than MIN_RUNS data rows
    data_rows=$(( $(wc -l < "$csv") - 1 ))
    if [[ "$data_rows" -lt "$MIN_RUNS" ]]; then
        echo "Skipping $group ($data_rows runs < $MIN_RUNS minimum)" >&2
        continue
    fi

    # Build a readable title from the filename
    # Filename format: workflow_labels_basebranch
    # e.g. mz-ci_mz-autotest_develop -> "mz-ci — mz-autotest (target: develop)"
    title=$(echo "$group" | sed -E 's/_([^_]+)$/ (target: \1)/; s/_/ — /; s/\+/ + /g')
    title="$title — Execution Time (excl. queue)"

    echo "Generating plot for $group ..." >&2
    gnuplot -e "datafile='$csv'; outfile='$png'; gtitle='$title'; scriptdir='$SCRIPT_DIR'" \
        "$GNUPLOT_SCRIPT"
    echo "  -> $png" >&2
done

echo "" >&2
echo "All plots generated in $DATA_DIR/" >&2
