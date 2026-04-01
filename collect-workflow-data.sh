#!/usr/bin/env bash
set -euo pipefail

# Requires bash 5+ for associative arrays
if [[ "${BASH_VERSINFO[0]}" -lt 5 ]]; then
    echo "Error: bash 5+ required. You have bash ${BASH_VERSION}." >&2
    echo "  On macOS: brew install bash && /opt/homebrew/bin/bash $0 $*" >&2
    exit 1
fi

# --- Defaults ---
REPO="digitalroute/mz-drx"
MONTH=""
OUTPUT_DIR="./ci-metrics-data"
WORKFLOWS=("mz-ci.yaml" "pe-ci.yaml")
RELEVANT_LABELS=("mz-autotest" "pe-autotest")

# --- Usage ---
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Collect GitHub Actions workflow run data and produce CSV files grouped by PR labels.
Fetches per-job timing to separate queue wait from actual execution time.

Options:
  -r, --repo OWNER/REPO    Repository (default: $REPO)
  -m, --month YYYY-MM       Month to collect (default: interactive picker)
  -o, --output-dir DIR      Output directory (default: $OUTPUT_DIR)
  -h, --help                Show this help
EOF
    exit 0
}

# --- Parse arguments ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        -r|--repo)      REPO="$2"; shift 2 ;;
        -m|--month)     MONTH="$2"; shift 2 ;;
        -o|--output-dir) OUTPUT_DIR="$2"; shift 2 ;;
        -h|--help)      usage ;;
        *) echo "Unknown option: $1" >&2; usage ;;
    esac
done

# --- Prerequisites ---
for cmd in gh jq; do
    if ! command -v "$cmd" &>/dev/null; then
        echo "Error: '$cmd' is required but not found in PATH." >&2
        exit 1
    fi
done

if ! gh auth status &>/dev/null; then
    echo "Error: gh is not authenticated. Run 'gh auth login' first." >&2
    exit 1
fi

# --- Month selection ---
if [[ -z "$MONTH" ]]; then
    # Build list of last 12 months
    months=()
    for i in $(seq 0 11); do
        if [[ "$(uname)" == "Darwin" ]]; then
            months+=("$(date -v-"${i}"m +%Y-%m)")
        else
            months+=("$(date -d "$i months ago" +%Y-%m)")
        fi
    done

    echo "Select month:" >&2
    for i in "${!months[@]}"; do
        # Get month name
        if [[ "$(uname)" == "Darwin" ]]; then
            name=$(date -jf "%Y-%m-%d" "${months[$i]}-01" +%B 2>/dev/null)
        else
            name=$(date -d "${months[$i]}-01" +%B 2>/dev/null)
        fi
        printf "  %2d) %s  %s\n" $((i + 1)) "${months[$i]}" "$name" >&2
    done

    read -rp "Choice [1]: " choice
    choice="${choice:-1}"
    MONTH="${months[$((choice - 1))]}"
fi

# Validate YYYY-MM format
if ! [[ "$MONTH" =~ ^[0-9]{4}-[0-9]{2}$ ]]; then
    echo "Error: Invalid month format '$MONTH'. Expected YYYY-MM." >&2
    exit 1
fi

SINCE="${MONTH}-01"
# Compute last day of month
if [[ "$(uname)" == "Darwin" ]]; then
    UNTIL=$(date -jf "%Y-%m-%d" -v+1m -v-1d "${SINCE}" +%Y-%m-%d 2>/dev/null)
else
    UNTIL=$(date -d "${SINCE} +1 month -1 day" +%Y-%m-%d 2>/dev/null)
fi

echo "Collecting runs for $MONTH ($SINCE to $UNTIL) for repo $REPO" >&2

mkdir -p "$OUTPUT_DIR"

# --- Parallelism setup ---
MAX_PARALLEL=8
TMPDIR_JOBS=$(mktemp -d)
trap 'rm -rf "$TMPDIR_JOBS"' EXIT

# --- ISO 8601 to epoch (cross-platform) ---
iso_to_epoch() {
    local ts="$1"
    if [[ -z "$ts" || "$ts" == "null" ]]; then
        echo 0
        return
    fi
    # Strip fractional seconds if present
    ts="${ts%%.*}Z"
    ts="${ts%%ZZ}Z"
    if [[ "$(uname)" == "Darwin" ]]; then
        date -jf "%Y-%m-%dT%H:%M:%SZ" "$ts" +%s 2>/dev/null || echo 0
    else
        date -d "$ts" +%s 2>/dev/null || echo 0
    fi
}

# --- PR label cache (bash 5 associative array) ---
declare -A PR_CACHE

fetch_pr_info() {
    local branch="$1"

    if [[ -n "${PR_CACHE[$branch]+x}" ]]; then
        echo "${PR_CACHE[$branch]}"
        return
    fi

    local pr_json pr_number label_group base_branch
    pr_json=$(gh pr list -R "$REPO" --head "$branch" --state all --json number,labels,baseRefName -L 1 2>/dev/null || echo "[]")

    if [[ "$pr_json" == "[]" ]] || [[ "$(echo "$pr_json" | jq 'length')" -eq 0 ]]; then
        PR_CACHE[$branch]="|no-labels|unknown"
        echo "|no-labels|unknown"
        return
    fi

    pr_number=$(echo "$pr_json" | jq -r '.[0].number // empty')
    base_branch=$(echo "$pr_json" | jq -r '.[0].baseRefName // "unknown"')

    label_group=$(echo "$pr_json" | jq -r --argjson relevant "$(printf '%s\n' "${RELEVANT_LABELS[@]}" | jq -R . | jq -s .)" \
        '[.[0].labels[].name] | map(select(. as $l | $relevant | index($l))) | sort | join("+")')

    if [[ -z "$label_group" ]]; then
        label_group="no-labels"
    fi

    PR_CACHE[$branch]="${pr_number:-}|${label_group}|${base_branch}"
    echo "${PR_CACHE[$branch]}"
}

# --- Fetch job timing for a run ---
# Returns: first_job_started_at|last_job_completed_at|exec_seconds|exec_minutes|queue_seconds|queue_minutes
fetch_job_timing() {
    local run_id="$1" created_at="$2"

    local jobs_json
    jobs_json=$(gh api "repos/$REPO/actions/runs/$run_id/jobs" \
        --paginate --jq '[.jobs[] | select(.conclusion != null) | {started_at, completed_at}]' 2>/dev/null || echo "[]")

    # --paginate may return multiple JSON arrays (one per page); merge them
    jobs_json=$(echo "$jobs_json" | jq -s 'add // []')

    if [[ "$jobs_json" == "[]" ]] || [[ "$(echo "$jobs_json" | jq 'length')" -eq 0 ]]; then
        echo "||0|0.0|0|0.0"
        return
    fi

    # First job started_at (earliest) and last job completed_at (latest)
    local first_started last_completed
    first_started=$(echo "$jobs_json" | jq -r '[.[].started_at] | sort | first')
    last_completed=$(echo "$jobs_json" | jq -r '[.[].completed_at] | sort | last')

    local first_epoch last_epoch created_epoch
    first_epoch=$(iso_to_epoch "$first_started")
    last_epoch=$(iso_to_epoch "$last_completed")
    created_epoch=$(iso_to_epoch "$created_at")

    # Execution time = last job completed - first job started
    local exec_seconds=0
    if [[ $last_epoch -gt 0 && $first_epoch -gt 0 ]]; then
        exec_seconds=$((last_epoch - first_epoch))
        if [[ $exec_seconds -lt 0 ]]; then exec_seconds=0; fi
    fi
    local exec_minutes
    exec_minutes=$(awk "BEGIN {printf \"%.1f\", $exec_seconds / 60.0}")

    # Queue time = first job started - run created
    local queue_seconds=0
    if [[ $first_epoch -gt 0 && $created_epoch -gt 0 ]]; then
        queue_seconds=$((first_epoch - created_epoch))
        if [[ $queue_seconds -lt 0 ]]; then queue_seconds=0; fi
    fi
    local queue_minutes
    queue_minutes=$(awk "BEGIN {printf \"%.1f\", $queue_seconds / 60.0}")

    echo "${first_started}|${last_completed}|${exec_seconds}|${exec_minutes}|${queue_seconds}|${queue_minutes}"
}

# --- Normalize base branch for grouping ---
# "develop" stays as-is, "mz9*-dev" variants map to "mz9x-dev", others kept as-is
normalize_base_branch() {
    local branch="$1"
    if [[ "$branch" =~ ^mz9[0-9]+-dev$ ]] || [[ "$branch" =~ ^patch/mz-9\. ]]; then
        echo "mz9x-dev"
    else
        # Replace slashes and other unsafe characters with dashes for filenames
        echo "${branch//\//-}"
    fi
}

# --- CSV setup ---
CSV_HEADER="run_id,workflow_name,status,conclusion,created_at,updated_at,wall_clock_seconds,wall_clock_minutes,exec_seconds,exec_minutes,queue_seconds,queue_minutes,first_job_started,last_job_completed,head_branch,event,pr_number,label_group,base_branch"

echo "$CSV_HEADER" > "$OUTPUT_DIR/all-runs.csv"

declare -A HEADER_WRITTEN

write_row() {
    local wf_slug="$1" label_group="$2" row="$3" base_group="$4"
    local filename="${wf_slug}_${label_group}_${base_group}.csv"
    local filepath="$OUTPUT_DIR/$filename"

    if [[ -z "${HEADER_WRITTEN[$filepath]+x}" ]]; then
        echo "$CSV_HEADER" > "$filepath"
        HEADER_WRITTEN[$filepath]=1
    fi

    echo "$row" >> "$filepath"
    echo "$row" >> "$OUTPUT_DIR/all-runs.csv"
}

# --- Batch PR info fetch ---
# Fetch all PRs in the date range upfront to avoid per-branch API calls
echo "Batch-fetching PR info..." >&2
pr_batch_json=$(gh pr list -R "$REPO" --state all \
    --json number,labels,baseRefName,headRefName \
    --search "created:${SINCE}..${UNTIL}" \
    -L 1000 2>/dev/null || echo "[]")

# Build the PR cache from batch results
if echo "$pr_batch_json" | jq empty 2>/dev/null; then
    while IFS='|' read -r branch pr_number base_branch labels_csv; do
        [[ -z "$branch" ]] && continue

        # Filter to relevant labels
        label_group=""
        for rl in "${RELEVANT_LABELS[@]}"; do
            if echo ",$labels_csv," | grep -q ",$rl,"; then
                if [[ -n "$label_group" ]]; then
                    label_group="${label_group}+${rl}"
                else
                    label_group="$rl"
                fi
            fi
        done
        label_group="${label_group:-no-labels}"

        PR_CACHE[$branch]="${pr_number}|${label_group}|${base_branch}"
    done < <(echo "$pr_batch_json" | jq -r '.[] | "\(.headRefName)|\(.number)|\(.baseRefName)|\([.labels[].name] | sort | join(","))"')
    echo "  Cached $(echo "$pr_batch_json" | jq 'length') PRs" >&2
fi

# --- Background job timing fetcher ---
fetch_job_timing_bg() {
    local run_id="$1" created_at="$2" outfile="$3"
    fetch_job_timing "$run_id" "$created_at" > "$outfile"
}

# --- Main loop ---
for wf in "${WORKFLOWS[@]}"; do
    wf_slug="${wf%.yaml}"
    echo "Fetching runs for workflow: $wf ..." >&2

    raw_json=$(gh run list -R "$REPO" -w "$wf" \
        --json databaseId,workflowName,status,conclusion,createdAt,updatedAt,headBranch,event \
        --created "${SINCE}..${UNTIL}" \
        -L 1000 2>&1)

    if ! echo "$raw_json" | jq empty 2>/dev/null; then
        echo "Warning: Failed to fetch runs for $wf: $raw_json" >&2
        continue
    fi

    # Filter to pull_request events and extract all fields in one jq pass
    # Output: tab-separated fields, one run per line
    total=$(echo "$raw_json" | jq 'length')
    runs_tsv=$(echo "$raw_json" | jq -r '.[] | select(.event == "pull_request") | [.databaseId, .workflowName, .status, (.conclusion // "n/a"), .createdAt, .updatedAt, .headBranch] | @tsv')
    run_count=$(echo "$runs_tsv" | grep -c . || true)
    echo "  Found $run_count PR runs (of $total total)" >&2

    # Phase 1: Filter runs and dispatch parallel job timing fetches
    declare -A RUN_META  # run_id -> tab-separated metadata
    kept=0

    while IFS=$'\t' read -r run_id workflow_name status conclusion created_at updated_at head_branch; do
        [[ -z "$run_id" ]] && continue

        pr_info=$(fetch_pr_info "$head_branch")
        pr_number="${pr_info%%|*}"
        rest="${pr_info#*|}"
        label_group="${rest%%|*}"
        base_branch="${rest#*|}"
        base_group=$(normalize_base_branch "$base_branch")

        # Skip runs with no relevant labels
        if [[ "$label_group" == "no-labels" ]]; then
            continue
        fi

        # Skip cross-pairings
        if [[ "$wf_slug" == "mz-ci" && "$label_group" == "pe-autotest" ]]; then
            continue
        fi
        if [[ "$wf_slug" == "pe-ci" && "$label_group" == "mz-autotest" ]]; then
            continue
        fi

        # Store metadata for phase 2
        RUN_META[$run_id]="${workflow_name}	${status}	${conclusion}	${created_at}	${updated_at}	${head_branch}	${pr_number}	${label_group}	${base_branch}	${base_group}"

        # Launch job timing fetch in background
        fetch_job_timing_bg "$run_id" "$created_at" "$TMPDIR_JOBS/$run_id" &

        kept=$((kept + 1))

        # Throttle concurrency
        while (( $(jobs -r | wc -l) >= MAX_PARALLEL )); do
            wait -n 2>/dev/null || true
        done

        if (( kept % 25 == 0 )); then
            echo "  Dispatched $kept job timing fetches..." >&2
        fi
    done <<< "$runs_tsv"

    # Wait for all background fetches to complete
    wait
    echo "  All $kept job timing fetches complete" >&2

    # Phase 2: Assemble CSV rows from metadata + job timing results
    for run_id in "${!RUN_META[@]}"; do
        IFS=$'\t' read -r workflow_name status conclusion created_at updated_at head_branch pr_number label_group base_branch base_group <<< "${RUN_META[$run_id]}"

        # Wall-clock duration
        created_epoch=$(iso_to_epoch "$created_at")
        updated_epoch=$(iso_to_epoch "$updated_at")
        wall_seconds=$((updated_epoch - created_epoch))
        if [[ $wall_seconds -lt 0 ]]; then wall_seconds=0; fi
        wall_minutes=$(awk "BEGIN {printf \"%.1f\", $wall_seconds / 60.0}")

        # Read job timing from temp file
        job_timing=""
        if [[ -f "$TMPDIR_JOBS/$run_id" ]]; then
            job_timing=$(cat "$TMPDIR_JOBS/$run_id")
            rm -f "$TMPDIR_JOBS/$run_id"
        fi
        job_timing="${job_timing:-||0|0.0|0|0.0}"
        IFS='|' read -r first_started last_completed exec_seconds exec_minutes queue_seconds queue_minutes <<< "$job_timing"

        row="${run_id},${workflow_name},${status},${conclusion},${created_at},${updated_at},${wall_seconds},${wall_minutes},${exec_seconds},${exec_minutes},${queue_seconds},${queue_minutes},${first_started},${last_completed},${head_branch},pull_request,${pr_number},${label_group},${base_branch}"

        write_row "$wf_slug" "$label_group" "$row" "$base_group"
    done

    unset RUN_META
    echo "  Done processing $wf ($kept runs kept)" >&2
done

# --- Summary ---
echo "" >&2
echo "Output files:" >&2
for f in "$OUTPUT_DIR"/*.csv; do
    count=$(($(wc -l < "$f") - 1))
    echo "  $f ($count runs)" >&2
done
