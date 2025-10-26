#!/usr/bin/env bash
# Batch generation for configs/generated_cn_ai_by_class/* 55个职业
# Mirrors scripts/run_full_resume_llm.sh but targets the China Occupational Classification taxonomy outputs.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 1) Load API credentials
if [[ -f "$ROOT_DIR/API_Key.md" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/API_Key.md"
else
  echo "[ERROR] API_Key.md not found at $ROOT_DIR" >&2
  exit 1
fi

# 2) Runtime environment defaults (override via env when needed)
export ENABLE_PDF_PARSING="${ENABLE_PDF_PARSING:-0}"
export LLM_MAX_RETRIES="${LLM_MAX_RETRIES:-1}"
export OPENAI_TIMEOUT="${OPENAI_TIMEOUT:-400}"
export LLM_REWRITE_SEARCH_QUERY="${LLM_REWRITE_SEARCH_QUERY:-0}"
export FALLBACK_TO_TEMPLATE="${FALLBACK_TO_TEMPLATE:-0}"

TARGET_PER_PROFESSION="${TARGET_PER_PROFESSION:-15}"
GENERATE_MAX_WORKERS="${GENERATE_MAX_WORKERS:-32}"
GENERATE_LIMIT="${GENERATE_LIMIT:-}"
SKIP_GENERATION="${SKIP_GENERATION:-1}"
TAXONOMY_PATH="${TAXONOMY_PATH:-$ROOT_DIR/configs/cn_taxonomy_ai_agents_by_classification_flat.json}"

CONFIG_DIR="$ROOT_DIR/configs/generated_cn_ai"
mkdir -p "$CONFIG_DIR"

# Normalize CLI targets (accept both foo.json or foo)
declare -a ARG_IDS=()
declare -a NORMALIZED_TARGETS=()
if [[ $# -gt 0 ]]; then
  for raw in "$@"; do
    name="${raw##*/}"
    if [[ "$name" == *.json ]]; then
      id="${name%.json}"
    else
      id="$name"
      name="${name}.json"
    fi
    ARG_IDS+=("$id")
    NORMALIZED_TARGETS+=("$name")
  done
fi

if [[ "$SKIP_GENERATION" != "1" ]]; then
  if [[ ! -f "$TAXONOMY_PATH" ]]; then
    echo "[ERROR] Taxonomy file not found: $TAXONOMY_PATH" >&2
    exit 2
  fi

  echo "[info] Running incremental config generation via generate_profession_configs.py"
  echo "[info] Target per profession: $TARGET_PER_PROFESSION"

  gen_cmd=(
    python3 "$ROOT_DIR/scripts/generate_profession_configs.py"
    --taxonomy "$TAXONOMY_PATH"
    --output-dir "$CONFIG_DIR"
    --incremental
    --target-per-profession "$TARGET_PER_PROFESSION"
    --max-workers "$GENERATE_MAX_WORKERS"
  )

  if [[ ${#ARG_IDS[@]} -gt 0 ]]; then
    gen_cmd+=(--industries "${ARG_IDS[@]}")
  fi
  if [[ -n "$GENERATE_LIMIT" ]]; then
    gen_cmd+=(--limit "$GENERATE_LIMIT")
  fi

  echo "[info] Generation command: ${gen_cmd[*]}"
  "${gen_cmd[@]}"
else
  echo "[info] SKIP_GENERATION=1 → skip incremental config generation."
fi

OUTPUT_BASE="${OUTPUT_BASE:-$ROOT_DIR/output/cn_ai_class}"
PACKAGE_BASE="${PACKAGE_BASE:-$ROOT_DIR/packages/cn_ai_class}"
mkdir -p "$OUTPUT_BASE" "$PACKAGE_BASE"

# Allow filtering by specific classification id (e.g., 2_2_02)
TARGETS=()
if [[ ${#NORMALIZED_TARGETS[@]} -gt 0 ]]; then
  TARGETS=("${NORMALIZED_TARGETS[@]}")
else
  while IFS= read -r -d '' file; do
    TARGETS+=("$(basename "$file")")
  done < <(find "$CONFIG_DIR" -maxdepth 1 -name '*.json' -print0 | sort -z)
fi

if [[ ${#TARGETS[@]} -eq 0 ]]; then
  echo "[ERROR] No config files found under $CONFIG_DIR. Aborting." >&2
  exit 3
fi

LIMIT="${LIMIT:-}"              # default unlimited
MAX_WORKERS="${MAX_WORKERS:-32}" # default concurrency per build_queries call
SKIP_DL="${SKIP_DOWNLOADS:-0}"
NO_INVERSE="${NO_INVERSE:-0}"   # default to positive-only generation
BUILD_INCREMENTAL="${BUILD_INCREMENTAL:-1}"

echo "[info] Using configuration directory: $CONFIG_DIR"
echo "[info] Target files: ${TARGETS[*]}"
echo "[info] OUTPUT_BASE : $OUTPUT_BASE"
echo "[info] PACKAGE_BASE: $PACKAGE_BASE"
echo "[info] MAX_TASKS/Profession: $TARGET_PER_PROFESSION"
echo "[info] LIMIT       : ${LIMIT:-unlimited}"
echo "[info] MAX_WORKERS : $MAX_WORKERS"
echo "[info] LLM rewrite : $LLM_REWRITE_SEARCH_QUERY"
echo "[info] Incremental : $BUILD_INCREMENTAL"

idx=0
total=${#TARGETS[@]}
for filename in "${TARGETS[@]}"; do
  config_path="$CONFIG_DIR/$filename"
  if [[ ! -f "$config_path" ]]; then
    echo "[warn] Skip missing config: $config_path"
    continue
  fi

  idx=$((idx + 1))
  name="${filename%.json}"
  out_path="$OUTPUT_BASE/${name}.jsonl"
  pkg_dir="$PACKAGE_BASE/${name}"

  mkdir -p "$pkg_dir"

  cmd=(
    python3 "$ROOT_DIR/build_queries.py"
    --config "$config_path"
    --output "$out_path"
    --package-dir "$pkg_dir"
    --emit-txt
    --split-views
    --log-level INFO
    --max-workers "$MAX_WORKERS"
  )

  if [[ -n "$LIMIT" ]]; then
    cmd+=(--limit "$LIMIT")
  fi
  if [[ "$SKIP_DL" == "1" || "$SKIP_DL" == "true" ]]; then
    cmd+=(--skip-downloads)
  fi
  if [[ "$NO_INVERSE" == "1" || "$NO_INVERSE" == "true" ]]; then
    cmd+=(--no-inverse)
  fi
  if [[ "$BUILD_INCREMENTAL" == "1" || "$BUILD_INCREMENTAL" == "true" ]]; then
    cmd+=(--incremental)
  fi

  echo "[info] ($idx/$total) Running: ${cmd[*]}"
  "${cmd[@]}"

  echo "[info] Completed $name → JSONL: $out_path | packages: $pkg_dir"
done

echo "[OK] All tasks finished. Outputs under $OUTPUT_BASE, packages under $PACKAGE_BASE."
