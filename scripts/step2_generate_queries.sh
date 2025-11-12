#!/usr/bin/env bash

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

# 二者选其一
# 1. 根据职业体系的config生成任务
CONFIG_DIR="${CONFIG_DIR:-$ROOT_DIR/configs/generated_cn_ai}"

# 2. 根据简历config生成任务
# CONFIG_DIR="${CONFIG_DIR:-$ROOT_DIR/configs/generated}"

if [[ ! -d "$CONFIG_DIR" ]]; then
  echo "[ERROR] Config directory not found: $CONFIG_DIR" >&2
  echo "[hint] Please run scripts/step1_generate_configs.sh first or set CONFIG_DIR." >&2
  exit 2
fi

# Normalize CLI targets (accept both foo.json or foo)
declare -a NORMALIZED_TARGETS=()
if [[ $# -gt 0 ]]; then
  for raw in "$@"; do
    name="${raw##*/}"
    if [[ "$name" != *.json ]]; then
      name="${name}.json"
    fi
    NORMALIZED_TARGETS+=("${name}")
  done
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
