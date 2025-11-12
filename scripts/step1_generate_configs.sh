#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -f "$ROOT_DIR/API_Key.md" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/API_Key.md"
else
  echo "[ERROR] API_Key.md not found at $ROOT_DIR" >&2
  exit 1
fi

CONFIG_TAXONOMY="${CONFIG_TAXONOMY:-$ROOT_DIR/configs/cn_taxonomy_ai_agents_by_classification_flat.json}"
OUTPUT_CONFIG_DIR="${OUTPUT_CONFIG_DIR:-$ROOT_DIR/configs/generated_cn_ai}"
TARGET_PER_PROFESSION="${TARGET_PER_PROFESSION:-15}"
MAX_WORKERS="${MAX_WORKERS:-16}"
LIMIT="${LIMIT:-}"
INCREMENTAL="${INCREMENTAL:-1}"
OVERWRITE="${OVERWRITE:-0}"

mkdir -p "$OUTPUT_CONFIG_DIR"

# Normalize CLI targets (accept both foo.json or foo)
declare -a ARG_IDS=()
if [[ $# -gt 0 ]]; then
  for raw in "$@"; do
    name="${raw##*/}"
    if [[ "$name" == *.json ]]; then
      id="${name%.json}"
    else
      id="$name"
    fi
    ARG_IDS+=("$id")
  done
fi

if [[ ! -f "$CONFIG_TAXONOMY" ]]; then
  echo "[ERROR] Taxonomy file not found: $CONFIG_TAXONOMY" >&2
  exit 2
fi

gen_cmd=(
  python3 "$ROOT_DIR/scripts/generate_profession_configs.py"
  --taxonomy "$CONFIG_TAXONOMY"
  --output-dir "$OUTPUT_CONFIG_DIR"
  --target-per-profession "$TARGET_PER_PROFESSION"
  --max-workers "$MAX_WORKERS"
)

if [[ ${#ARG_IDS[@]} -gt 0 ]]; then
  gen_cmd+=(--industries "${ARG_IDS[@]}")
fi
if [[ -n "$LIMIT" ]]; then
  gen_cmd+=(--limit "$LIMIT")
fi
if [[ "$INCREMENTAL" == "1" || "$INCREMENTAL" == "true" ]]; then
  gen_cmd+=(--incremental)
elif [[ "$OVERWRITE" == "1" || "$OVERWRITE" == "true" ]]; then
  gen_cmd+=(--overwrite)
fi

echo "[info] Generating configs into $OUTPUT_CONFIG_DIR"
echo "[info] Command: ${gen_cmd[*]}"
"${gen_cmd[@]}"
