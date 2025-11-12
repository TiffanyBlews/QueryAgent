#!/usr/bin/env bash
# Step 1 (Resume): extract project specs from Markdown resumes via LLM

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 1) Load API credentials (needed for the extractor LLM call)
if [[ -f "$ROOT_DIR/API_Key.md" ]]; then
  # shellcheck disable=SC1091
  source "$ROOT_DIR/API_Key.md"
else
  echo "[ERROR] API_Key.md not found at $ROOT_DIR" >&2
  exit 1
fi

OUT_PATH="${OUT_PATH:-$ROOT_DIR/configs/generated/offered_resume_queries_llm.json}"
MAX_WORKERS="${MAX_WORKERS:-16}"
mkdir -p "$(dirname "$OUT_PATH")"

# Allow callers to pass resume directories either via CLI args or RESUME_DIRS/RESUME_DIR envs
declare -a RESUME_DIR_ARGS=()
if [[ $# -gt 0 ]]; then
  for dir in "$@"; do
    RESUME_DIR_ARGS+=("$dir")
  done
elif [[ -n "${RESUME_DIRS:-}" ]]; then
  IFS=',' read -r -a RESUME_DIR_ARGS <<< "$RESUME_DIRS"
elif [[ -n "${RESUME_DIR:-}" ]]; then
  RESUME_DIR_ARGS+=("$RESUME_DIR")
else
  RESUME_DIR_ARGS+=("$ROOT_DIR/offered_resume")
fi

cmd=(
  python3 "$ROOT_DIR/scripts/generate_resume_queries_llm.py"
  --out "$OUT_PATH"
  --max-workers "$MAX_WORKERS"
)

for dir in "${RESUME_DIR_ARGS[@]}"; do
  if [[ -z "$dir" ]]; then
    continue
  fi
  resolved=""
  if [[ -d "$dir" ]]; then
    resolved="$(cd "$dir" && pwd)"
  elif [[ -d "$ROOT_DIR/$dir" ]]; then
    resolved="$(cd "$ROOT_DIR/$dir" && pwd)"
  fi
  if [[ -n "$resolved" ]]; then
    cmd+=(--resume-dir "$resolved")
  else
    echo "[warn] Resume dir not found: $dir" >&2
  fi
done

if [[ -n "${MODEL:-}" ]]; then
  cmd+=(--model "$MODEL")
fi
if [[ -n "${OPENAI_BASE_URL:-}" ]]; then
  cmd+=(--openai-base-url "$OPENAI_BASE_URL")
fi
if [[ -n "${OPENAI_API_KEY:-}" ]]; then
  cmd+=(--openai-api-key "$OPENAI_API_KEY")
fi

echo "[info] Generating resume-derived configs → $OUT_PATH"
echo "[info] Running: ${cmd[*]}"
"${cmd[@]}"

echo "[OK] Resume configs ready at $OUT_PATH (achievable subset emitted alongside)."
