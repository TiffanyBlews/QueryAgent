#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_TAXONOMY="${ROOT_DIR}/configs/cn_taxonomy_ai_agents_by_classification_flat.json"
OUTPUT_CONFIG_DIR="${ROOT_DIR}/configs/generated_cn_ai_by_class"
MAX_WORKERS="${MAX_WORKERS:-16}"
LIMIT_FLAG=""
if [[ -n "${LIMIT:-}" ]]; then
  LIMIT_FLAG="--limit ${LIMIT}"
fi

python3 "${ROOT_DIR}/scripts/generate_profession_configs.py" \
  --taxonomy "${CONFIG_TAXONOMY}" \
  --output-dir "${OUTPUT_CONFIG_DIR}" \
  --max-workers "${MAX_WORKERS}" \
  --overwrite ${LIMIT_FLAG}

bash "${ROOT_DIR}/scripts/run_cn_taxonomy_ai.sh" "$@"
