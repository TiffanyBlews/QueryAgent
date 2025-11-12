# CN AI 职业任务生成流水线

本仓库实现了一个面向《中国职业分类大典》的自动化题库生成与打包流水线。它先依据官方职业分类与SOP规范，使用LLM批量生成“职业 × (L3/L4/L5)”的任务描述，再通过检索+判题代理构造可复现实验、Ground Truth、评估标准以及打包文件。最终产物可直接用于中文智能体评测或人类考试命题。

## 输入 (Inputs)

### 1. 分类与职业基线
- `configs/cn_taxonomy_ai_agents_by_classification_flat.json`：行业→职业→描述的扁平化 taxonomy，是整条流水线的主输入。
- `中华人民共和国职业分类大典（2022版）*.txt`：原始制度文本，便于校验 taxonomy，与代码流程无直接依赖。

### 2. 运行所需密钥与模型配置
根目录需要有 `API_Key.md` ：
```bash
export OPENAI_API_KEY="sk-..."
export OPENAI_BASE_URL=''
export MODEL="gpt-4.1"
export SERPER_API_KEY="serper-..."
# 可选：OPENAI_BASE_URL、CRAWL_API_KEY/SECRET 用于自建网关或PDF解析
```
如需启用 PDF 解析，还需 `CRAWL_API_ENDPOINT`、`CRAWL_API_KEY`、`CRAWL_API_SECRET`。

### 3. 先验知识库 (可选)
- `resources/persona_bank/personas.jsonl`：预置 persona，用于 `context_builder` 在 prompt 中生成角色画像。
- `context_sources/**/metadata.json`：如果存在，`config_loader` 会自动把已有调研资料注入 Prompt。
- `ground_truth_cache/`：由 `query_agent/ground_truth_cache.py` 维护的下载缓存；重复任务会直接复用已下载的 Ground Truth。

## 输出 (Outputs)

| 位置 | 说明 |
| --- | --- |
| `configs/generated_cn_ai/*.json` | Step 1 输出的“任务规格”文件，含 L3/L4/L5 任务描述，Step 2 直接读取同一目录。|
| `output/cn_ai_class/<industry>_<timestamp>.jsonl` | 批量生成后的正式任务，每行都是带 Ground Truth、搜索记录、评估指南的 JSON。启用 `--incremental` 时会按时间戳追加。|
| `packages/cn_ai_class/<industry>/<level>/<orientation>/<query_id>/` | 完整打包目录；含 `query.json`、`solver_query.json`、`search_results.json`、`ground_truth/metadata.json(+下载件)`、`data_room/` (参考资料) 以及 `task.txt`。|
| `final_packages/<package-dir>/...` | “瘦身版”包，仅保留 `task.txt`、`data_room/`、`ground_truth/`，方便分发。|
| `output/*.txt` | 默认 `--emit-txt`，会生成聚合版纯文本任务清单。|
| `reports/*.md` | `scripts/analyse_cn_ai_output.py` 的统计输出。|

## 端到端流水线 (Process)

1. **生成职业任务配置** (`scripts/step1_generate_configs.sh` → `scripts/generate_profession_configs.py`)
   - 输入：taxonomy 中的行业/职业、历史任务（若 `--incremental`）。
   - 操作：针对每个职业向 LLM 发送带 SOP 约束的 prompt，要求产出 L3/L4/L5 三个任务：包含 `scenario`、`task_focus`、`deliverable_requirements`、`evaluation_focus`、`search_queries`。
   - 产物：默认写入 `configs/generated_cn_ai/<industry>.json`（可用 `OUTPUT_CONFIG_DIR` 改写）。`--incremental` 会合并已有任务、自动跳过重复 `query_id`；`--target-per-profession` 控制每个职业的任务条数。

2. **构造可执行评估任务** (`scripts/step2_generate_queries.sh` → `build_queries.py` + `query_agent/*`)
   - `load_specs` 读取 `configs/generated_cn_ai/*.json` 中的规格（脚本会检查目录是否存在，若缺失请先运行 Step 1），并补齐 persona、context、search query（可通过 `LLM_SEARCH_QUERY=1` 让 LLM 重写检索词）。
   - 通过 `inverse_utils.expand_with_inverse_specs` 自动复制出逆向任务（可用 `--no-inverse` 关闭）。
   - `QueryConstructionAgent` 对每个 `QuerySpec` 执行：
     1. 调用 Serper (或 Google CSE / DuckDuckGo 回退) 获取检索结果，并可根据需要放宽 query。
     2. `ground_truth.select_ground_truth_bundle` 选出可下载的主证据 + supporting，必要时用 `ground_truth_cache` 落地文件。
     3. 组合 persona、context、SOP lint 检查，构建消息后请求 OpenAI-compatible 模型生成结构化任务 JSON。
     4. 若设 `ENABLE_PDF_PARSING=1`，`pdf_parser` 会调用爬虫服务抽取 PDF 正文，提升 Ground Truth 可读性。
     5. `packager.save_query_package` 将任务、ground_truth、搜索记录、参考资料下载到包目录，并生成 solver/judge 双视图。可用 `--skip-downloads`、`--split-views`、`--emit-txt` 控制行为。
   - `generate_batch` 默认并行（`--max-workers` 或 `QUERY_AGENT_MAX_WORKERS`），失败任务最多重试三次，并在增量模式下跳过已存在的 `query_id`。

3. **结果巡检/统计** (`scripts/analyse_cn_ai_output.py`)
   - 扫描 `output/cn_ai_class/*.jsonl`，统计各分类任务总数、层级、正/逆向分布、平均检索词数量、职业覆盖度。
   - 可通过 `--report report.md` 输出 Markdown 报表，辅助验收与对比。

## 快速上手

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Step 1：生成/刷新职业任务配置
LIMIT=5 bash scripts/step1_generate_configs.sh          # LIMIT 控制抽样职业数

# Step 2：基于配置批量生成任务、打包产物
MAX_WORKERS=16 bash scripts/step2_generate_queries.sh 2_2_02.json 4_4_02.json

# 生成完毕后可做统计
python3 scripts/analyse_cn_ai_output.py --input output/cn_ai_class --report reports/cn_ai.md
```

常用参数：
- `LIMIT`（Step 1 & Step 2）：可限制 Step 1 中 `generate_profession_configs.py` 处理的职业数量，以及 Step 2 中 `build_queries.py --limit` 生成的任务数量；默认不设限。
- `MAX_WORKERS`（Step 1/Step 2）：分别影响配置与任务生成阶段的线程数，默认 16 与 32。
- `TARGET_PER_PROFESSION`（Step 1）：传递给 `generate_profession_configs.py --target-per-profession`，控制每个职业的任务条数。
- `SKIP_DOWNLOADS=1`：跳过参考资料/ground truth 下载，仅写元数据。
- `NO_INVERSE=1`：只输出正向任务。

## 代码结构速览

```
├── query_agent/
│   ├── agent.py                # QueryConstructionAgent 及批量调度
│   ├── config_loader.py        # 解析 profession/config JSON，自动补齐 context
│   ├── search.py               # Serper + Google CSE + DuckDuckGo 检索封装
│   ├── ground_truth*.py        # Ground Truth 选取与缓存
│   ├── packager.py             # 产出包目录并下载证据
│   ├── persona_registry.py     # persona 选取逻辑
│   └── ...
├── scripts/
│   ├── step1_generate_configs.sh  # 包装 generate_profession_configs.py
│   ├── step2_generate_queries.sh  # 串联 build_queries.py + 打包流程
│   ├── generate_profession_configs.py
│   └── analyse_cn_ai_output.py
├── configs/
│   ├── cn_taxonomy_ai_agents_by_classification_flat.json
│   └── generated_cn_ai/
└── resources/persona_bank/personas.jsonl
```

## 常见自定义点

| 控制项 | 使用位置 | 作用 |
| --- | --- | --- |
| `LLM_REWRITE_SEARCH_QUERY=1` | `build_queries.py` | 在生成阶段再次让 LLM 优化检索词，提升可检索性。|
| `FALLBACK_TO_TEMPLATE=1` | `QueryConstructionAgent` | LLM 失败时改走内置模板（无 Ground Truth 下载）。|
| `ENABLE_LOCAL_OVERRIDES=1` | `search.py` | 把特定 query 映射到本地 `file://` 资料，可用于权威手册离线复用。|
| `ENABLE_PDF_PARSING=1` | `agent.py`/`pdf_parser.py` | 调用爬虫服务抽取 PDF 正文，并随包落地。|
| `BUILD_INCREMENTAL=1` | `scripts/step2_generate_queries.sh` → `build_queries.py` | 避免重复写入已存在的 query，方便多次追加。|

通过以上输入、输出与流程梳理，可以快速定位数据源、管线环节以及调试入口，支持对职业评测任务的批量构建、打包与质量评估。
