"""
High-level agent that stitches together search + LLM prompting to produce queries.
"""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Sequence
import re
import threading

from .llm import LLMError, OpenAIChatClient
from .prompting import build_messages
from .packager import save_query_package
from .search import SearchError, SearchResult, serper_search
from .spec import QuerySpec
from .data_structures import ContextBundle, EvaluationGuide, GroundTruthBundle, PersonaProfile
from .ground_truth import select_ground_truth_bundle
from .ground_truth_cache import cache_ground_truth_bundle
from .sop_linter import lint_payload
from .pdf_parser import WebPDFParser, PDFParsingError

logger = logging.getLogger(__name__)


def _relax_search_query(query: str) -> str:
    """
    Remove restrictive operators, such as domain scopes, file type filters, and
    year ranges, to broaden the search surface.
    """
    original = query or ""
    relaxed = original

    # Remove file type constraints like filetype:pdf
    relaxed = re.sub(r"\bfiletype:\S+", "", relaxed, flags=re.IGNORECASE)

    # Remove chained site filters introduced with OR (e.g., OR site:gov.cn)
    relaxed = re.sub(r"\bOR\b\s+site:\S+", "", relaxed, flags=re.IGNORECASE)

    # Remove any remaining site:domain filters
    relaxed = re.sub(r"\bsite:\S+", "", relaxed, flags=re.IGNORECASE)

    # Drop explicit year ranges such as 2022..2025
    relaxed = re.sub(r"\b\d{4}\.\.\d{4}\b", "", relaxed)

    # Collapse multiple spaces and trim
    relaxed = re.sub(r"\s{2,}", " ", relaxed).strip()

    # Do not return an empty query; fall back to the original if needed.
    return relaxed or original


def _build_query_variants(query: str) -> List[str]:
    """
    Construct the ordered list of search query variants, starting with the
    original and appending relaxed forms if they differ.
    """
    base = query or ""
    variants: List[str] = [base]

    relaxed = _relax_search_query(base)
    if relaxed and relaxed not in variants:
        variants.append(relaxed)

    return variants


class QueryConstructionAgent:
    """
    Orchestrates Serper search and LLM prompt construction for batch query generation.
    """

    def __init__(
        self,
        *,
        llm_client: Optional[OpenAIChatClient] = None,
        serper_endpoint: str = "https://google.serper.dev/search",
        market: str = "us",
        context_blocks: Optional[Sequence[Dict[str, str]]] = None,
    ) -> None:
        # offline 模式已移除：始终使用 LLM 客户端
        self.llm = llm_client or OpenAIChatClient()
        self.serper_endpoint = serper_endpoint
        self.market = market
        self.context_blocks: List[Dict[str, str]] = list(context_blocks or [])

        # Initialize PDF parser (optional, controlled by environment variable)
        self.enable_pdf_parsing = os.environ.get("ENABLE_PDF_PARSING", "0").lower() in ("1", "true", "yes")
        if self.enable_pdf_parsing:
            try:
                self.pdf_parser = WebPDFParser()
                logger.info("PDF parsing enabled and initialized successfully")
            except PDFParsingError as exc:
                logger.warning("Failed to initialize PDF parser: %s. PDF parsing will be disabled.", exc)
                self.pdf_parser = None
                self.enable_pdf_parsing = False
        else:
            self.pdf_parser = None
            logger.debug("PDF parsing is disabled by configuration")

    def run_search(self, spec: QuerySpec, *, num_results: int = 5) -> List[SearchResult]:
        language = "zh" if spec.language.lower().startswith("zh") else "en"
        if os.environ.get("SKIP_WEB_SEARCH") == "1":
            snippet_fragments: List[str] = []
            if spec.scenario:
                snippet_fragments.append(spec.scenario)
            if spec.task_focus:
                snippet_fragments.append("任务聚焦：" + "；".join(spec.task_focus[:3]))
            if spec.deliverable_requirements:
                snippet_fragments.append("交付要求：" + "；".join(spec.deliverable_requirements[:2]))
            snippet = " ".join(snippet_fragments) or f"Placeholder ground truth for {spec.query_id}"
            placeholder = SearchResult(
                title=f"Internal Knowledge Pack · {spec.query_id}",
                url=f"https://example.com/internal/{spec.query_id}",
                snippet=snippet,
                source="internal-placeholder",
                date=None,
                search_query=spec.search_query,
            )
            logger.info(
                "Skipping live search for '%s' (level=%s) due to SKIP_WEB_SEARCH=1.",
                spec.search_query,
                spec.level,
            )
            return [placeholder]

        aggregated_results: List[SearchResult] = []
        seen_keys: set[str] = set()
        errors: List[str] = []

        for base_idx, base_query in enumerate(spec.search_queries):
            query_variants = _build_query_variants(base_query)
            last_variant_index: Optional[int] = None
            results: Optional[List[SearchResult]] = None
            last_error: Optional[Exception] = None

            for attempt in range(3):
                variant_index = min(attempt, len(query_variants) - 1)
                query_variant = query_variants[variant_index]

                if last_variant_index != variant_index and variant_index > 0:
                    logger.info(
                        "Relaxing search query[%d] for '%s': '%s' -> '%s'",
                        base_idx,
                        spec.query_id,
                        base_query,
                        query_variant,
                    )
                last_variant_index = variant_index

                try:
                    remaining = max(num_results - len(aggregated_results), 1)
                    results = serper_search(
                        query_variant,
                        endpoint=self.serper_endpoint,
                        market=self.market,
                        language=language,
                        num=remaining,
                    )
                    break
                except SearchError as exc:
                    last_error = exc
                    logger.warning(
                        "Search failed for '%s' (level=%s) on attempt %d with query '%s': %s",
                        spec.query_id,
                        spec.level,
                        attempt + 1,
                        query_variant,
                        exc,
                    )
                    if attempt < 2:
                        time.sleep(2 ** attempt)

            if results is None:
                if last_error:
                    errors.append(str(last_error))
                continue

            for result in results:
                key = result.url or f"{result.title}|{result.snippet}"
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                aggregated_results.append(result)
                if len(aggregated_results) >= num_results:
                    break

            logger.info(
                "Search for '%s' (query[%d]=%s) returned %d results (level=%s).",
                spec.query_id,
                base_idx,
                query_variants[0],
                len(results),
                spec.level,
            )

            if len(aggregated_results) >= num_results:
                break

        if not aggregated_results:
            error_msg = "; ".join(errors) or "No results"
            raise SearchError(
                f"Search failed for '{spec.query_id}' (level={spec.level}) using queries {spec.search_queries}: {error_msg}"
            )

        logger.debug(
            "Aggregated %d search results for %s from %d base query variants.",
            len(aggregated_results),
            spec.query_id,
            len(spec.search_queries),
        )

        return aggregated_results[:num_results]

    def _enhance_pdf_results(self, results: List[SearchResult]) -> List[SearchResult]:
        """
        Enhance search results by parsing PDF content for better query generation.
        Only runs if PDF parsing is enabled.
        """
        if not self.enable_pdf_parsing or not self.pdf_parser:
            return results

        enhanced_results = []
        for result in results:
            if not self.pdf_parser.is_pdf_url(result.url):
                enhanced_results.append(result)
                continue

            # Try to parse PDF content
            logger.info("Attempting to parse PDF content from: %s", result.url)
            success, pdf_content = self.pdf_parser.parse_pdf_url_safe(result.url)

            if success and pdf_content.strip():
                # Create enhanced result with PDF content
                # Truncate content if too long (keep first 5000 chars for prompt efficiency)
                truncated_content = pdf_content[:5000]
                if len(pdf_content) > 5000:
                    truncated_content += "\n\n[Content truncated...]"

                enhanced_result = SearchResult(
                    title=result.title,
                    url=result.url,
                    snippet=truncated_content,  # Replace snippet with parsed content
                    source=f"{result.source or 'unknown'}-pdf-parsed",
                    date=result.date,
                    search_query=result.search_query,
                )
                enhanced_results.append(enhanced_result)
                logger.info(
                    "Successfully enhanced PDF result for %s (content length: %d chars)",
                    result.url,
                    len(pdf_content),
                )
            else:
                # Keep original result if parsing failed
                enhanced_results.append(result)
                logger.warning("Failed to parse PDF content from %s, using original snippet", result.url)

        return enhanced_results

    def build_query(self, spec: QuerySpec, *, search_results: Optional[Sequence[SearchResult]] = None) -> Dict:
        """
        Generate a single query dictionary given a specification.
        """

        try:
            results = list(search_results) if search_results else self.run_search(spec)
        except SearchError as exc:
            logger.error("Search failed for %s: %s", spec.query_id, exc)
            raise

        # Enhance PDF results with parsed content for better query generation
        enhanced_results = self._enhance_pdf_results(results)

        context_bundle = spec.context_bundle or self._fallback_context(spec)
        language = "zh" if spec.language.lower().startswith("zh") else "en"
        ground_truth_bundle = select_ground_truth_bundle(
            spec,
            enhanced_results,  # Use enhanced results for ground truth selection
            serper_endpoint=self.serper_endpoint,
            market=self.market,
            language=language,
        )

        combined_context_blocks = list(self.context_blocks)
        combined_context_blocks.extend(spec.context_documents or [])

        gt_cache_info = cache_ground_truth_bundle(ground_truth_bundle)

        messages = build_messages(spec, context_bundle, ground_truth_bundle, combined_context_blocks)
        try:
            raw_output = self.llm.run_json_completion(messages)
        except LLMError as exc:
            # Optional fallback to rule-based template when LLM is unavailable
            fallback_to_template = os.environ.get("FALLBACK_TO_TEMPLATE", "0").lower() in ("1", "true", "yes")
            if fallback_to_template:
                logger.warning(
                    "LLM generation failed for %s (%s). Falling back to template output.",
                    spec.query_id,
                    exc,
                )
                return self._offline_payload(
                    spec,
                    context_bundle,
                    ground_truth_bundle,
                    gt_cache_info,
                    list(enhanced_results),
                )
            else:
                logger.error(
                    "LLM generation failed for %s (%s). Fallback disabled; failing the query.",
                    spec.query_id,
                    exc,
                )
                raise

        payload = self._post_process(
            raw_output,
            spec,
            context_bundle,
            ground_truth_bundle,
            gt_cache_info,
            list(enhanced_results),  # Use enhanced results in final payload
        )
        return payload

    def _offline_payload(
        self,
        spec: QuerySpec,
        context: ContextBundle,
        gt_bundle: GroundTruthBundle,
        gt_cache: Optional[Dict[str, object]] = None,
        enhanced_results: Optional[Sequence[SearchResult]] = None,
    ) -> Dict:
        """Construct a deterministic, rule-based payload without calling the LLM.

        This is used when FALLBACK_TO_TEMPLATE=1 is set and network/LLM is unavailable.
        """
        # Basic fields
        orientation = spec.normalized_orientation()
        level = spec.normalized_level()
        title = gt_bundle.primary.title or spec.query_id
        scenario = spec.scenario.strip() if spec.scenario else "需要在有限时间内完成任务并通过评估。"

        # Objectives/deliverables/evaluation
        task_objectives = list(spec.task_focus) if spec.task_focus else [
            "围绕参考资料梳理关键步骤，形成符合SOP的执行计划。",
        ]
        if orientation == "inverse":
            task_objectives.append("识别前提矛盾，提交证伪流程与日志。")

        expected_outputs = list(spec.deliverable_requirements) if spec.deliverable_requirements else [
            "提交结构化主文档（背景/方法/结果/下一步），可复核。",
        ]
        if orientation == "inverse":
            expected_outputs.append("提交不可完成性证明及证据链。")

        evaluation_focus = list(spec.evaluation_focus) if spec.evaluation_focus else [
            "可验证性与可追溯性；与参考资料一致。",
        ]

        # Inputs/resources
        provided = [
            f"主参考来源：{gt_bundle.primary.title}（{gt_bundle.primary.url}）",
            "项目提供的上下文资料（见context字段）。",
        ]

        # References list from enhanced results
        refs: List[Dict[str, str]] = []
        for r in (enhanced_results or [])[:5]:
            refs.append({
                "title": r.title,
                "url": r.url,
                "snippet": r.snippet,
                "source": r.source or "web",
                "search_query": r.search_query,
            })

        payload: Dict[str, object] = {
            "query_id": spec.query_id,
            "level": level,
            "title": f"{('逆向' if orientation=='inverse' else '正向')} · {title}",
            "orientation": orientation,
            "role_and_background": scenario,
            "task_objectives": task_objectives,
            "inputs_and_resources": {
                "provided_materials": provided,
                "allowed_external_research": (
                    "优先使用‘参考资料/提供的资料’；如需补充，请限于公开可验证资料，记录来源与访问日期。"
                ),
                "reference_usage": (
                    f"关键论断需引用参考资料（如《{gt_bundle.primary.title}》），标注章节/段落/URL锚点。"
                ),
            },
            "deliverables": {
                "expected_outputs": expected_outputs,
                "format_requirements": "建议Markdown结构化呈现；表格注明来源与口径。",
                "quality_bar": "; ".join(evaluation_focus),
            },
            "grading_rubric": evaluation_focus,
            "notes": (spec.notes or "").strip(),
            "industry": spec.industry,
            "profession": spec.profession,
            "ground_truth": {
                "primary": gt_bundle.primary.to_dict(),
                "supporting": [s.to_dict() for s in gt_bundle.supporting],
                "cache": gt_cache or {},
            },
            "references": refs,
            "context": context.to_dict(),
            "tool_usage_expectation": (
                "以单一核心Agent（Call Code或Deep Research）主导，强调检索-复核-对比；"
                "禁止大规模训练，允许短时验证实验（≤2 GPU·小时）"
            ),
        }

        return self._post_process(payload, spec, context, gt_bundle, gt_cache or {}, list(enhanced_results or []))

    def _fallback_context(self, spec: QuerySpec) -> ContextBundle:
        persona = PersonaProfile(
            identifier=f"default-{spec.profession or 'general'}",
            name=f"{spec.profession or '行业专家'} · 默认Persona",
            seniority="mid",
            description=spec.scenario or "负责项目交付的核心成员，需在时间盒内完成可验证的成果。",
            motivations=["交付高质量成果", "确保审计合规"],
            pain_points=["信息不足", "跨部门配合不顺畅"],
        )
        default_constraints = [
            "匹配任务描述的交付粒度，避免开放式探索。",
            "引用的任何资料必须可验证、可追溯。",
        ]
        return ContextBundle(
            persona=persona,
            user_statement=spec.scenario or "需要在有限时间内完成任务并通过评估。",
            constraints=default_constraints,
            available_assets=[
                "提供的参考资料（对外字段仅称‘参考资料/提供的资料’）",
            ],
            success_metrics=[
                "产出需覆盖任务描述的关键要求。",
                "所有结论需可复核。",
            ],
        )

    # offline/template 生成逻辑已移除：仅支持通过 LLM 生成

    @staticmethod
    def _post_process(
        raw_output: Dict,
        spec: QuerySpec,
        context: ContextBundle,
        ground_truth_bundle: GroundTruthBundle,
        gt_cache: Optional[Dict[str, object]],
        all_results: List[SearchResult],
    ) -> Dict:
        """
        Ensure ground truth information is consistent and attach metadata.
        """

        payload = dict(raw_output)
        payload.setdefault("query_id", spec.query_id)
        payload["level"] = spec.normalized_level()
        payload["orientation"] = spec.normalized_orientation()
        payload.setdefault("context", context.to_dict())
        context_sources = payload.get("context_sources") or []
        if not context_sources and spec.context_documents:
            for doc in spec.context_documents:
                context_sources.append(
                    {
                        "name": doc.get("name"),
                        "source_url": doc.get("source"),
                        "local_path": doc.get("path"),
                        "sha256": doc.get("sha256"),
                        "content_type": doc.get("content_type"),
                        "query": doc.get("query"),
                        "snippet": doc.get("content"),
                    }
                )
        payload["context_sources"] = context_sources

        ground_truth_section = payload.get("ground_truth") or {}
        ground_truth_section.update(ground_truth_bundle.to_dict())
        # Clarify evidence usage policy:
        # - Ground Truth (primary + supporting) is the canonical basis for evaluation.
        # - References are additional, non-canonical sources that can be cited to enrich answers,
        #   but grading aligns to Ground Truth where conflicts arise.
        ground_truth_section.setdefault(
            "usage_notes",
            (
                "引用规范：优先使用Ground Truth（主+辅）作为判分依据；允许引用 references 列表中的公开资料作为补充，"
                "需标注来源/访问日期/页码（或段落）；若与Ground Truth存在冲突，以Ground Truth为准。"
            ),
        )
        payload["ground_truth"] = ground_truth_section

        if gt_cache:
            ground_truth_section.setdefault("cache", gt_cache)

        # Build references from search results, excluding any source already designated
        # as Ground Truth (primary or supporting) to avoid ambiguity and duplication.
        payload["references"] = []
        gt_urls = {ground_truth_bundle.primary.url}
        gt_urls.update(src.url for src in ground_truth_bundle.supporting)
        for result in all_results:
            if result.url in gt_urls:
                continue
            payload["references"].append(result.to_dict())
        payload["search_results"] = [result.to_dict() for result in all_results]

        payload.setdefault("standard_answer", {
            "summary": "请基于Ground Truth提炼关键论断并形成可验证的执行方案。",
            "key_points": [
                "覆盖任务目标、行动步骤与验收标准。",
                "每个关键判断引用Ground Truth并提供验证方式。",
            ],
        })
        payload.setdefault(
            "evaluation_guide",
            EvaluationGuide(
                summary="评估交付是否满足SOP 8.0（三E、训练/算力红线、安全合规与时间窗口）并与Ground Truth一致。",
                checkpoints=[
                    "任务范围、交付格式、验收标准均有明确说明（Executable）。",
                    "关键判断可触发高阶能动性，考察目标明确（Examining）。",
                    "评分标准可量化、可复核；与参考资料/基准有对齐指标（Evaluable）。",
                    "遵守训练/算力红线：training-free；禁止从头训练或长时间/昂贵算力依赖。",
                    "引用公开、中立、国际化资料；必要时脱敏；设定并遵守资料使用的时间窗口。",
                ],
                scoring_rubric=payload.get("grading_rubric", []),
            ).to_dict(),
        )

        payload["sop_version"] = "8.0"
        payload["spec_metadata"] = spec.to_metadata()

        # Remove any ground-truth primary reference from provided_materials (solver-facing).
        payload = QueryConstructionAgent._drop_primary_from_provided_materials(payload)

        # Scrub public fields to avoid exposing the literal term "Ground Truth".
        payload = QueryConstructionAgent._scrub_public_gt_terms(payload)

        # Remove内部资料引用，除非上下文明确提供。
        payload = QueryConstructionAgent._sanitize_internal_scope(payload, context)

        # Enforce SOP compliance post-processing (e.g., L4 training-free guardrails)
        payload = QueryConstructionAgent._enforce_sop_compliance(payload)

        return payload

    @staticmethod
    def _enforce_sop_compliance(payload: Dict) -> Dict:
        """Normalize payload to comply with Accurant_SOP constraints.

        Rules (initial set):
        - L4 must be training-free or clearly short, bounded experiments; avoid large-scale training.
        - Prefer wording of inference/validation/replication over training; add resource/time guardrails.
        """
        level = str(payload.get("level") or "").upper()
        if level != "L4":
            return payload

        def _sanitize_text(s: str) -> str:
            # Replace training-related terms with validation/inference wording
            rules = [
                (r"分布式训练", "分布式推理/验证"),
                (r"训练/推理", "推理/验证"),
                (r"训练日志", "推理/验证日志"),
                (r"训练吞吐", "推理吞吐"),
                (r"训练\s*性能", "推理/验证性能"),
                (r"训练PPL", "验证PPL"),
                (r"训练\s*稳定性", "验证稳定性"),
                (r"训练", "验证"),
                (r"fine-?tune|微调", "验证实验"),
                (r"大规模", "小规模可复核"),
                (r"长时间", "短时"),
            ]
            out = s
            for pat, rep in rules:
                out = re.sub(pat, rep, out, flags=re.IGNORECASE)
            return out

        # Sanitize arrays of strings under common fields
        for key in ("task_objectives",):
            arr = payload.get(key) or []
            if isinstance(arr, list):
                payload[key] = [_sanitize_text(str(x)) for x in arr]
        # deliverables.expected_outputs
        deliver = payload.get("deliverables") or {}
        if isinstance(deliver, dict):
            ex = deliver.get("expected_outputs") or []
            if isinstance(ex, list):
                deliver["expected_outputs"] = [_sanitize_text(str(x)) for x in ex]
            fmt = deliver.get("format_requirements")
            if isinstance(fmt, str):
                deliver["format_requirements"] = _sanitize_text(fmt)
            qb = deliver.get("quality_bar")
            if isinstance(qb, str):
                deliver["quality_bar"] = _sanitize_text(qb)
            payload["deliverables"] = deliver

        # grading_rubric
        rub = payload.get("grading_rubric") or []
        if isinstance(rub, list):
            payload["grading_rubric"] = [_sanitize_text(str(x)) for x in rub]

        # evaluation_guide.checkpoints
        eg = payload.get("evaluation_guide") or {}
        if isinstance(eg, dict):
            cps = eg.get("checkpoints") or []
            if isinstance(cps, list):
                eg["checkpoints"] = [_sanitize_text(str(x)) for x in cps]
            sr = eg.get("scoring_rubric") or []
            if isinstance(sr, list):
                eg["scoring_rubric"] = [_sanitize_text(str(x)) for x in sr]
            payload["evaluation_guide"] = eg

        # standard_answer.key_points
        sa = payload.get("standard_answer") or {}
        if isinstance(sa, dict):
            kp = sa.get("key_points") or []
            if isinstance(kp, list):
                sa["key_points"] = [_sanitize_text(str(x)) for x in kp]
            summ = sa.get("summary")
            if isinstance(summ, str):
                sa["summary"] = _sanitize_text(summ)
            payload["standard_answer"] = sa

        # tool_usage_expectation & notes: add explicit guardrails
        tue = payload.get("tool_usage_expectation")
        if isinstance(tue, str):
            payload["tool_usage_expectation"] = (
                "以单一核心Agent（Call Code或Deep Research）主导，强调检索-复核-对比；"
                "禁止大规模训练，允许短时验证实验（≤2 GPU·小时）"
            )
        notes = payload.get("notes") or ""
        notes_add = (
            " 资源与时间护栏：≤1周完成；仅使用公开可获取或合成数据；"
            "禁止长时间/大规模训练；如需运行实验，仅限短时验证（≤2 GPU·小时）。"
        )
        payload["notes"] = (str(notes).strip() + " " + notes_add).strip()

        return payload

    @staticmethod
    def _drop_primary_from_provided_materials(payload: Dict) -> Dict:
        """Ensure inputs_and_resources.provided_materials does NOT include ground_truth.primary.

        - Filter out entries that contain the primary URL or share the same host, or contain the primary title.
        - If the list becomes empty, backfill with top-N entries from `references` that are not the primary host.
        """
        try:
            from urllib.parse import urlparse
            import re as _re
        except Exception:
            return payload

        gt = payload.get("ground_truth") or {}
        primary = (gt.get("primary") or {})
        primary_url = (primary.get("url") or "").strip()
        primary_title = (primary.get("title") or "").strip()

        if not primary_url:
            return payload

        p_host = urlparse(primary_url).netloc.lower() if primary_url else ""
        inres = payload.get("inputs_and_resources") or {}
        items = list(inres.get("provided_materials") or [])
        kept = []
        for s in items:
            text = str(s or "")
            urls = _re.findall(r"https?://[^\s)\]\"]+", text)
            hosts = {urlparse(u).netloc.lower() for u in urls}
            # drop if any url equals primary or host equals primary host, or title contained
            if any(u.strip() == primary_url for u in urls) or (p_host and p_host in hosts) or (
                primary_title and (primary_title in text)
            ):
                continue
            kept.append(s)

        # backfill if empty using references (non-primary host)
        if not kept:
            refs = payload.get("references") or []
            for ref in refs[:3]:
                if not isinstance(ref, dict):
                    continue
                u = (ref.get("url") or "").strip()
                if not u:
                    continue
                h = urlparse(u).netloc.lower()
                if u == primary_url or (p_host and h == p_host):
                    continue
                title = (ref.get("title") or u).strip()
                kept.append(f"{title}: {u}")
                if len(kept) >= 3:
                    break

        inres["provided_materials"] = kept
        payload["inputs_and_resources"] = inres
        return payload

    @staticmethod
    def _context_supports_internal_assets(context: ContextBundle) -> bool:
        """
        Decide whether任务上下文显式允许使用“内部资料/数据”。
        """
        keywords = ("内部", "internal", "机密", "confidential")
        fields: List[str] = []
        fields.extend(context.constraints or [])
        fields.extend(context.available_assets or [])
        fields.extend(context.success_metrics or [])
        fields.append(context.user_statement or "")
        persona = context.persona
        fields.append(persona.description or "")

        for text in fields:
            if not text:
                continue
            if any(keyword in text for keyword in keywords):
                return True
        return False

    @staticmethod
    def _sanitize_internal_scope(payload: Dict, context: ContextBundle) -> Dict:
        """
        Remove/replace对“内部资料”类资源的默认要求，除非上下文明确提供。
        """
        if QueryConstructionAgent._context_supports_internal_assets(context):
            return payload

        patterns = [
            (r"(公司)?内部资料", "提供的公开资料"),
            (r"(公司)?内部数据", "提供的公开数据"),
            (r"(公司)?内部文档", "提供的参考资料"),
            (r"(公司)?内部报告", "公开报告"),
            (r"(公司)?内部系统", "授权的公开系统"),
            (r"内部流程文档", "提供的流程资料"),
            (r"内部流程", "公开可验证流程"),
        ]

        def _replace_text(value: object) -> object:
            if not isinstance(value, str):
                return value
            result = value
            for pattern, replacement in patterns:
                result = re.sub(pattern, replacement, result)
            return result

        def _sanitize_list(items: object) -> object:
            if not isinstance(items, list):
                return items
            return [_replace_text(item) for item in items]

        # Top-level fields
        for key in ("role_and_background", "notes", "tool_usage_expectation", "estimated_human_time"):
            if key in payload:
                payload[key] = _replace_text(payload[key])

        # Task, grading, etc.
        for key in ("task_objectives", "grading_rubric"):
            if key in payload:
                payload[key] = _sanitize_list(payload[key])

        # Deliverables
        deliver = payload.get("deliverables") or {}
        if isinstance(deliver, dict):
            for subkey in ("expected_outputs",):
                if subkey in deliver:
                    deliver[subkey] = _sanitize_list(deliver[subkey])
            for subkey in ("format_requirements", "quality_bar"):
                if subkey in deliver:
                    deliver[subkey] = _replace_text(deliver[subkey])
            payload["deliverables"] = deliver

        # Inputs/resources
        inres = payload.get("inputs_and_resources") or {}
        if isinstance(inres, dict):
            if "provided_materials" in inres:
                inres["provided_materials"] = _sanitize_list(inres["provided_materials"])
            for subkey in ("allowed_external_research", "reference_usage", "ground_truth_usage"):
                if subkey in inres:
                    inres[subkey] = _replace_text(inres[subkey])
            clause = "不得假设额外的公司内部资料，除非已在“提供的资料”中明确列出。"
            existing = inres.get("allowed_external_research")
            if isinstance(existing, str):
                if clause not in existing:
                    separator = "" if existing.endswith(("。", ".", "；", ";")) else " "
                    inres["allowed_external_research"] = f"{existing}{separator}{clause}"
            else:
                inres["allowed_external_research"] = clause
            payload["inputs_and_resources"] = inres

        # Standard answer
        standard_answer = payload.get("standard_answer") or {}
        if isinstance(standard_answer, dict):
            for subkey in ("summary",):
                if subkey in standard_answer:
                    standard_answer[subkey] = _replace_text(standard_answer[subkey])
            if "key_points" in standard_answer:
                standard_answer["key_points"] = _sanitize_list(standard_answer["key_points"])
            payload["standard_answer"] = standard_answer

        # Evaluation guide
        evaluation = payload.get("evaluation_guide") or {}
        if isinstance(evaluation, dict):
            for subkey in ("summary",):
                if subkey in evaluation:
                    evaluation[subkey] = _replace_text(evaluation[subkey])
            for subkey in ("checkpoints", "scoring_rubric"):
                if subkey in evaluation:
                    evaluation[subkey] = _sanitize_list(evaluation[subkey])
            payload["evaluation_guide"] = evaluation

        return payload

    @staticmethod
    def _scrub_public_gt_terms(payload: Dict) -> Dict:
        """Remove/replace the literal term 'Ground Truth' from outward-facing fields.

        This keeps the judge-only `ground_truth` object intact, while ensuring solver-facing
        text uses '参考资料/提供的资料' instead of 'Ground Truth'.
        """
        def _replace(s: str) -> str:
            # Replace anywhere, do not rely on word boundaries (to handle CJK adjacency).
            return re.sub(r"Ground\s*Truth", "参考资料", s, flags=re.IGNORECASE)

        # Top-level string fields
        for key in ("title", "role_and_background", "tool_usage_expectation", "estimated_human_time", "notes"):
            val = payload.get(key)
            if isinstance(val, str):
                payload[key] = _replace(val)

        # Arrays of strings
        for key in ("task_objectives", "grading_rubric"):
            arr = payload.get(key)
            if isinstance(arr, list):
                payload[key] = [_replace(str(x)) for x in arr]

        # Deliverables block
        deliver = payload.get("deliverables") or {}
        if isinstance(deliver, dict):
            ex = deliver.get("expected_outputs")
            if isinstance(ex, list):
                deliver["expected_outputs"] = [_replace(str(x)) for x in ex]
            for k in ("format_requirements", "quality_bar"):
                v = deliver.get(k)
                if isinstance(v, str):
                    deliver[k] = _replace(v)
            payload["deliverables"] = deliver

        # Inputs/resources
        inres = payload.get("inputs_and_resources") or {}
        if isinstance(inres, dict):
            pm = inres.get("provided_materials")
            if isinstance(pm, list):
                inres["provided_materials"] = [_replace(str(x)) for x in pm]
            for k in ("allowed_external_research", "ground_truth_usage", "reference_usage"):
                v = inres.get(k)
                if isinstance(v, str):
                    inres[k] = _replace(v)
            payload["inputs_and_resources"] = inres

        # Standard answer summary/key points
        sa = payload.get("standard_answer") or {}
        if isinstance(sa, dict):
            summ = sa.get("summary")
            if isinstance(summ, str):
                sa["summary"] = _replace(summ)
            kps = sa.get("key_points")
            if isinstance(kps, list):
                sa["key_points"] = [_replace(str(x)) for x in kps]
            payload["standard_answer"] = sa

        # Evaluation guide (sanitize regardless of level)
        eg = payload.get("evaluation_guide") or {}
        if isinstance(eg, dict):
            s = eg.get("summary")
            if isinstance(s, str):
                eg["summary"] = _replace(s)
            for k in ("checkpoints", "scoring_rubric"):
                arr = eg.get(k)
                if isinstance(arr, list):
                    eg[k] = [_replace(str(x)) for x in arr]
            payload["evaluation_guide"] = eg

        # Context success metrics/constraints/assets
        ctx = payload.get("context") or {}
        if isinstance(ctx, dict):
            # Persona fields
            person = ctx.get("persona") or {}
            if isinstance(person, dict):
                for subk in ("name", "description"):
                    v = person.get(subk)
                    if isinstance(v, str):
                        person[subk] = _replace(v)
                ctx["persona"] = person
            # User statement
            us = ctx.get("user_statement")
            if isinstance(us, str):
                ctx["user_statement"] = _replace(us)
            for sub in ("constraints", "available_assets", "success_metrics"):
                arr = ctx.get(sub)
                if isinstance(arr, list):
                    ctx[sub] = [_replace(str(x)) for x in arr]
            payload["context"] = ctx

        return payload


def generate_batch(
    agent: QueryConstructionAgent,
    specs: Sequence[QuerySpec],
    *,
    package_dir: Optional[Path] = None,
    package_include_references: bool = True,
    package_reference_limit: int = 3,
    package_download_ground_truth: bool = True,
    package_split_views: bool = False,
    max_workers: Optional[int] = None,
) -> List[Dict]:
    """
    Helper to generate a batch of queries.

    Parameters
    ----------
    max_workers:
        Maximum number of worker threads used to process specs. When None,
        falls back to QUERY_AGENT_MAX_WORKERS or 1.
    """
    env_workers = os.environ.get("QUERY_AGENT_MAX_WORKERS")
    resolved_workers = max_workers
    if resolved_workers is None and env_workers:
        try:
            resolved_workers = int(env_workers)
        except ValueError:
            logger.warning(
                "Ignoring invalid QUERY_AGENT_MAX_WORKERS value: %s",
                env_workers,
            )
    if resolved_workers is None:
        resolved_workers = 1
    resolved_workers = max(1, resolved_workers)

    search_cache: Dict[tuple, List[SearchResult]] = {}
    cache_lock = threading.Lock()

    def _process_spec(spec: QuerySpec) -> Optional[Dict]:
        logger.info(
            "Generating query: %s (level=%s, orientation=%s)",
            spec.query_id,
            spec.level,
            spec.orientation,
        )
        cache_key = (
            tuple(spec.search_queries),
            spec.language.lower(),
            agent.market,
            agent.serper_endpoint,
        )
        with cache_lock:
            search_results = search_cache.get(cache_key)

        if search_results is None:
            try:
                search_results = agent.run_search(spec)
            except SearchError as exc:
                logger.warning(
                    "Search failed for query %s (%s), skipping to next query: %s",
                    spec.query_id,
                    spec.search_query,
                    exc,
                )
                return None
            with cache_lock:
                search_cache[cache_key] = search_results

        payload: Optional[Dict] = None
        last_error: Optional[Exception] = None
        for attempt in range(3):
            try:
                payload = agent.build_query(spec, search_results=search_results)
                break
            except (SearchError, LLMError) as exc:
                last_error = exc
                logger.warning(
                    "Attempt %d failed for %s due to %s",
                    attempt + 1,
                    spec.query_id,
                    exc,
                )
        if payload is None:
            logger.error(
                "Failed to generate query %s after %d attempts due to %s; skipping (offline fallback removed).",
                spec.query_id,
                3,
                last_error,
            )
            return None

        if package_dir:
            pkg_path = save_query_package(
                payload,
                package_dir,
                include_references=package_include_references,
                reference_limit=package_reference_limit,
                download_ground_truth=package_download_ground_truth,
                split_views=package_split_views,
            )
            payload["_package_dir"] = str(pkg_path.resolve())
        return payload

    if resolved_workers == 1 or len(specs) <= 1:
        outputs: List[Dict] = []
        for spec in specs:
            payload = _process_spec(spec)
            if payload:
                outputs.append(payload)
        return outputs

    results: List[Optional[Dict]] = [None] * len(specs)
    with ThreadPoolExecutor(max_workers=resolved_workers) as executor:
        future_to_index = {
            executor.submit(_process_spec, spec): idx for idx, spec in enumerate(specs)
        }
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            spec = specs[idx]
            try:
                payload = future.result()
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "Unhandled error while generating query %s: %s",
                    spec.query_id,
                    exc,
                )
                continue
            if payload:
                results[idx] = payload

    return [payload for payload in results if payload is not None]
