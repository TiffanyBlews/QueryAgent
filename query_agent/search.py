"""
Search utilities built on top of the Serper.dev API.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

import requests
from html import unescape
import re
from urllib.parse import quote

SKIP_EXTENSIONS = {
    ".ico",
    ".png",
    ".jpg",
    ".jpeg",
    ".svg",
    ".gif",
    ".webp",
    ".css",
    ".js",
    ".ttf",
    ".woff",
    ".txt",
}


def _should_skip_url(url: str) -> bool:
    lowered = url.lower()
    if "duckduckgo.com" in lowered or "r.jina.ai" in lowered:
        return True
    for ext in SKIP_EXTENSIONS:
        if lowered.endswith(ext):
            return True
    return False


class SearchError(RuntimeError):
    """Raised when the web search request fails."""


@dataclass
class SearchResult:
    """Normalized representation of a single search hit."""

    title: str
    url: str
    snippet: str
    source: Optional[str] = None
    date: Optional[str] = None
    search_query: Optional[str] = None

    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "title": self.title,
            "url": self.url,
            "snippet": self.snippet,
            "source": self.source,
            "date": self.date,
            "search_query": self.search_query,
        }


LOCAL_DOC_BASE = "/workspace/Qiji_benchmark/Value_Bench_Final/SOP_Query_Agent/ground_truth_sources"
# Local overrides are disabled by default because they point to environment-specific file:// paths
# that may not exist on the current machine, causing packaging to miss primary artifacts.
# Set ENABLE_LOCAL_OVERRIDES=1 to turn them on intentionally.
LOCAL_SEARCH_OVERRIDES_RAW: Dict[str, List[Dict[str, Optional[str]]]] = {
    "WHO ethics governance artificial intelligence health checklist 2024 pdf": [
        {
            "title": "WHO Ethics and Governance of Artificial Intelligence for Health (2023)",
            "url": f"file://{LOCAL_DOC_BASE}/who_ai_guidance.md",
            "snippet": "WHO 2023 指南提出上线前自检清单与多维治理要求，覆盖数据、模型、流程、伦理管控。",
            "source": "local-ground-truth",
        }
    ],
    "WHO ai ethics governance realtime autonomous diagnosis ban pdf": [
        {
            "title": "WHO Ethics and Governance of AI for Health — Human Oversight Requirements",
            "url": f"file://{LOCAL_DOC_BASE}/who_ai_guidance.md",
            "snippet": "指南明确AI诊断需保留医生监督与伦理审查，禁止完全自动出具诊断结果。",
            "source": "local-ground-truth",
        }
    ],
    "WHO hospital responsible ai governance implementation roadmap 2024 pdf": [
        {
            "title": "WHO Responsible AI Governance Implementation Guidance (Hospital Context)",
            "url": f"file://{LOCAL_DOC_BASE}/who_ai_guidance.md",
            "snippet": "文件给出跨部门治理框架、职责矩阵与审计要求，适用于大型医院体系。",
            "source": "local-ground-truth",
        }
    ],
    "WHO responsible ai clinical audit toolkit 2024 pdf": [
        {
            "title": "WHO Clinical Audit Checklist for Responsible AI",
            "url": f"file://{LOCAL_DOC_BASE}/who_ai_guidance.md",
            "snippet": "指南中的临床审计章节强调抽样验证、偏差分析、整改跟踪与证据留存。",
            "source": "local-ground-truth",
        }
    ],
    "WHO responsible ai health system transformation partnership framework 2024 pdf": [
        {
            "title": "WHO Responsible AI Strategic Roadmap for Health Systems",
            "url": f"file://{LOCAL_DOC_BASE}/who_ai_guidance.md",
            "snippet": "WHO 建议通过三阶段战略路线图与生态合作推进负责任AI落地。",
            "source": "local-ground-truth",
        }
    ],
    "IPCC AR6 urban climate risk summary for policymakers pdf": [
        {
            "title": "IPCC AR6 Synthesis Report — Urban Risk Highlights",
            "url": f"file://{LOCAL_DOC_BASE}/ipcc_ar6_cities.md",
            "snippet": "AR6 综合报告列出了热浪、暴雨、海平面上升对沿海大城市的复合风险。",
            "source": "local-ground-truth",
        }
    ],
    "IPCC AR6 urban adaptation abolish heatwave shelters impossible": [
        {
            "title": "IPCC AR6: Urban Adaptation Requires Heatwave Shelters",
            "url": f"file://{LOCAL_DOC_BASE}/ipcc_ar6_cities.md",
            "snippet": "报告强调保护脆弱人群与避暑中心的重要性，反驳取消避暑设施的做法。",
            "source": "local-ground-truth",
        }
    ],
    "IPCC AR6 urban adaptation pathways implementation pdf": [
        {
            "title": "IPCC AR6 WGII Chapter 6 — Urban Adaptation Pathways",
            "url": f"file://{LOCAL_DOC_BASE}/ipcc_ar6_cities.md",
            "snippet": "章节提供城市适应路径、政策工具、阶段性目标与跨部门协同建议。",
            "source": "local-ground-truth",
        }
    ],
    "city climate adaptation finance toolkit ipcc 2024 pdf": [
        {
            "title": "IPCC Guidance on Climate Adaptation Finance",
            "url": f"file://{LOCAL_DOC_BASE}/ipcc_ar6_cities.md",
            "snippet": "IPCC 建议组合公共预算、国际资金、私营投资，并建立绩效与透明披露机制。",
            "source": "local-ground-truth",
        }
    ],
    "IPCC climate resilient development pathways urban investment blueprint 2024 pdf": [
        {
            "title": "Climate-Resilient Development Pathways (CRDP) Overview",
            "url": f"file://{LOCAL_DOC_BASE}/ipcc_ar6_cities.md",
            "snippet": "CRDP 强调减缓与适应协同、分阶段投资、治理结构与监测体系。",
            "source": "local-ground-truth",
        }
    ],
    "NIST AI risk management framework risk register template 2024 pdf": [
        {
            "title": "NIST AI RMF 1.0 — Risk Register Guidance",
            "url": f"file://{LOCAL_DOC_BASE}/nist_ai_rmf.md",
            "snippet": "Framework 要求记录风险描述、影响、控制、缺口、缓解计划与证据。",
            "source": "local-ground-truth",
        }
    ],
    "NIST AI RMF waive governance controls impossible": [
        {
            "title": "NIST AI RMF Governance Controls Are Mandatory",
            "url": f"file://{LOCAL_DOC_BASE}/nist_ai_rmf.md",
            "snippet": "RMF 明确要求日志、监控、问责机制，禁止放弃治理控制点。",
            "source": "local-ground-truth",
        }
    ],
    "NIST AI RMF enterprise rollout playbook 2024 pdf": [
        {
            "title": "NIST AI RMF Playbook for Pilot-to-Production",
            "url": f"file://{LOCAL_DOC_BASE}/nist_ai_rmf.md",
            "snippet": "RMF 强调跨职能治理、生命周期管理、指标库与培训体系。",
            "source": "local-ground-truth",
        }
    ],
    "城市应急协调官 跨部门协同 coordination internal_ops cross_function_sync 最佳实践 标准流程 case study 2024": [
        {
            "title": "National Incident Management System (2017) — FEMA",
            "url": "https://www.fema.gov/sites/default/files/documents/fema_nims_doctrine_2017.pdf",
            "snippet": "FEMA 的 NIMS 指南详细说明跨机构协调结构、指挥与协同流程、资源管理与信息流，适用于应急响应组织的例会与任务分配。",
            "source": "fema.gov",
            "date": "2017-10-01",
        }
    ],
    "ISO 42001 ai management third party risk checklist pdf": [
        {
            "title": "ISO/IEC 42001:2023 Third-Party Control Requirements",
            "url": f"file://{LOCAL_DOC_BASE}/iso_iec_42001.md",
            "snippet": "AIMS 要求识别并监控外部提供方，定义准入、合同与审计流程。",
            "source": "local-ground-truth",
        }
    ],
    "global ai governance innovation strategy roadmap 2025 pdf": [
        {
            "title": "Global AI Governance and Innovation Roadmap",
            "url": f"file://{LOCAL_DOC_BASE}/global_ai_governance.md",
            "snippet": "WEF 与OECD 报告总结全球监管趋势及Responsible-by-design路线。",
            "source": "local-ground-truth",
        }
    ],
    "TAM SAM SOM bottom up market sizing template 2025 pdf": [
        {
            "title": "TAM/SAM/SOM Market Sizing Template",
            "url": f"file://{LOCAL_DOC_BASE}/tam_sam_som_template.md",
            "snippet": "模板展示Top-down与Bottom-up测算步骤、字段定义与敏感性分析。",
            "source": "local-ground-truth",
        }
    ],
    "market sizing template zero data impossible": [
        {
            "title": "Market Sizing Requires Verifiable Data Inputs",
            "url": f"file://{LOCAL_DOC_BASE}/tam_sam_som_template.md",
            "snippet": "文档说明若缺乏数据或调研输入，无法构建可信的市场规模模型。",
            "source": "local-ground-truth",
        }
    ],
    "competitive landscape case study go to market strategy template 2025 pdf": [
        {
            "title": "B2B Go-to-Market Strategy Casebook",
            "url": f"file://{LOCAL_DOC_BASE}/gtm_strategy.md",
            "snippet": "Deloitte 案例总结ICP、渠道矩阵、指标体系与风险管理。",
            "source": "local-ground-truth",
        }
    ],
    "customer journey mapping enterprise saas benchmark 2025 pdf": [
        {
            "title": "Enterprise SaaS Customer Journey Benchmarks",
            "url": f"file://{LOCAL_DOC_BASE}/saas_customer_journey.md",
            "snippet": "SaaS 客户旅程与付费动因基准数据、指标与调研框架。",
            "source": "local-ground-truth",
        }
    ],
    "continuous market intelligence program operating model 2025 pdf": [
        {
            "title": "Continuous Market Intelligence Operating Model",
            "url": f"file://{LOCAL_DOC_BASE}/market_intelligence_ops.md",
            "snippet": "Gartner 提出的采集-验证-分析-分发-复盘循环与治理机制。",
            "source": "local-ground-truth",
        }
    ],
    "seed stage post investment checklist venture capital 2024 pdf": [
        {
            "title": "Seed-Stage Post-Investment Checklist",
            "url": f"file://{LOCAL_DOC_BASE}/vc_post_investment.md",
            "snippet": "NVCA/ACA 指南涵盖治理、财务、产品、市场、风险监控任务。",
            "source": "local-ground-truth",
        }
    ],
    "venture capital ic memo zero diligence impossible": [
        {
            "title": "IC Memo Requires Comprehensive Due Diligence",
            "url": f"file://{LOCAL_DOC_BASE}/vc_post_investment.md",
            "snippet": "行业标准明确IC材料须引用可验证数据与风险披露，无法在零尽调下完成。",
            "source": "local-ground-truth",
        }
    ],
    "deal sourcing playbook venture capital automation 2024 pdf": [
        {
            "title": "Deal Sourcing Playbook with Data & Automation",
            "url": f"file://{LOCAL_DOC_BASE}/deal_sourcing_playbook.md",
            "snippet": "Bain/EY 报告总结多渠道获源、线索评分、指标与合规要求。",
            "source": "local-ground-truth",
        }
    ],
    "lp reporting portal upgrade playbook 2025 pdf": [
        {
            "title": "Digital LP Reporting Portal Upgrade Guide",
            "url": f"file://{LOCAL_DOC_BASE}/lp_reporting_portal.md",
            "snippet": "ILPA 与Deloitte 建议门户功能、数据治理、KPI 与实施路线。",
            "source": "local-ground-truth",
        }
    ],
    "venture capital portfolio exit program blueprint 2025 pdf": [
        {
            "title": "Portfolio Exit Program Blueprint",
            "url": f"file://{LOCAL_DOC_BASE}/exit_program.md",
            "snippet": "Bain/Deloitte 归纳退出路径规划、买家数据库、价值提升策略。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt workflow universal playbook 2025 pdf": [
        {
            "title": "ChatGPT 通用岗位协作流程",
            "url": f"file://{LOCAL_DOC_BASE}/chatgpt_any_role_playbook.md",
            "snippet": "涵盖任务拆解、提示结构、质量审查、度量指标的通用人机协作框架。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt sales enablement generative ai playbook 2025 pdf": [
        {
            "title": "ChatGPT 销售赋能手册",
            "url": f"file://{LOCAL_DOC_BASE}/chatgpt_sales_playbook.md",
            "snippet": "覆盖线索资格、会议准备、邮件个性化与合规审批的销售流程蓝图。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt customer success ai handbook 2025 pdf": [
        {
            "title": "ChatGPT 客户成功运营手册",
            "url": f"file://{LOCAL_DOC_BASE}/chatgpt_customer_success_playbook.md",
            "snippet": "提供客户生命周期管理、健康度监测、QBR 准备的AI工作流要点。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt product management discovery prompt 2025 pdf": [
        {
            "title": "ChatGPT 产品团队工作流指南",
            "url": f"file://{LOCAL_DOC_BASE}/chatgpt_product_playbook.md",
            "snippet": "讲解Discovery/Delivery双轨流程与PRD、实验计划的提示模板。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt engineering pair programming runbook 2025 pdf": [
        {
            "title": "ChatGPT 工程团队协作指引",
            "url": f"file://{LOCAL_DOC_BASE}/chatgpt_engineering_playbook.md",
            "snippet": "涵盖AI辅助编码、测试、审查、运维的安全与质量规范。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt hr compliance ai playbook 2025 pdf": [
        {
            "title": "ChatGPT 人力资源应用指南",
            "url": f"file://{LOCAL_DOC_BASE}/chatgpt_hr_playbook.md",
            "snippet": "覆盖招聘、入职、绩效、培训、员工关系的合规化流程。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt it service management ai playbook 2025 pdf": [
        {
            "title": "ChatGPT IT 运维与服务管理手册",
            "url": f"file://{LOCAL_DOC_BASE}/chatgpt_it_playbook.md",
            "snippet": "总结事件、请求、变更、知识库、自助门户的AI赋能实践。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt manager coaching prompts 2025 pdf": [
        {
            "title": "ChatGPT 团队管理者赋能指南",
            "url": f"file://{LOCAL_DOC_BASE}/chatgpt_manager_playbook.md",
            "snippet": "提供OKR拆解、1:1议程、反馈脚本、冲突调解的标准提示框架。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt executive governance strategy 2025 pdf": [
        {
            "title": "ChatGPT 高管战略与治理应用手册",
            "url": f"file://{LOCAL_DOC_BASE}/chatgpt_executive_playbook.md",
            "snippet": "聚焦战略备选、董事会材料、风险登记、利益相关者沟通等场景。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt industry healthcare playbook 2025 pdf": [
        {
            "title": "Healthcare & Life Sciences AI 协同手册",
            "url": f"file://{LOCAL_DOC_BASE}/industry_healthcare_playbook.md",
            "snippet": "涵盖临床安全、隐私保护、运营调度、研究治理等医疗场景指引。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt industry financial services playbook 2025 pdf": [
        {
            "title": "Financial Services AI 协同手册",
            "url": f"file://{LOCAL_DOC_BASE}/industry_financial_services_playbook.md",
            "snippet": "聚焦三道防线、合规审批、模型风险、客户体验等金融治理要点。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt industry manufacturing playbook 2025 pdf": [
        {
            "title": "Manufacturing & Supply Chain AI 协同手册",
            "url": f"file://{LOCAL_DOC_BASE}/industry_manufacturing_playbook.md",
            "snippet": "覆盖工艺规划、预测维护、供应链计划、质量体系与安全要求。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt industry retail playbook 2025 pdf": [
        {
            "title": "Retail & E-commerce AI 协同手册",
            "url": f"file://{LOCAL_DOC_BASE}/industry_retail_playbook.md",
            "snippet": "介绍个性化营销、库存优化、客服合规与品牌治理的最佳实践。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt industry technology playbook 2025 pdf": [
        {
            "title": "Technology & Software AI 协同手册",
            "url": f"file://{LOCAL_DOC_BASE}/industry_technology_playbook.md",
            "snippet": "涉及研发、工程、产品、支持等工作流的AI治理与卓越中心策略。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt industry energy playbook 2025 pdf": [
        {
            "title": "Energy & Utilities AI 协同手册",
            "url": f"file://{LOCAL_DOC_BASE}/industry_energy_playbook.md",
            "snippet": "涵盖电网调度、预测维护、排放管理与关键基础设施安全。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt industry education playbook 2025 pdf": [
        {
            "title": "Education & EdTech AI 协同手册",
            "url": f"file://{LOCAL_DOC_BASE}/industry_education_playbook.md",
            "snippet": "强调学生数据保护、课程设计、学术诚信与教师发展机制。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt industry government playbook 2025 pdf": [
        {
            "title": "Government & Public Services AI 协同手册",
            "url": f"file://{LOCAL_DOC_BASE}/industry_government_playbook.md",
            "snippet": "突出公共服务透明度、责任制、偏差审查与公众沟通渠道。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt industry logistics playbook 2025 pdf": [
        {
            "title": "Logistics & Transportation AI 协同手册",
            "url": f"file://{LOCAL_DOC_BASE}/industry_logistics_playbook.md",
            "snippet": "聚焦运输调度、仓储运营、合规文档与控制塔协同模型。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt industry media playbook 2025 pdf": [
        {
            "title": "Media & Entertainment AI 协同手册",
            "url": f"file://{LOCAL_DOC_BASE}/industry_media_playbook.md",
            "snippet": "说明版权、品牌安全、内容审核与舆情监测等媒体治理要点。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt industry hospitality playbook 2025 pdf": [
        {
            "title": "Hospitality & Travel AI 协同手册",
            "url": f"file://{LOCAL_DOC_BASE}/industry_hospitality_playbook.md",
            "snippet": "涉及收益管理、客户体验、服务标准与安全合规控制。",
            "source": "local-ground-truth",
        }
    ],
    "chatgpt industry agriculture playbook 2025 pdf": [
        {
            "title": "Agriculture & Food Systems AI 协同手册",
            "url": f"file://{LOCAL_DOC_BASE}/industry_agriculture_playbook.md",
            "snippet": "涵盖作物预测、可持续农业、供应链追溯与食品安全管理。",
            "source": "local-ground-truth",
        }
    ],
}


def google_cse_search(
    query: str,
    *,
    api_key: Optional[str] = None,
    search_engine_id: Optional[str] = None,
    num: int = 5,
    language: str = "zh",
    timeout: float = 10.0,
) -> List[SearchResult]:
    """
    Execute a web search using Google Custom Search Engine API.
    """
    key = api_key or os.environ.get("GOOGLE_API_KEY")
    engine_id = search_engine_id or os.environ.get("SEARCH_ENGINE_ID")

    if not key or not engine_id:
        raise SearchError("Google CSE API key or Search Engine ID not configured")

    endpoint = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": key,
        "cx": engine_id,
        "q": query,
        "num": min(num, 10),  # Google CSE max is 10
        "lr": f"lang_{'zh' if language.lower().startswith('zh') else 'en'}"
    }

    start = time.time()
    try:
        response = requests.get(endpoint, params=params, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise SearchError(f"Google CSE search failed: {exc}") from exc

    duration = time.time() - start
    data = response.json()

    items = data.get("items", [])
    results: List[SearchResult] = []

    for item in items:
        if not item:
            continue
        url = item.get("link", "")
        if _should_skip_url(url):
            continue

        results.append(
            SearchResult(
                title=item.get("title", ""),
                url=url,
                snippet=item.get("snippet", ""),
                source="google-cse",
                date=None,
                search_query=query,
            )
        )

    if not results:
        raise SearchError(
            f"Google CSE search returned no usable results for '{query}' "
            f"(elapsed {duration:.2f}s)."
        )

    return results


def serper_search(
    query: str,
    *,
    api_key: Optional[str] = None,
    endpoint: str = "https://google.serper.dev/search",
    num: int = 5,
    market: str = "us",
    language: str = "zh",
    timeout: float = 10.0,
) -> List[SearchResult]:
    """
    Execute a web search using the Serper.dev API and return normalized results.
    """

    # Optional local overrides (disabled by default). Enable with ENABLE_LOCAL_OVERRIDES=1.
    if os.environ.get("ENABLE_LOCAL_OVERRIDES") == "1" and query in LOCAL_SEARCH_OVERRIDES_RAW:
        overrides: List[SearchResult] = []
        for item in LOCAL_SEARCH_OVERRIDES_RAW[query]:
            overrides.append(
                SearchResult(
                    title=item.get("title") or query,
                    url=item.get("url") or "",
                    snippet=item.get("snippet") or "",
                    source=item.get("source"),
                    date=item.get("date"),
                    search_query=query,
                )
            )
        return overrides[:num]

    key = api_key or os.environ.get("SERPER_API_KEY")
    use_fallback = False
    if not key:
        use_fallback = True

    if not use_fallback:
        headers = {"X-API-KEY": key, "Content-Type": "application/json"}
        payload: Dict[str, object] = {
            "q": query,
            "gl": market,
            "hl": language,
            "num": num,
        }

        start = time.time()
        try:
            response = requests.post(endpoint, headers=headers, json=payload, timeout=timeout)
            response.raise_for_status()
        except requests.RequestException as exc:  # noqa: BLE001
            status = getattr(exc.response, "status_code", None)
            if status == 403:
                use_fallback = True
            else:
                raise SearchError(f"Serper search failed: {exc}") from exc
        else:
            duration = time.time() - start
            data = response.json()

            organic: List[Dict[str, Optional[str]]] = data.get("organic") or []
            news: List[Dict[str, Optional[str]]] = data.get("news") or []
            items = organic[:num] if organic else news[:num]

            results: List[SearchResult] = []
            for item in items:
                if not item:
                    continue
                results.append(
                    SearchResult(
                        title=item.get("title") or "",
                        url=item.get("link") or "",
                        snippet=item.get("snippet") or "",
                        source=item.get("source"),
                        date=item.get("date"),
                        search_query=query,
                    )
                )

            if results:
                return results

            raise SearchError(
                f"Serper search returned no usable results for '{query}' "
                f"(elapsed {duration:.2f}s)."
            )

    # Try Google CSE as second fallback
    try:
        return google_cse_search(query, num=num, language=language, timeout=timeout)
    except SearchError:
        # Fall back to DuckDuckGo as final option
        pass

    return duckduckgo_search(query, num=num, language=language, timeout=timeout)


def duckduckgo_search(query: str, *, num: int = 5, language: str = "zh", timeout: float = 10.0) -> List[SearchResult]:
    """
    Fallback search using DuckDuckGo via r.jina.ai proxy.
    """

    kl = "cn-zh" if language.lower().startswith("zh") else "us-en"
    encoded_query = quote(query)
    proxied_url = f"https://r.jina.ai/https://duckduckgo.com/?q={encoded_query}&kl={kl}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/119.0 Safari/537.36"
        )
    }

    response = None
    for attempt in range(4):
        try:
            response = requests.get(proxied_url, headers=headers, timeout=timeout * (attempt + 1))
            response.raise_for_status()
            break
        except requests.RequestException as exc:  # noqa: BLE001
            status = getattr(exc.response, "status_code", None)
            if status == 429:
                time.sleep(3 * (attempt + 1))
                continue
            if attempt == 3:
                raise SearchError(f"DuckDuckGo search failed: {exc}") from exc
            time.sleep(1.5 * (attempt + 1))

    text = response.text
    blocks = re.split(r"\n\d+\.\s+", text)
    results: List[SearchResult] = []
    seen_urls = set()

    for block in blocks[1:]:
        lines = [line.rstrip() for line in block.splitlines() if line.strip()]
        if not lines:
            continue
        first_line = lines[0]
        snippet = re.sub(r"\s+", " ", first_line).strip()

        title = None
        link = None
        for line in lines[1:]:
            match = re.search(r"\*\s+\[(?:#+\s*)?(.*?)\]\((https?://[^\)]+)\)", line)
            if not match:
                continue
            candidate_title, candidate_url = match.groups()
            if "duckduckgo.com" in candidate_url or "external-content.duckduckgo.com" in candidate_url:
                continue
            title = unescape(candidate_title.strip())
            link = candidate_url
            break

        if not link:
            # fallback: try to extract url from first line if present
            link_match = re.search(r"(https?://[^\s\)]+)", block)
            if link_match:
                link = link_match.group(1)
                title = title or link

        if not link or link in seen_urls or _should_skip_url(link):
            continue

        seen_urls.add(link)
        results.append(
            SearchResult(
                title=title or link,
                url=link,
                snippet=snippet,
                source="duckduckgo",
                date=None,
                search_query=query,
            )
        )

        if len(results) >= num:
            break

    if not results:
        raise SearchError(f"DuckDuckGo search returned no results for '{query}'.")

    return results
