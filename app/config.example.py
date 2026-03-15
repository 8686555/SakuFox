from dataclasses import dataclass
from pathlib import Path
import os

# 说明：
# 1) 将本文件复制为 app/config.py 并填写实际参数
# 2) app/config.py 已加入 .gitignore，请勿提交密钥

LLM_PROVIDER = "mock"  # mock | openai | anthropic

OPENAI_API_KEY = ""
OPENAI_BASE_URL = "https://api.openai.com"
OPENAI_ENDPOINT = "/v1/chat/completions"
OPENAI_MODEL = "gpt-4o-mini"

ANTHROPIC_API_KEY = ""
ANTHROPIC_BASE_URL = "https://api.anthropic.com"
ANTHROPIC_ENDPOINT = "/v1/messages"
ANTHROPIC_MODEL = "claude-3-5-sonnet-latest"
ANTHROPIC_VERSION = "2023-06-01"
MAX_SELECTED_TABLES = 5
SQL_BUNDLE_SYSTEM_PROMPT = (
    "你是企业 SQL 分析助手。"
    "请先详细思考，分析用户意图、表结构选择和 SQL 构建逻辑。思考过程直接输出文本。"
    "思考完成后，必须输出一个 Markdown JSON 代码块（```json ... ```），其中包含键 sql, explanation, suggestions。"
    "suggestions 必须是长度为3的字符串数组。"
    "SQL 必须为单条 SELECT 语句，不要包含注释和 markdown。"
)
INSIGHT_PROMPT_METRICS = "你是企业数据分析师，输出 Markdown，说明关键指标口径与当前结论，不要输出代码。"
INSIGHT_PROMPT_ANOMALY = "你是异常归因分析师，输出 Markdown，聚焦异常与可能原因，不要输出代码。"
INSIGHT_PROMPT_ACTIONS = "你是业务负责人，输出 Markdown，给出可执行动作与优先级，不要输出代码。"
SUGGESTIONS_SYSTEM_PROMPT = (
    "你是企业 SQL 分析助手。基于用户问题和已生成的 SQL/解释，生成 3 个后续分析建议（suggestions）。"
    "请返回一个 JSON 字符串数组，例如 [\"建议1\", \"建议2\", \"建议3\"]。"
)
ANALYSIS_PLANS_SYSTEM_PROMPT = (
    "你是企业数据分析规划专家。请输出 JSON，包含 planning_message 和 plans。"
    "plans 是长度2的数组，分别是 sql_only 与 sql_python。每项包含 plan_id,name,description,tools,estimated_cost,kind,guidance,sql_hint。"
    "tools 只能使用 execute_select_sql 与 python_interpreter。不要输出 markdown。"
)
PLAN_ARTIFACTS_SYSTEM_PROMPT = (
    "你是 SQL + Python 分析执行代理。请输出 JSON，字段为 sql, python_code, python_blocks, explanation, tools。"
    "sql 必须是单条 SELECT。python_code 中如需访问数据库，只能调用 execute_select_sql(sql)。"
    "若需要多段 Python，请将后续代码段放入 python_blocks 数组。若为 sql_only 方案，python_code 置空字符串。"
    "图表输出必须写入 chart_specs(list)。不要输出 markdown。"
)


def _read_dotenv() -> dict[str, str]:
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return {}
    output: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        output[key.strip()] = value.strip().strip('"').strip("'")
    return output


def _pick(*values: str, default: str = "") -> str:
    for value in values:
        if value and str(value).strip():
            return str(value).strip()
    return default


@dataclass(frozen=True)
class AppConfig:
    llm_provider: str
    openai_api_key: str
    openai_base_url: str
    openai_endpoint: str
    openai_model: str
    anthropic_api_key: str
    anthropic_base_url: str
    anthropic_endpoint: str
    anthropic_model: str
    anthropic_version: str
    max_selected_tables: int
    sql_bundle_system_prompt: str
    insight_prompt_metrics: str
    insight_prompt_anomaly: str
    insight_prompt_actions: str
    suggestions_system_prompt: str
    analysis_plans_system_prompt: str
    plan_artifacts_system_prompt: str


def load_config() -> AppConfig:
    dotenv = _read_dotenv()
    return AppConfig(
        llm_provider=_pick(os.getenv("LLM_PROVIDER", ""), dotenv.get("LLM_PROVIDER", ""), default=LLM_PROVIDER).lower(),
        openai_api_key=_pick(os.getenv("OPENAI_API_KEY", ""), dotenv.get("OPENAI_API_KEY", ""), default=OPENAI_API_KEY),
        openai_base_url=_pick(os.getenv("OPENAI_BASE_URL", ""), dotenv.get("OPENAI_BASE_URL", ""), default=OPENAI_BASE_URL).rstrip("/"),
        openai_endpoint=_pick(os.getenv("OPENAI_ENDPOINT", ""), dotenv.get("OPENAI_ENDPOINT", ""), default=OPENAI_ENDPOINT),
        openai_model=_pick(os.getenv("OPENAI_MODEL", ""), dotenv.get("OPENAI_MODEL", ""), default=OPENAI_MODEL),
        anthropic_api_key=_pick(os.getenv("ANTHROPIC_API_KEY", ""), dotenv.get("ANTHROPIC_API_KEY", ""), default=ANTHROPIC_API_KEY),
        anthropic_base_url=_pick(os.getenv("ANTHROPIC_BASE_URL", ""), dotenv.get("ANTHROPIC_BASE_URL", ""), default=ANTHROPIC_BASE_URL).rstrip("/"),
        anthropic_endpoint=_pick(os.getenv("ANTHROPIC_ENDPOINT", ""), dotenv.get("ANTHROPIC_ENDPOINT", ""), default=ANTHROPIC_ENDPOINT),
        anthropic_model=_pick(os.getenv("ANTHROPIC_MODEL", ""), dotenv.get("ANTHROPIC_MODEL", ""), default=ANTHROPIC_MODEL),
        anthropic_version=_pick(os.getenv("ANTHROPIC_VERSION", ""), dotenv.get("ANTHROPIC_VERSION", ""), default=ANTHROPIC_VERSION),
        max_selected_tables=int(_pick(os.getenv("MAX_SELECTED_TABLES", ""), dotenv.get("MAX_SELECTED_TABLES", ""), default=str(MAX_SELECTED_TABLES))),
        sql_bundle_system_prompt=_pick(os.getenv("SQL_BUNDLE_SYSTEM_PROMPT", ""), dotenv.get("SQL_BUNDLE_SYSTEM_PROMPT", ""), default=SQL_BUNDLE_SYSTEM_PROMPT),
        insight_prompt_metrics=_pick(os.getenv("INSIGHT_PROMPT_METRICS", ""), dotenv.get("INSIGHT_PROMPT_METRICS", ""), default=INSIGHT_PROMPT_METRICS),
        insight_prompt_anomaly=_pick(os.getenv("INSIGHT_PROMPT_ANOMALY", ""), dotenv.get("INSIGHT_PROMPT_ANOMALY", ""), default=INSIGHT_PROMPT_ANOMALY),
        insight_prompt_actions=_pick(os.getenv("INSIGHT_PROMPT_ACTIONS", ""), dotenv.get("INSIGHT_PROMPT_ACTIONS", ""), default=INSIGHT_PROMPT_ACTIONS),
        suggestions_system_prompt=_pick(os.getenv("SUGGESTIONS_SYSTEM_PROMPT", ""), dotenv.get("SUGGESTIONS_SYSTEM_PROMPT", ""), default=SUGGESTIONS_SYSTEM_PROMPT),
        analysis_plans_system_prompt=_pick(os.getenv("ANALYSIS_PLANS_SYSTEM_PROMPT", ""), dotenv.get("ANALYSIS_PLANS_SYSTEM_PROMPT", ""), default=ANALYSIS_PLANS_SYSTEM_PROMPT),
        plan_artifacts_system_prompt=_pick(os.getenv("PLAN_ARTIFACTS_SYSTEM_PROMPT", ""), dotenv.get("PLAN_ARTIFACTS_SYSTEM_PROMPT", ""), default=PLAN_ARTIFACTS_SYSTEM_PROMPT),
    )
