# SakuFox 🦊: 敏捷智能数据工作空间

> **Saku (樱花) 代表纯粹与热血（来自于路明非），Fox (狐狸) 代表敏锐与精准。像林间灵狐般，精准捕捉海量数据背后的核心逻辑。**

SakuFox 是一个强大、安全且交互式的数据工作区。名字灵感源自《龙族》中樱花般的瞬间爆发（也来自路明非的名字Sakura）与狐狸的机敏（Fox），致力于将复杂的多源数据转化为透明、可落地的业务洞察。

![intro](images/preview.png)
![intro](images/usage.png)

---

## ✨ 核心特性

### 📁 **持久化工作空间 (Workspaces)**
告别短暂的会话。为不同的项目创建、命名和管理专用的工作空间。
*   **隔离性**：每个工作空间独立维护其数据库连接、上传文件和业务知识。
*   **上下文持久化**：在不同工作空间之间切换，而不会丢失分析状态。

### 🔗 **统一数据上下文 (Unified Data Context)**
将所有数据源集成到 AI 的统一勾选上下文中：
*   **多数据库支持**：通过统一的连接弹窗连接 SQLite、PostgreSQL 等。
*   **精细化 Schema 选择**：精确挑选要暴露给 LLM 的数据表（最多 5 张），以保持高精度并避免超出上下文限制。
*   **大文件分析**：支持上传 `CSV`、`Excel`、`JSON` 或 `TXT` 文件。后端将其存储在磁盘上，并使用 **Pandas** 进行原生分析，支持数百万行数据且无需截断。

### 🧠 **自主迭代 Agent (Autonomous Iterative Agent)**
Agent 不仅仅返回一个表格，它执行完整的分析闭环：
*   **思考流展示**：实时观察 AI 在制定方案时的内部推理过程。
*   **双引擎逻辑**：Agent 会根据需求自动在 **SQL**（用于数据检索）和 **Python**（用于深度统计处理和可视化）之间切换。
*   **动态可视化**：基于发现的数据自动生成交互式的 **ECharts** 可视化图表。

### 🛡️ **企业级安全与控制**
*   **业务知识沉淀**：补充领域特定规则，这些规则会持久化保存在工作空间中，辅助 AI 理解。
*   **权限白名单**：数据表和文档仅在匹配用户组权限时才可访问。
*   **沙箱安全**：Python 代码在受控环境下执行，确保系统稳定性。

---

## 🚀 快速开始

### 前置要求
*   Python 3.10+
*   现代 Web 浏览器 (Chrome/Edge/Firefox)

### 安装步骤

1.  **环境设置**
    ```bash
    # 创建并激活虚拟环境
    python -m venv .venv
    .\.venv\Scripts\activate  # Windows
    # source .venv/bin/activate # Mac/Linux
    
    # 安装依赖
    pip install -r requirements.txt
    ```

2.  **配置**
    *   初始化 `app/config.py` (从 `app/config.example.py` 复制)。
    *   设置您的 LLM 提供商 (`openai`, `anthropic`, 或使用 `mock` 进行测试)。

3.  **运行应用**
    ```bash
    python -m uvicorn app.main:app --reload
    ```

4.  **访问仪表盘**
    *   访问 `http://localhost:8000/web/dashboard.html`。
    *   登录 (例如使用用户名 `admin` 进行 LDAP 演示)。
    *   创建一个新 **工作空间**，连接数据库，开始提问！

---

## 🏗️ 技术架构

*   **后端**: [FastAPI](https://fastapi.tiangolo.com/) + [SQLAlchemy](https://www.sqlalchemy.org/)
    *   **Agent**: 具备工具调用能力的自定义状态机 Agent。
    *   **数据层**: 使用 [Pandas](https://pandas.pydata.org/) 进行高性能文件分析。
*   **Frontend**: 原生 JavaScript + CSS (高质感暗色/毛玻璃美学设计)
    *   **图表**: [Apache ECharts](https://echarts.apache.org/)
    *   **Markdown**: [Marked.js](https://marked.js.org/)

## 📝 核心 API 端点

| 方法 | 端点 | 描述 |
| :--- | :--- | :--- |
| `POST` | `/api/chat/iterate` | 流式输出。SQL/Python 生成的主分析循环。 |
| `POST` | `/api/sandboxes` | 持久化工作空间管理的 CRUD 操作。 |
| `POST` | `/api/data/upload` | 将本地文件直接上传到工作空间磁盘存储。 |
| `POST` | `/api/chat/feedback` | 向当前工作空间沉淀业务知识/规则。 |
| `GET`  | `/api/sandboxes/{id}/db-tables` | 获取并选择外部数据库中的可用表结构。 |

---

## 🤝 贡献

欢迎贡献代码！请随时提交 Pull Request 或通过 Issue 提出功能建议。
