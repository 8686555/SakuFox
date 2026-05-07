# 报告格式切换功能实现计划

## 概述

将现有的"禁止PPT格式"改为"用户可选格式"，在一键分析结束后，报告页面提供切换按钮，让用户选择：
- **HTML格式**：漂亮的连续可滚动网页报告（当前默认）
- **PPT格式**：16:9幻灯片+滚动吸附风格

切换时重新调用LLM生成对应格式的报告。

## 当前状态分析

### 现有PPT检测机制（需移除）

| 文件 | 函数/位置 | 作用 |
|------|-----------|------|
| `app/agent.py` | `_report_html_looks_like_slide_deck` (L1469-1499) | 后端PPT检测函数 |
| `app/main.py` | `_report_html_looks_like_slide_deck` (L1376-1406) | 后端PPT检测函数 |
| `web/report.js` | `looksLikeSlideDeck` (L123-148) | 前端PPT检测函数 |
| `web/dashboard.js` | `looksLikeSlideDeck` (L1349-1375) | 前端PPT检测函数 |

### 现有提示模板（需修改）

| 文件 | 提示Key | 当前状态 |
|------|---------|----------|
| `app/config.example.py` | `report_bundle_system` | 明确禁止PPT |
| `app/config.example.py` | `report_bundle_user` | 包含"Do not make it look like PPT slides" |
| `app/config.example.py` | `html_report_system` | 明确禁止PPT |
| `app/config.example.py` | `html_report_user` | 包含禁止PPT的详细说明 |

### 报告生成流程

1. `app/main.py` `/api/auto-analyze` 接口调用 `generate_auto_analysis_report_bundle`
2. `app/agent.py` `generate_auto_analysis_report_bundle` 使用提示模板生成报告
3. 报告存储到数据库，前端通过 `/web/report.html?iteration_id=xxx` 查看

## 实现方案

### 1. 配置文件修改

**文件**: `app/config.example.py` 和 `app/config.py`

#### 1.1 添加PPT格式提示模板

新增 `report_bundle_ppt_system` 和 `report_bundle_ppt_user` 提示：

```python
"report_bundle_ppt_system": {
    "en": (
        "You are a principal analytics presentation designer. Produce only JSON for a self-contained analytics slide deck. "
        "Design the standalone HTML presentation from the completed iteration results. "
        "The html_document must be a 16:9 slide deck with scroll-snap navigation, real browser-renderable HTML."
    ),
    "zh": (
        "你是首席分析报告演示设计师。请只输出用于自包含分析幻灯片的 JSON。"
        "根据已完成的迭代结果设计独立 HTML 演示文稿。"
        "html_document 必须是 16:9 幻灯片格式，带滚动吸附导航，真正可被浏览器渲染的 HTML。"
    ),
},
"report_bundle_ppt_user": {
    "en": (
        # ... 包含16:9幻灯片、scroll-snap-type、每页一个章节等要求
    ),
    "zh": (
        # ... 中文版本
    ),
},
```

#### 1.2 修改现有HTML格式提示

移除禁止PPT的语句，改为强调HTML格式特点：
- 移除 "Do not make it look like PPT slides or a presentation deck"
- 移除 "不要做成 PPT 或演示文稿风格"
- 保留连续可滚动布局的描述

### 2. 数据模型修改

**文件**: `app/models.py`

#### 2.1 AutoAnalyzeRequest 添加 report_format 字段

```python
class AutoAnalyzeRequest(IterateRequest):
    """Run one-click autonomous multi-round analysis until the model stops using tools."""
    message: str = ""
    max_rounds: int | None = Field(default=None, ge=1)
    trace_mode: str = Field(default="full", pattern="^full$")
    report_format: str = Field(default="html", pattern="^(html|ppt)$")  # 新增
```

#### 2.2 新增重新生成报告请求模型

```python
class RegenerateReportRequest(BaseModel):
    """Regenerate report with different format."""
    iteration_id: str
    report_format: str = Field(pattern="^(html|ppt)$")
```

### 3. 后端逻辑修改

**文件**: `app/main.py`

#### 3.1 移除PPT检测函数

删除 `_report_html_looks_like_slide_deck` 函数 (L1376-1406)

#### 3.2 修改 `_normalize_auto_report_bundle` 函数

移除对 `_report_html_looks_like_slide_deck` 的调用

#### 3.3 修改 `/api/auto-analyze` 接口

传递 `report_format` 参数到 `generate_auto_analysis_report_bundle`

#### 3.4 新增 `/api/regenerate-report` 接口

```python
@app.post("/api/regenerate-report")
def regenerate_report(request: RegenerateReportRequest, user: User = Depends(get_current_user)):
    """重新生成指定格式的报告"""
    # 1. 获取iteration数据
    # 2. 调用 generate_auto_analysis_report_bundle 生成新格式报告
    # 3. 更新数据库中的报告数据
    # 4. 返回新的报告bundle
```

### 4. Agent逻辑修改

**文件**: `app/agent.py`

#### 4.1 移除PPT检测函数

删除 `_report_html_looks_like_slide_deck` 函数 (L1469-1499)

#### 4.2 修改 `generate_auto_analysis_report_bundle` 函数

添加 `report_format` 参数，根据格式选择不同的提示模板：

```python
def generate_auto_analysis_report_bundle(
    message: str,
    session_history: list[dict],
    business_knowledge: list[str],
    session_patches: list[str],
    loop_rounds: list[dict],
    chart_specs: list[dict],
    final_result_rows: list[dict],
    stop_reason: str,
    rounds_completed: int,
    provider: str | None = None,
    model: str | None = None,
    report_format: str = "html",  # 新增参数
) -> dict:
    # 根据report_format选择提示模板
    if report_format == "ppt":
        system_prompt = PROMPTS["report_bundle_ppt_system"][lang_code]
        user_prompt = PROMPTS["report_bundle_ppt_user"][lang_code]
    else:
        system_prompt = PROMPTS["report_bundle_system"][lang_code]
        user_prompt = PROMPTS["report_bundle_user"][lang_code]
    # ...
```

### 5. 前端修改

**文件**: `web/report.html`

#### 5.1 添加格式切换按钮

在toolbar区域添加切换按钮：

```html
<div class="toolbar">
  <div></div>
  <div class="actions">
    <button id="btnBack" class="btn btn-outline btn-sm"><i class="fa-solid fa-arrow-left"></i> Back To Chat</button>
    <select id="reportFormatSelect" class="btn btn-outline btn-sm">
      <option value="html">HTML 格式</option>
      <option value="ppt">PPT 格式</option>
    </select>
    <button id="btnPrint" class="btn btn-primary btn-sm"><i class="fa-solid fa-file-pdf"></i> Export PDF</button>
  </div>
</div>
```

**文件**: `web/report.js`

#### 5.2 移除PPT检测函数

删除 `looksLikeSlideDeck` 函数 (L123-148)

#### 5.3 修改 `normalizeHtmlDocument` 函数

移除对 `looksLikeSlideDeck` 的调用

#### 5.4 添加格式切换逻辑

```javascript
document.getElementById("reportFormatSelect").addEventListener("change", async function(e) {
    const format = e.target.value;
    const iterationId = getIterationIdFromUrl();
    
    // 显示加载状态
    showLoading();
    
    // 调用重新生成接口
    const response = await fetch("/api/regenerate-report", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ iteration_id: iterationId, report_format: format })
    });
    
    const bundle = await response.json();
    // 渲染新报告
    renderReportBundle(bundle);
});
```

**文件**: `web/dashboard.js`

#### 5.5 移除PPT检测函数

删除 `looksLikeSlideDeck` 函数 (L1349-1375)

### 6. PPT格式CSS模板

在PPT提示模板中包含以下CSS要求：

```css
/* 16:9幻灯片布局 */
.slide-deck {
    scroll-snap-type: y mandatory;
    overflow-y: scroll;
    height: 100vh;
}
.slide {
    scroll-snap-align: start;
    aspect-ratio: 16/9;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    justify-content: center;
    padding: 40px;
}
/* 每页一个章节 */
.slide h2 {
    font-size: 48px;
    margin-bottom: 30px;
}
.slide-content {
    font-size: 24px;
}
```

## 文件修改清单

| 文件 | 修改类型 | 主要内容 |
|------|----------|----------|
| `app/config.example.py` | 修改+新增 | 移除禁止PPT语句，添加PPT提示模板 |
| `app/config.py` | 修改+新增 | 同上，保持与example同步 |
| `app/models.py` | 新增 | 添加report_format字段，新增RegenerateReportRequest |
| `app/main.py` | 修改+新增 | 移除检测函数，修改auto-analyze，新增regenerate-report接口 |
| `app/agent.py` | 修改 | 移除检测函数，修改报告生成函数 |
| `web/report.html` | 修改 | 添加格式切换下拉框 |
| `web/report.js` | 修改 | 移除检测函数，添加切换逻辑 |
| `web/dashboard.js` | 修改 | 移除检测函数 |

## 验证步骤

1. **单元测试**: 运行 `tests/test_report_bundle_retry.py` 确保现有测试通过
2. **功能测试**:
   - 启动服务，执行一键分析
   - 在报告页面切换格式，验证重新生成功能
   - 验证HTML格式报告显示正常
   - 验证PPT格式报告显示16:9幻灯片+滚动吸附
3. **回归测试**: 确保移除PPT检测后，现有报告功能不受影响

## 假设与决策

- **假设**: 用户切换格式时愿意等待重新生成的时间
- **决策**: 使用scroll-snap实现PPT滚动吸附，而非JavaScript导航按钮
- **决策**: PPT格式每页一个章节，保持简洁
- **决策**: 两种格式都支持图表挂载（ECharts）

## 风险与注意事项

1. **性能**: 重新生成报告需要调用LLM，可能耗时较长，需显示加载状态
2. **一致性**: 同一iteration可能有多个版本的报告，需考虑存储策略
3. **兼容性**: 确保PPT格式在移动端也能正常显示