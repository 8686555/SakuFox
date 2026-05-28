let sqlToolboxState = {
  sandboxes: [],
  currentSandboxId: "",
  runs: [],
  views: [],
  currentRun: null,
  currentColumns: [],
  selectedRunId: "",
  isExecuting: false,
  isSaving: false,
};

function escapeHtml(text) {
  return String(text ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function tr(key, fallback, params = {}) {
  const text = i18n.t(key, params);
  return !text || text === key ? fallback : text;
}

function currentSandbox() {
  return sqlToolboxState.sandboxes.find((item) => item.sandbox_id === sqlToolboxState.currentSandboxId) || null;
}

function normalizeFields(fields) {
  if (!fields) return [];
  return Array.isArray(fields) ? fields : [];
}

function formatCount(value, suffixKey, suffixFallback) {
  return `${escapeHtml(String(value ?? 0))} ${escapeHtml(tr(suffixKey, suffixFallback))}`;
}

function formatStatusTag(status) {
  const safeStatus = status === "success" ? "success" : (status || "failed");
  const kind = safeStatus === "success" ? "success" : "error";
  return `<span class="sql-toolbox-status-tag" data-kind="${kind}">${escapeHtml(safeStatus)}</span>`;
}

function setButtonLoading(button, labelEl, loading, idleText, loadingText) {
  if (!button || !labelEl) return;
  button.disabled = loading;
  labelEl.textContent = loading ? loadingText : idleText;
}

function setStatusBadge(prefix, text, kind = "neutral") {
  const badge = document.getElementById(`${prefix}Badge`);
  const textEl = document.getElementById(`${prefix}Text`);
  if (!badge || !textEl) return;
  badge.dataset.kind = kind;
  textEl.textContent = text;
}

function setRunStatus(text, kind = "neutral") {
  setStatusBadge("runStatus", text, kind);
}

function setSaveStatus(text, kind = "neutral") {
  setStatusBadge("saveStatus", text, kind);
}

function renderResultTable(rows, columns) {
  const wrap = document.getElementById("resultTableWrap");
  const table = document.getElementById("resultTable");
  if (!wrap || !table) return;

  const safeRows = Array.isArray(rows) ? rows : [];
  const safeColumns = Array.isArray(columns) && columns.length > 0
    ? columns.map((col) => (typeof col === "string" ? col : col.name)).filter(Boolean)
    : (safeRows[0] ? Object.keys(safeRows[0]) : []);

  if (!safeColumns.length) {
    wrap.style.display = "none";
    table.innerHTML = "";
    return;
  }

  const headHtml = `<thead><tr>${safeColumns.map((col) => `<th>${escapeHtml(col)}</th>`).join("")}</tr></thead>`;
  const bodyHtml = safeRows.length
    ? `<tbody>${safeRows.map((row) => `<tr>${safeColumns.map((col) => `<td>${escapeHtml(row?.[col])}</td>`).join("")}</tr>`).join("")}</tbody>`
    : `<tbody><tr><td colspan="${safeColumns.length}" style="text-align:center;color:#64748b;">${escapeHtml(tr("sql_toolbox_no_rows", "No rows returned"))}</td></tr></tbody>`;
  table.innerHTML = headHtml + bodyHtml;
  wrap.style.display = "block";
}

function renderFieldDescriptionInputs(columns) {
  const grid = document.getElementById("fieldDescGrid");
  const empty = document.getElementById("fieldDescEmpty");
  if (!grid || !empty) return;

  const safeColumns = Array.isArray(columns) ? columns : [];
  if (!safeColumns.length) {
    grid.innerHTML = "";
    empty.style.display = "block";
    return;
  }

  empty.style.display = "none";
  grid.innerHTML = safeColumns
    .map((col) => {
      const name = typeof col === "string" ? col : (col?.name || "");
      return `
        <div class="sql-toolbox-field-item">
          <label>${escapeHtml(name)}</label>
          <input type="text" data-field-name="${escapeHtml(name)}" placeholder="${escapeHtml(tr("sql_toolbox_field_desc_placeholder", "Field description (optional)"))}" />
        </div>
      `;
    })
    .join("");
}

function syncFieldDescriptionsFromRun(run) {
  const columns = normalizeFields(run?.columns || []);
  sqlToolboxState.currentColumns = columns;
  renderFieldDescriptionInputs(columns);
}

function setResultMeta(text) {
  const resultMeta = document.getElementById("resultMeta");
  if (resultMeta) resultMeta.textContent = text;
}

function resetEditorState() {
  sqlToolboxState.currentRun = null;
  sqlToolboxState.currentColumns = [];
  sqlToolboxState.selectedRunId = "";
  setResultMeta(tr("sql_toolbox_not_run", "Not executed yet"));
  renderResultTable([], []);
  renderFieldDescriptionInputs([]);
  const viewNameInput = document.getElementById("viewNameInput");
  const viewDescInput = document.getElementById("viewDescInput");
  if (viewNameInput) viewNameInput.value = "";
  if (viewDescInput) viewDescInput.value = "";
  setRunStatus(tr("sql_toolbox_waiting", "Waiting to run"), "neutral");
  setSaveStatus(tr("sql_toolbox_not_saved", "Not saved yet"), "neutral");
}

function renderModelList() {
  const list = document.getElementById("modelList");
  if (!list) return;

  const sandbox = currentSandbox();
  if (!sandbox) {
    list.innerHTML = `<li class="list-item">${escapeHtml(tr("sql_toolbox_need_sandbox", "Please select a sandbox first"))}</li>`;
    return;
  }

  const physicalTables = Array.isArray(sandbox.tables) ? sandbox.tables : [];
  const virtualViews = Array.isArray(sandbox.virtual_views) ? sandbox.virtual_views : [];
  const uploads = sandbox.uploads && typeof sandbox.uploads === "object" ? Object.entries(sandbox.uploads) : [];

  const items = [];
  physicalTables.forEach((name) => {
    items.push(`
      <li class="list-item">
        <div class="sql-toolbox-item-row">
          <div class="sql-toolbox-item-copy">
            <strong>${escapeHtml(name)}</strong>
          </div>
          <span class="sql-toolbox-type-pill" data-tone="table">${escapeHtml(tr("sql_toolbox_physical_table", "Physical Table"))}</span>
        </div>
      </li>
    `);
  });

  virtualViews.forEach((view) => {
    const name = view?.name || "";
    if (!name) return;
    items.push(`
      <li class="list-item">
        <div class="sql-toolbox-item-row">
          <div class="sql-toolbox-item-copy">
            <strong>${escapeHtml(name)}</strong>
            ${view.description ? `<div class="section-muted" style="margin-top:6px;">${escapeHtml(view.description)}</div>` : ""}
          </div>
          <span class="sql-toolbox-type-pill" data-tone="view">${escapeHtml(tr("virtual_view_label", "Virtual View"))}</span>
        </div>
      </li>
    `);
  });

  uploads.forEach(([key, value]) => {
    const name = typeof key === "string" ? key : (value?.name || value?.dataset_name || "");
    if (!name) return;
    items.push(`
      <li class="list-item">
        <div class="sql-toolbox-item-row">
          <div class="sql-toolbox-item-copy">
            <strong>${escapeHtml(name)}</strong>
          </div>
          <span class="sql-toolbox-type-pill" data-tone="upload">${escapeHtml(tr("sql_toolbox_upload_file", "Uploaded File"))}</span>
        </div>
      </li>
    `);
  });

  list.innerHTML = items.length ? items.join("") : `<li class="list-item">${escapeHtml(tr("sql_toolbox_no_models", "No tables/views available"))}</li>`;
}

function renderRuns() {
  const list = document.getElementById("runList");
  if (!list) return;

  const runs = sqlToolboxState.runs || [];
  if (!runs.length) {
    list.innerHTML = `<li class="list-item">${escapeHtml(tr("sql_toolbox_no_runs", "No execution history"))}</li>`;
    return;
  }

  list.innerHTML = runs
    .map((run) => {
      const isSelected = run.run_id === sqlToolboxState.selectedRunId;
      const rowCount = typeof run.row_count === "number" ? run.row_count : 0;
      return `
        <li class="list-item clickable${isSelected ? " selected" : ""}" data-run-id="${escapeHtml(run.run_id)}" tabindex="0" role="button">
          <div class="sql-toolbox-item-header">
            <div class="sql-toolbox-item-copy">
              <strong>${escapeHtml(run.run_id)}</strong>
            </div>
            <button class="btn btn-outline btn-sm sql-toolbox-action-btn load-run-btn" type="button" data-run-id="${escapeHtml(run.run_id)}">${escapeHtml(tr("sql_toolbox_load_btn", "Load"))}</button>
          </div>
          <div class="sql-toolbox-run-meta">
            ${formatStatusTag(run.status)}
            <span>${formatCount(rowCount, "rows", "rows")}</span>
            <span>${escapeHtml(String(run.duration_ms || 0))}ms</span>
          </div>
          <div class="sql-toolbox-run-sql-preview">${escapeHtml((run.sql || "").slice(0, 220))}</div>
        </li>
      `;
    })
    .join("");

  list.querySelectorAll("[data-run-id]").forEach((item) => {
    item.addEventListener("click", async (event) => {
      const target = event.target;
      if (target instanceof HTMLElement && target.closest(".load-run-btn")) return;
      const runId = item.getAttribute("data-run-id");
      const run = sqlToolboxState.runs.find((entry) => entry.run_id === runId);
      if (!run) return;
      await loadRunIntoEditor(run);
    });
    item.addEventListener("keydown", async (event) => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      const runId = item.getAttribute("data-run-id");
      const run = sqlToolboxState.runs.find((entry) => entry.run_id === runId);
      if (!run) return;
      await loadRunIntoEditor(run);
    });
  });

  list.querySelectorAll(".load-run-btn").forEach((btn) => {
    btn.addEventListener("click", async (event) => {
      event.stopPropagation();
      const runId = btn.getAttribute("data-run-id");
      const run = sqlToolboxState.runs.find((entry) => entry.run_id === runId);
      if (!run) return;
      await loadRunIntoEditor(run);
    });
  });
}

function renderViews() {
  const list = document.getElementById("viewList");
  if (!list) return;

  const views = sqlToolboxState.views || [];
  if (!views.length) {
    list.innerHTML = `<li class="list-item">${escapeHtml(tr("sql_toolbox_no_views", "No analysis views yet"))}</li>`;
    return;
  }

  list.innerHTML = views
    .map((view) => {
      const cols = Array.isArray(view.columns) ? view.columns : [];
      return `
        <li class="list-item">
          <div class="sql-toolbox-view-row">
            <div class="sql-toolbox-item-copy">
              <strong>${escapeHtml(view.name || view.view_id)}</strong>
              <div class="section-muted" style="margin-top:6px;">${escapeHtml(view.description || "")}</div>
              <div class="sql-toolbox-view-meta">
                <span>${formatCount(cols.length, "cols", "cols")}</span>
                <span>source ${escapeHtml(view.source_run_id || "")}</span>
              </div>
            </div>
            <button class="btn btn-outline btn-sm sql-toolbox-action-btn delete-view-btn" type="button" data-view-id="${escapeHtml(view.view_id)}">${escapeHtml(tr("sql_toolbox_delete_btn", "Delete"))}</button>
          </div>
        </li>
      `;
    })
    .join("");

  list.querySelectorAll(".delete-view-btn").forEach((btn) => {
    btn.addEventListener("click", async (event) => {
      event.stopPropagation();
      const viewId = btn.getAttribute("data-view-id");
      if (!viewId) return;
      await deleteVirtualView(viewId);
    });
  });
}

function updateActionButtons() {
  const runBtn = document.getElementById("runBtn");
  const runBtnLabel = document.getElementById("runBtnLabel");
  const saveViewBtn = document.getElementById("saveViewBtn");
  const saveBtnLabel = document.getElementById("saveBtnLabel");

  setButtonLoading(
    runBtn,
    runBtnLabel,
    sqlToolboxState.isExecuting,
    tr("sql_toolbox_run", "Run Validation"),
    tr("sql_toolbox_running_btn", "Running..."),
  );

  const canSave = sqlToolboxState.currentRun && sqlToolboxState.currentRun.status === "success" && !sqlToolboxState.isSaving;
  if (saveViewBtn) {
    saveViewBtn.disabled = !canSave;
  }
  if (saveBtnLabel) {
    saveBtnLabel.textContent = sqlToolboxState.isSaving
      ? tr("sql_toolbox_saving_btn", "Saving...")
      : tr("sql_toolbox_save_btn", "Save Analysis View");
  }
}

async function loadSandboxes(selectId = "") {
  const res = await api("/api/sandboxes");
  sqlToolboxState.sandboxes = res.sandboxes || [];

  const sandboxSelect = document.getElementById("sandboxSelect");
  if (!sandboxSelect) return;

  const previous = selectId || sqlToolboxState.currentSandboxId || sandboxSelect.value;
  sandboxSelect.innerHTML = "";

  if (!sqlToolboxState.sandboxes.length) {
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = tr("sql_toolbox_no_sandbox", "No sandboxes available");
    sandboxSelect.appendChild(opt);
    sqlToolboxState.currentSandboxId = "";
    renderModelList();
    renderRuns();
    renderViews();
    updateActionButtons();
    return;
  }

  sqlToolboxState.sandboxes.forEach((sb) => {
    const opt = document.createElement("option");
    opt.value = sb.sandbox_id;
    opt.textContent = sb.name || sb.sandbox_id;
    sandboxSelect.appendChild(opt);
  });

  if (previous && sqlToolboxState.sandboxes.some((sb) => sb.sandbox_id === previous)) {
    sandboxSelect.value = previous;
  } else {
    sandboxSelect.value = sqlToolboxState.sandboxes[0].sandbox_id;
  }

  sqlToolboxState.currentSandboxId = sandboxSelect.value || "";
  renderModelList();
}

async function loadRunsAndViews() {
  const sandboxId = sqlToolboxState.currentSandboxId;
  if (!sandboxId) {
    sqlToolboxState.runs = [];
    sqlToolboxState.views = [];
    renderRuns();
    renderViews();
    updateActionButtons();
    return;
  }

  const [runsRes, viewsRes] = await Promise.all([
    api(`/api/sql-toolbox/runs?sandbox_id=${encodeURIComponent(sandboxId)}`),
    api(`/api/sandboxes/${encodeURIComponent(sandboxId)}/virtual-views`),
  ]);

  sqlToolboxState.runs = runsRes.runs || [];
  sqlToolboxState.views = viewsRes.virtual_views || [];
  renderRuns();
  renderViews();
  updateActionButtons();
}

async function refreshEverything(selectId = "") {
  await loadSandboxes(selectId);
  await loadRunsAndViews();
}

async function loadRunIntoEditor(run) {
  const sqlInput = document.getElementById("sqlInput");
  const viewNameInput = document.getElementById("viewNameInput");
  const viewDescInput = document.getElementById("viewDescInput");
  if (!sqlInput || !viewNameInput || !viewDescInput) return;

  sqlInput.value = run.sql || "";
  sqlToolboxState.currentRun = run;
  sqlToolboxState.selectedRunId = run.run_id || "";
  syncFieldDescriptionsFromRun(run);

  setResultMeta(`run ${run.run_id} | ${run.status || "unknown"} | rows=${run.row_count || 0} | ${run.duration_ms || 0}ms`);
  renderResultTable(run.result_preview || [], run.columns || []);

  const inferredName = `${(currentSandbox()?.name || "sandbox").replace(/[^A-Za-z0-9_]/g, "_")}_view`;
  if (!viewNameInput.value) {
    viewNameInput.value = inferredName.replace(/_+/g, "_").replace(/^_+|_+$/g, "").slice(0, 48);
  }
  if (!viewDescInput.value) {
    viewDescInput.value = "";
  }

  setSaveStatus(
    run.status === "success"
      ? tr("sql_toolbox_can_save_run", "This successful run can be saved")
      : tr("sql_toolbox_cannot_save_run", "This run cannot be saved"),
    run.status === "success" ? "success" : "error",
  );
  renderRuns();
  updateActionButtons();
}

async function executeCurrentSql() {
  const sandboxId = sqlToolboxState.currentSandboxId;
  const sqlInput = document.getElementById("sqlInput");

  if (!sandboxId) {
    setRunStatus(tr("sql_toolbox_need_sandbox", "Please select a sandbox first"), "error");
    return;
  }
  if (!sqlInput || !sqlInput.value.trim()) {
    setRunStatus(tr("sql_toolbox_need_sql", "Please enter SQL"), "error");
    return;
  }

  sqlToolboxState.currentRun = null;
  sqlToolboxState.selectedRunId = "";
  sqlToolboxState.isExecuting = true;
  updateActionButtons();
  setRunStatus(tr("sql_toolbox_executing", "Executing..."), "running");
  setResultMeta(tr("sql_toolbox_executing", "Executing..."));

  try {
    const res = await api("/api/sql-toolbox/execute", {
      method: "POST",
      body: JSON.stringify({
        sandbox_id: sandboxId,
        sql: sqlInput.value,
      }),
    });
    const run = res.run;
    sqlToolboxState.currentRun = run;
    sqlToolboxState.selectedRunId = run.run_id || "";
    syncFieldDescriptionsFromRun(run);
    renderResultTable(run.result_preview || [], run.columns || []);
    setResultMeta(`run ${run.run_id} | ${run.status} | rows=${run.row_count || 0} | ${run.duration_ms || 0}ms`);
    setRunStatus(tr("sql_toolbox_execute_success", "Execution successful"), "success");
    setSaveStatus(tr("sql_toolbox_can_save", "You can save this as an analysis view"), "success");
    await loadRunsAndViews();
  } catch (err) {
    const message = err?.message || tr("sql_toolbox_execute_failed", "Execution failed");
    setRunStatus(message, "error");
    setResultMeta(message);
    setSaveStatus(tr("sql_toolbox_cannot_save_run", "This run cannot be saved"), "error");
    await loadRunsAndViews();
  } finally {
    sqlToolboxState.isExecuting = false;
    renderRuns();
    updateActionButtons();
  }
}

async function saveCurrentView() {
  const sandboxId = sqlToolboxState.currentSandboxId;
  const run = sqlToolboxState.currentRun;
  const nameInput = document.getElementById("viewNameInput");
  const descInput = document.getElementById("viewDescInput");
  if (!sandboxId) {
    setSaveStatus(tr("sql_toolbox_need_sandbox", "Please select a sandbox first"), "error");
    return;
  }
  if (!run || run.status !== "success") {
    setSaveStatus(tr("sql_toolbox_select_success_run", "Please select a successful execution run first"), "error");
    return;
  }

  const name = (nameInput?.value || "").trim();
  const description = (descInput?.value || "").trim();
  if (!name) {
    setSaveStatus(tr("sql_toolbox_need_view_name", "Please enter a view name"), "error");
    return;
  }
  if (!description) {
    setSaveStatus(tr("sql_toolbox_need_view_desc", "Please enter a business description"), "error");
    return;
  }

  const fieldDescriptions = {};
  document.querySelectorAll("#fieldDescGrid input[data-field-name]").forEach((input) => {
    const fieldName = input.getAttribute("data-field-name");
    const value = (input.value || "").trim();
    if (fieldName && value) {
      fieldDescriptions[fieldName] = value;
    }
  });

  sqlToolboxState.isSaving = true;
  updateActionButtons();
  setSaveStatus(tr("sql_toolbox_saving_btn", "Saving..."), "running");

  try {
    const res = await api(`/api/sandboxes/${encodeURIComponent(sandboxId)}/virtual-views`, {
      method: "POST",
      body: JSON.stringify({
        source_run_id: run.run_id,
        name,
        description,
        field_descriptions: fieldDescriptions,
      }),
    });
    setSaveStatus(tr("sql_toolbox_save_success", "Saved: {name}", { name: res.virtual_view.name }), "success");
    await loadRunsAndViews();
  } catch (err) {
    setSaveStatus(err?.message || tr("sql_toolbox_save_failed", "Save failed"), "error");
  } finally {
    sqlToolboxState.isSaving = false;
    updateActionButtons();
  }
}

async function deleteVirtualView(viewId) {
  const sandboxId = sqlToolboxState.currentSandboxId;
  if (!sandboxId || !viewId) return;
  const view = sqlToolboxState.views.find((item) => item.view_id === viewId);
  const label = view?.name || viewId;
  if (!confirm(tr("sql_toolbox_delete_confirm", `Delete analysis view "${label}"?`, { name: label }))) return;

  try {
    await api(`/api/sandboxes/${encodeURIComponent(sandboxId)}/virtual-views/${encodeURIComponent(viewId)}`, {
      method: "DELETE",
    });
    await loadRunsAndViews();
    setSaveStatus(tr("sql_toolbox_delete_success", "Analysis view deleted"), "success");
  } catch (err) {
    setSaveStatus(err?.message || tr("sql_toolbox_delete_failed", "Delete failed"), "error");
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  const userInfo = document.getElementById("userInfo");
  const sandboxSelect = document.getElementById("sandboxSelect");
  const runBtn = document.getElementById("runBtn");
  const saveViewBtn = document.getElementById("saveViewBtn");
  const sqlInput = document.getElementById("sqlInput");

  renderFieldDescriptionInputs([]);
  updateActionButtons();

  try {
    const me = await api("/api/me");
    if (typeof setAppFeatures === "function") setAppFeatures(me.features || {});
    userInfo.textContent = me.user.username || me.user.display_name || "";
  } catch (err) {
    if (window.APP_FEATURES?.auth_system !== false) window.location.href = "/web/login.html";
    return;
  }

  sandboxSelect.addEventListener("change", async () => {
    sqlToolboxState.currentSandboxId = sandboxSelect.value || "";
    resetEditorState();
    renderModelList();
    await loadRunsAndViews();
  });

  runBtn.addEventListener("click", executeCurrentSql);
  saveViewBtn.addEventListener("click", saveCurrentView);
  sqlInput.addEventListener("keydown", (event) => {
    if ((event.ctrlKey || event.metaKey) && event.key === "Enter") {
      event.preventDefault();
      executeCurrentSql();
    }
  });

  try {
    await refreshEverything();
    if (sqlToolboxState.currentSandboxId) {
      const currentRuns = sqlToolboxState.runs || [];
      if (currentRuns.length > 0) {
        await loadRunIntoEditor(currentRuns[0]);
      }
    }
  } catch (err) {
    setRunStatus(err?.message || tr("sql_toolbox_loading_fail", "Load failed"), "error");
  } finally {
    updateActionButtons();
  }
});
