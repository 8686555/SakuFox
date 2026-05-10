(function () {
  const TEXTS = {
    zh: {
      pageTitle: "\u5206\u6790\u62a5\u544a",
      back: "\u8fd4\u56de\u5bf9\u8bdd",
      exportPdf: "\u5bfc\u51fa PDF",
      loading: "\u6b63\u5728\u52a0\u8f7d\u62a5\u544a...",
      missingIteration: "\u7f3a\u5c11 iteration_id",
      loadFailed: "\u52a0\u8f7d\u62a5\u544a\u5931\u8d25",
      emptyReport: "\u62a5\u544a\u5185\u5bb9\u4e3a\u7a7a",
      renderFailed: "\u62a5\u544a\u6e32\u67d3\u5931\u8d25",
      iteration: "\u8fed\u4ee3",
      chatPlaceholder: "\u63d0\u51fa\u5c55\u793a\u4fee\u6539\u8981\u6c42...",
      chatSend: "\u53d1\u9001",
      chatEmpty: "\u8bf7\u8f93\u5165\u4fee\u6539\u8981\u6c42",
      chatUpdating: "\u6b63\u5728\u4fee\u6539 HTML \u5c55\u793a...",
      chatUpdated: "\u5df2\u66f4\u65b0 HTML \u5c55\u793a\u3002",
      chatFailed: "\u4fee\u6539\u5931\u8d25",
    },
    en: {
      pageTitle: "Analysis Report",
      back: "Back To Chat",
      exportPdf: "Export PDF",
      loading: "Loading report...",
      missingIteration: "Missing iteration_id",
      loadFailed: "Failed to load report",
      emptyReport: "Report content is empty",
      renderFailed: "Failed to render report",
      iteration: "Iteration",
      chatPlaceholder: "Describe how to change the presentation...",
      chatSend: "Send",
      chatEmpty: "Please enter a revision request",
      chatUpdating: "Updating HTML presentation...",
      chatUpdated: "HTML presentation updated.",
      chatFailed: "Revision failed",
    },
  };

  function qs(id) {
    return document.getElementById(id);
  }

  function getToken() {
    return localStorage.getItem("token");
  }

  function getQueryParam(name) {
    const params = new URLSearchParams(window.location.search);
    return params.get(name) || "";
  }

  function getLang() {
    const fromQuery = getQueryParam("lang");
    if (fromQuery === "en" || fromQuery === "zh") return fromQuery;
    const fromStorage = localStorage.getItem("lang");
    return fromStorage === "en" ? "en" : "zh";
  }

  function t(lang, key) {
    const pack = TEXTS[lang] || TEXTS.zh;
    return pack[key] || TEXTS.en[key] || key;
  }

  function showError(message) {
    const loading = qs("loading");
    const err = qs("error");
    if (loading) loading.style.display = "none";
    if (err) {
      err.style.display = "block";
      err.textContent = message;
    }
  }

  function extractHtmlFromJsonLike(rawText) {
    const text = String(rawText || "").trim();
    if (!text) return "";
    const stripFence = text.replace(/^```(?:json|html)?\s*/i, "").replace(/\s*```$/i, "").trim();
    const extractStandaloneHtml = (candidate) => {
      const match = String(candidate || "").match(/<!doctype html[\s\S]*?<\/html>|<html[\s\S]*?<\/html>/i);
      return match ? match[0].trim() : "";
    };

    const tryParse = (candidate) => {
      try {
        const parsed = JSON.parse(candidate);
        if (parsed && typeof parsed === "object" && typeof parsed.html_document === "string") {
          const rawHtml = parsed.html_document.trim();
          return extractStandaloneHtml(rawHtml) || rawHtml;
        }
      } catch (_) {
        // ignore
      }
      return "";
    };

    let html = tryParse(stripFence);
    if (html) return html;

    const firstBrace = stripFence.indexOf("{");
    const lastBrace = stripFence.lastIndexOf("}");
    if (firstBrace >= 0 && lastBrace > firstBrace) {
      html = tryParse(stripFence.slice(firstBrace, lastBrace + 1));
      if (html) return html;
    }

    const htmlField = stripFence.match(/"html_document"\s*:\s*"([\s\S]*?)"\s*(?:,\s*"chart_bindings"|,\s*"summary"|,\s*"title"|,\s*"legacy_markdown"|\})/i);
    if (htmlField && htmlField[1]) {
      try {
        const rawHtml = JSON.parse(`"${htmlField[1]}"`).trim();
        const html = extractStandaloneHtml(rawHtml) || rawHtml;
        return hasReportRenderArtifacts(html) ? "" : html;
      } catch (_) {
        const rawHtml = htmlField[1].trim();
        const html = extractStandaloneHtml(rawHtml) || rawHtml;
        return hasReportRenderArtifacts(html) ? "" : html;
      }
    }

    const htmlBlock = stripFence.match(/<!doctype html[\s\S]*?<\/html>|<html[\s\S]*?<\/html>/i);
    if (htmlBlock) return htmlBlock[0].trim();

    return "";
  }

  function hasReportRenderArtifacts(htmlText) {
    const raw = String(htmlText || "");
    if (!raw.trim()) return true;
    const visible = raw
      .replace(/<script[\s\S]*?<\/script>|<style[\s\S]*?<\/style>/gi, " ")
      .replace(/<[^>]+>/g, "\n");
    return /\\u[0-9a-fA-F]{4}|\\[ntr]|"html_document"\s*:|"chart_bindings"\s*:|\{\s*"title"\s*:|�|(?:Ã|Â|å|æ|ç|è|é|ä){2,}/i.test(visible)
      || /&lt;\s*(?:!doctype|\/?html|\/?body|\/?div|\/?table)/i.test(raw);
  }

  function escapeText(text) {
    return String(text || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;");
  }

  function appendReportChatMessage(container, role, text, isError = false) {
    if (!container) return;
    const item = document.createElement("div");
    item.className = `report-chat-message ${role || "assistant"}${isError ? " error" : ""}`;
    item.textContent = String(text || "");
    container.appendChild(item);
    container.classList.add("has-messages");
    container.scrollTop = container.scrollHeight;
    return item;
  }

  function normalizeHtmlDocument(rawText) {
    const text = String(rawText || "").trim();
    if (!text) return "";

    const extracted = extractHtmlFromJsonLike(text);
    if (extracted && !hasReportRenderArtifacts(extracted)) return extracted;

    const htmlBlock = text.match(/<!doctype html[\s\S]*?<\/html>|<html[\s\S]*?<\/html>/i);
    if (htmlBlock && !hasReportRenderArtifacts(htmlBlock[0])) return htmlBlock[0].trim();

    if (text.startsWith("{") || text.startsWith("[")) return "";
    if (/"html_document"\s*:|"chart_bindings"\s*:|\\u[0-9a-fA-F]{4}|&lt;\s*(?:!doctype|\/?html)/i.test(text)) return "";

    return `<!doctype html><html lang="zh-CN"><head><meta charset="UTF-8"/><meta name="viewport" content="width=device-width, initial-scale=1.0"/><title>Report</title><style>body{font-family:Arial,sans-serif;margin:24px;color:#111827;background:#fff;}pre{white-space:pre-wrap;line-height:1.6;}</style></head><body><pre>${escapeText(text)}</pre></body></html>`;
  }

  async function fetchReport(iterationId, lang) {
    const headers = { "Content-Type": "application/json", "X-Language": lang };
    const token = getToken();
    if (token) headers.Authorization = `Bearer ${token}`;
    const response = await fetch(`/api/reports/iterations/${encodeURIComponent(iterationId)}`, {
      method: "GET",
      headers,
      credentials: "include",
    });
    if (!response.ok) {
      throw new Error(`${t(lang, "loadFailed")}: ${response.status}`);
    }
    return response.json();
  }

  async function sendReportChat(iterationId, message, lang) {
    const headers = { "Content-Type": "application/json", "X-Language": lang };
    const token = getToken();
    if (token) headers.Authorization = `Bearer ${token}`;
    const response = await fetch(`/api/reports/iterations/${encodeURIComponent(iterationId)}/chat`, {
      method: "POST",
      headers,
      credentials: "include",
      body: JSON.stringify({ message }),
    });
    if (!response.ok) {
      let detail = `${t(lang, "chatFailed")}: ${response.status}`;
      try {
        const err = await response.json();
        detail = err.detail || detail;
      } catch (_) {
        // ignore
      }
      throw new Error(detail);
    }
    return response.json();
  }

  function syncFrameHeight(frame) {
    try {
      const doc = frame.contentDocument;
      if (!doc) return;
      if (doc.documentElement) doc.documentElement.style.overflow = "auto";
      if (doc.body) doc.body.style.overflow = "auto";
      if (doc.body) doc.body.style.margin = doc.body.style.margin || "0";
      const bodyHeight = doc.body ? Math.max(doc.body.scrollHeight, Math.ceil(doc.body.getBoundingClientRect().height || 0)) : 0;
      const htmlHeight = doc.documentElement ? Math.max(doc.documentElement.scrollHeight, Math.ceil(doc.documentElement.getBoundingClientRect().height || 0)) : 0;
      const viewportFloor = Math.max(800, window.innerHeight - 140);
      const target = Math.max(bodyHeight, htmlHeight, viewportFloor);
      frame.style.height = `${target}px`;
    } catch (_) {
      // no-op
    }
  }

  function printFrame(frame) {
    if (!frame || !frame.contentWindow) return;
    try {
      frame.contentWindow.focus();
      frame.contentWindow.print();
    } catch (_) {
      window.print();
    }
  }

  function normalizeReportFormat(value) {
    const text = String(value || "").trim().toLowerCase();
    return ["report", "ppt", "custom"].includes(text) ? text : "report";
  }

  function applyPptViewportFit(frame, reportFormat) {
    if (!frame || normalizeReportFormat(reportFormat) !== "ppt") return;
    const doc = frame.contentDocument;
    if (!doc || !doc.documentElement || !doc.body) return;

    const html = doc.documentElement;
    const body = doc.body;
    const viewportWidth = Math.max(320, frame.clientWidth || frame.parentElement?.clientWidth || 0);
    if (!viewportWidth) return;

    body.style.zoom = "";
    body.style.transform = "";
    body.style.transformOrigin = "";
    body.style.width = "";
    body.style.maxWidth = "";
    body.style.marginLeft = "";
    body.style.marginRight = "";
    body.style.overflowX = "hidden";
    html.style.overflowX = "hidden";

    const naturalWidth = Math.max(
      body.scrollWidth || 0,
      html.scrollWidth || 0,
      body.offsetWidth || 0,
      html.clientWidth || 0
    );
    if (!naturalWidth) return;

    const scale = Math.min(1, (viewportWidth - 8) / naturalWidth);
    if (scale >= 0.995) return;

    body.style.zoom = String(scale);
    body.style.width = `${naturalWidth}px`;
    body.style.maxWidth = `${naturalWidth}px`;
    body.style.marginLeft = "auto";
    body.style.marginRight = "auto";
  }

  async function init() {
    const frame = qs("reportFrame");
    const loading = qs("loading");
    const btnBack = qs("btnBack");
    const btnPrint = qs("btnPrint");
    const chatInput = qs("reportChatInput");
    const chatSend = qs("reportChatSend");
    const chatStatus = qs("reportChatStatus");
    const chatMessages = qs("reportChatMessages");
    const iterationId = getQueryParam("iteration_id");
    const printMode = getQueryParam("print") === "1";
    const lang = getLang();
    let currentReportFormat = "report";
    let currentHtmlDocument = "";

    document.documentElement.lang = lang === "en" ? "en" : "zh-CN";
    document.title = t(lang, "pageTitle");
    if (loading) loading.textContent = t(lang, "loading");
    if (btnBack) btnBack.innerHTML = '<i class="fa-solid fa-arrow-left"></i> ' + t(lang, "back");
    if (btnPrint) btnPrint.innerHTML = '<i class="fa-solid fa-file-pdf"></i> ' + t(lang, "exportPdf");
    if (chatInput) chatInput.placeholder = t(lang, "chatPlaceholder");
    if (chatSend) chatSend.textContent = t(lang, "chatSend");

    btnBack.onclick = () => {
      if (window.history.length > 1) window.history.back();
      else window.location.href = "/dashboard";
    };
    btnPrint.onclick = () => printFrame(frame);

    if (!iterationId) {
      showError(t(lang, "missingIteration"));
      return;
    }

    const setChatStatus = (message, isError = false) => {
      if (!chatStatus) return;
      chatStatus.textContent = message || "";
      chatStatus.style.color = isError ? "#dc2626" : "#475569";
    };

    const renderHtmlDocument = (htmlDocument, reportFormat) => {
      currentHtmlDocument = htmlDocument;
      frame.onload = () => {
        try {
          const doc = frame.contentDocument;
          if (!doc) return;
          if (doc.documentElement) doc.documentElement.style.minHeight = "100%";
          if (doc.body) doc.body.style.minHeight = "100%";
          currentReportFormat = normalizeReportFormat(reportFormat);
          applyPptViewportFit(frame, currentReportFormat);
          syncFrameHeight(frame);
          setTimeout(() => applyPptViewportFit(frame, currentReportFormat), 80);
          setTimeout(() => syncFrameHeight(frame), 250);
          setTimeout(() => applyPptViewportFit(frame, currentReportFormat), 300);
          setTimeout(() => syncFrameHeight(frame), 900);
          setTimeout(() => applyPptViewportFit(frame, currentReportFormat), 900);
          if (printMode) {
            setTimeout(() => printFrame(frame), 300);
          }
        } catch (err) {
          showError(`${t(lang, "renderFailed")}: ${err.message || err}`);
        }
      };
      frame.srcdoc = htmlDocument;
      frame.style.display = "block";
      window.onresize = () => {
        applyPptViewportFit(frame, currentReportFormat);
        syncFrameHeight(frame);
      };
      if (loading) loading.style.display = "none";
    };

    async function loadReport() {
      try {
        const data = await fetchReport(iterationId, lang);
        const htmlDocument = extractHtmlFromJsonLike(data.final_report_html || "") || String(data.final_report_html || "").trim();
        if (!htmlDocument) {
          showError(t(lang, "emptyReport"));
          return;
        }
        renderHtmlDocument(htmlDocument, data.report_meta?.report_format || "report");
      } catch (err) {
        showError(err.message || String(err));
      }
    }

    async function submitReportChat() {
      const message = String(chatInput?.value || "").trim();
      if (!message) {
        setChatStatus(t(lang, "chatEmpty"), true);
        return;
      }
      appendReportChatMessage(chatMessages, "user", message);
      const pendingMessage = appendReportChatMessage(chatMessages, "assistant", `${t(lang, "chatUpdating")} ${lang === "en" ? "Please wait." : "请稍候。"}`);
      if (chatSend) chatSend.disabled = true;
      if (chatInput) chatInput.disabled = true;
      setChatStatus(t(lang, "chatUpdating"));
      try {
        const data = await sendReportChat(iterationId, message, lang);
        const htmlDocument = extractHtmlFromJsonLike(data.html_document || "") || String(data.html_document || "").trim();
        if (!htmlDocument) {
          throw new Error(t(lang, "emptyReport"));
        }
        const reportFormat = data.report_meta?.report_format || data.report_format || currentReportFormat || "ppt";
        renderHtmlDocument(htmlDocument, reportFormat);
        if (chatInput) chatInput.value = "";
        const reply = data.assistant_message || t(lang, "chatUpdated");
        if (pendingMessage) {
          pendingMessage.textContent = reply;
          pendingMessage.className = "report-chat-message assistant";
        }
        setChatStatus(t(lang, "chatUpdated"));
      } catch (err) {
        const errorText = err.message || String(err);
        if (pendingMessage) {
          pendingMessage.textContent = errorText;
          pendingMessage.className = "report-chat-message assistant error";
        }
        setChatStatus(errorText, true);
      } finally {
        if (chatSend) chatSend.disabled = false;
        if (chatInput) {
          chatInput.disabled = false;
          chatInput.focus();
        }
      }
    }

    if (chatSend) chatSend.onclick = submitReportChat;
    if (chatInput) {
      chatInput.addEventListener("keydown", (event) => {
        if (event.key === "Enter" && !event.shiftKey) {
          event.preventDefault();
          submitReportChat();
        }
      });
    }

    await loadReport();
  }

  init();
})();
