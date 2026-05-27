const messageEl = document.getElementById("loginMessage");
const providerWrap = document.getElementById("oauthProviders");
const form = document.getElementById("localLoginForm");
const loginModeBtn = document.getElementById("loginModeBtn");
const registerModeBtn = document.getElementById("registerModeBtn");
const submitBtn = document.getElementById("submitLoginBtn");
const displayNameInput = document.getElementById("displayNameInput");

let authMode = "login";
let providers = {};

function tr(key, fallback, params = {}) {
  const text = window.i18n ? i18n.t(key, params) : key;
  return text && text !== key ? text : fallback;
}

function setMessage(text, isError = false) {
  messageEl.textContent = text || "";
  messageEl.style.color = isError ? "#dc2626" : "#64748b";
}

function setMode(mode) {
  authMode = mode === "register" ? "register" : "login";
  const isRegister = authMode === "register";
  displayNameInput.style.display = isRegister ? "" : "none";
  loginModeBtn.className = isRegister ? "btn btn-outline btn-block" : "btn btn-primary btn-block";
  registerModeBtn.className = isRegister ? "btn btn-primary btn-block" : "btn btn-outline btn-block";
  submitBtn.innerHTML = isRegister
    ? `<i class="fa-solid fa-user-plus"></i> <span>${tr("login_register_submit", "注册并登录")}</span>`
    : `<i class="fa-solid fa-right-to-bracket"></i> <span>${tr("login_submit", "登录")}</span>`;
  setMessage("");
}

async function loadProviders() {
  try {
    const res = await fetch("/api/auth/providers", { credentials: "include" });
    providers = await res.json();
    if (!providers.enabled) {
      window.location.href = "/dashboard";
      return;
    }
    if (!providers.local) {
      form.style.display = "none";
      setMessage(tr("login_local_disabled", "本地账号登录未启用。"), true);
    }
    if (!providers.registration) {
      registerModeBtn.style.display = "none";
    }
    const oauthProviders = Array.isArray(providers.oauth) ? providers.oauth : [];
    providerWrap.innerHTML = oauthProviders
      .map((provider) => `
        <a class="btn btn-outline btn-block" href="/api/auth/oauth/${encodeURIComponent(provider.name)}/login">
          <i class="fa-solid fa-key"></i> ${provider.label || provider.name}
        </a>
      `)
      .join("");
  } catch (err) {
    setMessage(tr("login_provider_failed", "认证配置加载失败。"), true);
  }
}

async function submitLocalAuth(event) {
  event.preventDefault();
  const username = document.getElementById("usernameInput").value.trim();
  const password = document.getElementById("passwordInput").value;
  const displayName = displayNameInput.value.trim();
  const endpoint = authMode === "register" ? "/api/auth/register" : "/api/auth/login";
  const payload = authMode === "register"
    ? { username, password, display_name: displayName || null }
    : { provider: "local", username, password };

  setMessage(authMode === "register" ? tr("login_registering", "正在注册...") : tr("login_signing_in", "正在登录..."));
  submitBtn.disabled = true;
  try {
    const res = await fetch(endpoint, {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json", "X-Language": i18n.lang },
      body: JSON.stringify(payload),
    });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.detail || tr("login_failed", "登录失败"));
    }
    localStorage.setItem("token", data.token);
    window.location.href = "/dashboard";
  } catch (err) {
    setMessage(err.message || tr("login_failed", "登录失败"), true);
  } finally {
    submitBtn.disabled = false;
  }
}

async function initLoginPage() {
  if (window.i18n) await i18n.init();
  loginModeBtn.addEventListener("click", () => setMode("login"));
  registerModeBtn.addEventListener("click", () => setMode("register"));
  form.addEventListener("submit", submitLocalAuth);
  await loadProviders();
  setMode("login");
}

initLoginPage();
