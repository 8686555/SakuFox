const messageEl = document.getElementById("loginMessage");
const providerWrap = document.getElementById("oauthProviders");
const form = document.getElementById("ldapLoginForm");

function setMessage(text, isError = false) {
  messageEl.textContent = text || "";
  messageEl.style.color = isError ? "#dc2626" : "#64748b";
}

async function loadProviders() {
  try {
    const res = await fetch("/api/auth/providers", { credentials: "include" });
    const data = await res.json();
    if (!data.ldap && form) {
      form.style.display = "none";
    }
    const providers = Array.isArray(data.oauth) ? data.oauth : [];
    providerWrap.innerHTML = providers
      .map((provider) => `
        <a class="btn btn-outline btn-block" href="/api/auth/oauth/${encodeURIComponent(provider.name)}/login">
          <i class="fa-solid fa-key"></i> ${provider.label || provider.name}
        </a>
      `)
      .join("");
    if (!data.ldap && providers.length === 0) {
      setMessage("No authentication provider is configured.", true);
    }
  } catch (err) {
    setMessage("Failed to load authentication providers.", true);
  }
}

form?.addEventListener("submit", async (event) => {
  event.preventDefault();
  setMessage("Signing in...");
  try {
    const username = document.getElementById("usernameInput").value.trim();
    const password = document.getElementById("passwordInput").value;
    const res = await fetch("/api/auth/login", {
      method: "POST",
      credentials: "include",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ provider: "ldap", username, password }),
    });
    const data = await res.json();
    if (!res.ok) {
      throw new Error(data.detail || "Login failed");
    }
    localStorage.setItem("token", data.token);
    window.location.href = "/dashboard";
  } catch (err) {
    setMessage(err.message || "Login failed", true);
  }
});

loadProviders();
