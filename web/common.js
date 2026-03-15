const API_BASE = "";

async function api(path, options = {}) {
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  // Mock token for backend compatibility, though backend now ignores it
  headers.Authorization = `Bearer mock_token`;
  
  const res = await fetch(API_BASE + path, { ...options, headers });
  
  if (!res.ok) {
    const err = await res.json();
    throw new Error(err.detail || "请求失败");
  }
  
  return res.json();
}
