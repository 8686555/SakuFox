import base64
import hashlib
import re
import time
import uuid
from urllib.parse import urlencode

import httpx
from fastapi import Header, HTTPException, Request

from app.config import load_config
from app.store import User, store


class AuthManager:
    def __init__(self) -> None:
        self.config = load_config()
        self.oauth_states: dict[str, dict] = {}

    def refresh_config(self) -> None:
        self.config = load_config()

    def providers(self) -> dict:
        self.refresh_config()
        auth_type = self.config.auth_type
        oauth_providers = self.config.oauth_providers or {}
        return {
            "auth_type": auth_type,
            "ldap": auth_type in {"mock", "ldap", "hybrid"},
            "oauth": [
                {"name": name, "label": provider.get("label") or name}
                for name, provider in oauth_providers.items()
            ],
            "mock": auth_type == "mock",
        }

    def issue_login(self, user: User) -> tuple[str, User]:
        token = store.issue_token(user)
        return token, user

    def login_with_ldap(self, username: str | None, password: str | None = None) -> tuple[str, User]:
        self.refresh_config()
        if self.config.auth_type == "mock":
            return self.issue_login(self._mock_user(username or "admin", provider="ldap"))
        if self.config.auth_type not in {"ldap", "hybrid"}:
            raise HTTPException(status_code=400, detail="LDAP login is not enabled")
        if not username or not password:
            raise HTTPException(status_code=401, detail="LDAP username and password are required")
        return self.issue_login(self._ldap_authenticate(username=username, password=password))

    def login_with_oauth(self, oauth_token: str | None, provider_name: str | None = None) -> tuple[str, User]:
        self.refresh_config()
        if self.config.auth_type == "mock":
            username = store.oauth_tokens.get(oauth_token or "", "admin")
            return self.issue_login(self._mock_user(username, provider="oauth"))
        if self.config.auth_type not in {"oauth", "hybrid"}:
            raise HTTPException(status_code=400, detail="OAuth login is not enabled")
        if not oauth_token:
            raise HTTPException(status_code=401, detail="OAuth access token is required")
        provider = self._get_oauth_provider(provider_name)
        return self.issue_login(self._oauth_user_from_access_token(provider_name or "oauth", provider, oauth_token))

    def start_oauth_login(self, provider_name: str, request: Request) -> str:
        self.refresh_config()
        if self.config.auth_type not in {"oauth", "hybrid"}:
            raise HTTPException(status_code=400, detail="OAuth login is not enabled")
        provider = self._get_oauth_provider(provider_name)
        authorization_endpoint = self._provider_value(provider, "authorization_endpoint")
        client_id = provider.get("client_id")
        if not authorization_endpoint or not client_id:
            raise HTTPException(status_code=500, detail=f"OAuth provider {provider_name} is missing authorization_endpoint or client_id")

        state = f"st_{uuid.uuid4().hex}"
        code_verifier = base64.urlsafe_b64encode(uuid.uuid4().bytes + uuid.uuid4().bytes).decode("ascii").rstrip("=")
        digest = hashlib.sha256(code_verifier.encode("ascii")).digest()
        code_challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        redirect_uri = provider.get("redirect_uri") or str(request.url_for("oauth_callback", provider_name=provider_name))
        self.oauth_states[state] = {
            "provider": provider_name,
            "expires_at": time.time() + max(60, int(self.config.oauth_state_ttl_seconds or 600)),
            "code_verifier": code_verifier,
            "redirect_uri": redirect_uri,
        }
        query = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": provider.get("scope") or "openid email profile",
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        return f"{authorization_endpoint}?{urlencode(query)}"

    def complete_oauth_callback(self, provider_name: str, code: str | None, state: str | None) -> tuple[str, User]:
        self.refresh_config()
        if not code or not state:
            raise HTTPException(status_code=400, detail="OAuth callback is missing code or state")
        state_data = self.oauth_states.pop(state, None)
        if not state_data or state_data.get("provider") != provider_name or state_data.get("expires_at", 0) < time.time():
            raise HTTPException(status_code=400, detail="OAuth state is invalid or expired")
        provider = self._get_oauth_provider(provider_name)
        token_endpoint = self._provider_value(provider, "token_endpoint")
        if not token_endpoint:
            raise HTTPException(status_code=500, detail=f"OAuth provider {provider_name} is missing token_endpoint")
        token_payload = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": state_data["redirect_uri"],
            "client_id": provider.get("client_id"),
            "client_secret": provider.get("client_secret"),
            "code_verifier": state_data["code_verifier"],
        }
        with httpx.Client(timeout=10.0) as client:
            token_res = client.post(token_endpoint, data={k: v for k, v in token_payload.items() if v})
            token_res.raise_for_status()
            token_data = token_res.json()
        access_token = token_data.get("access_token")
        if not access_token:
            raise HTTPException(status_code=401, detail="OAuth provider did not return an access token")
        return self.issue_login(self._oauth_user_from_access_token(provider_name, provider, access_token, token_data=token_data))

    def get_current_user(self, request: Request, authorization: str | None = None) -> User:
        token = self._extract_token(request, authorization)
        user = store.get_user_by_token(token) if token else None
        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")
        return user

    def logout(self, request: Request, authorization: str | None = None) -> bool:
        token = self._extract_token(request, authorization)
        return store.revoke_token(token) if token else False

    def _extract_token(self, request: Request, authorization: str | None) -> str:
        if authorization and authorization.lower().startswith("bearer "):
            return authorization.split(" ", 1)[1].strip()
        return request.cookies.get(self.config.auth_cookie_name, "")

    def _mock_user(self, username: str, provider: str) -> User:
        username = (username or "admin").strip()
        if username == "admin":
            profile = {"display_name": "管理员", "groups": ["admin", "finance", "hr"], "roles": ["Admin"]}
            user_id = "u_admin"
        else:
            profile = store.ldap_users.get(username)
            if not profile:
                raise HTTPException(status_code=401, detail="Unknown mock user")
            user_id = None
        return store.upsert_auth_user(
            user_id=user_id,
            username=username,
            display_name=profile.get("display_name") or username,
            provider=provider,
            groups=profile.get("groups") or [],
            roles=profile.get("roles"),
            external_id=f"{provider}:{username}",
        )

    def _ldap_authenticate(self, *, username: str, password: str) -> User:
        if not self.config.ldap_server_uri or not self.config.ldap_search_base:
            raise HTTPException(status_code=500, detail="LDAP is not configured")

        try:
            from ldap3 import ALL, Connection, Server
        except ImportError as exc:
            raise HTTPException(status_code=500, detail="ldap3 is required for LDAP authentication") from exc

        server = Server(self.config.ldap_server_uri, get_info=ALL)
        bind_user = self.config.ldap_bind_dn or None
        bind_password = self.config.ldap_bind_password or None
        search_filter = self.config.ldap_user_filter.format(username=username)
        attributes = [
            self.config.ldap_uid_field,
            self.config.ldap_display_name_field,
            self.config.ldap_email_field,
            self.config.ldap_group_field,
        ]
        try:
            with Connection(server, user=bind_user, password=bind_password, auto_bind=True) as conn:
                found = conn.search(self.config.ldap_search_base, search_filter, attributes=attributes)
                if not found or not conn.entries:
                    raise HTTPException(status_code=401, detail="LDAP user not found")
                entry = conn.entries[0]
                user_dn = entry.entry_dn
                attrs = entry.entry_attributes_as_dict
            with Connection(server, user=user_dn, password=password, auto_bind=True):
                pass
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=401, detail="LDAP authentication failed") from exc

        groups = self._extract_ldap_groups(attrs.get(self.config.ldap_group_field) or [])
        username_value = self._first_attr(attrs, self.config.ldap_uid_field) or username
        return store.upsert_auth_user(
            username=username_value,
            display_name=self._first_attr(attrs, self.config.ldap_display_name_field) or username_value,
            email=self._first_attr(attrs, self.config.ldap_email_field) or "",
            external_id=user_dn,
            provider="ldap",
            groups=groups,
        )

    def _extract_ldap_groups(self, values) -> list[str]:
        if isinstance(values, str):
            values = [values]
        groups: list[str] = []
        pattern = self.config.ldap_group_name_regex
        for raw in values or []:
            text = str(raw)
            match = re.search(pattern, text) if pattern else None
            group = match.group(1) if match else text
            group = group.strip()
            if group and group not in groups:
                groups.append(group)
        return groups

    def _oauth_user_from_access_token(self, provider_name: str, provider: dict, access_token: str, token_data: dict | None = None) -> User:
        userinfo_endpoint = self._provider_value(provider, "userinfo_endpoint")
        claims = dict(token_data or {})
        if userinfo_endpoint:
            with httpx.Client(timeout=10.0) as client:
                userinfo_res = client.get(userinfo_endpoint, headers={"Authorization": f"Bearer {access_token}"})
                userinfo_res.raise_for_status()
                claims.update(userinfo_res.json())
        groups_claim = provider.get("groups_claim") or "groups"
        roles_claim = provider.get("roles_claim") or "roles"
        username = claims.get(provider.get("username_claim") or "preferred_username") or claims.get("email") or claims.get("sub")
        if not username:
            raise HTTPException(status_code=401, detail="OAuth user profile is missing username")
        groups = claims.get(groups_claim) or []
        roles = claims.get(roles_claim) or None
        if isinstance(groups, str):
            groups = [groups]
        if isinstance(roles, str):
            roles = [roles]
        return store.upsert_auth_user(
            username=str(username),
            display_name=claims.get("name") or str(username),
            email=claims.get("email") or "",
            external_id=claims.get("sub") or str(username),
            provider=f"oauth:{provider_name}",
            groups=[str(item) for item in groups],
            roles=[str(item) for item in roles] if roles else None,
        )

    def _get_oauth_provider(self, provider_name: str | None) -> dict:
        providers = self.config.oauth_providers or {}
        if not provider_name:
            if len(providers) == 1:
                provider_name = next(iter(providers))
            else:
                raise HTTPException(status_code=400, detail="OAuth provider is required")
        provider = providers.get(provider_name)
        if not provider:
            raise HTTPException(status_code=404, detail=f"OAuth provider {provider_name} not found")
        provider = dict(provider)
        metadata_url = provider.get("server_metadata_url")
        if metadata_url and not provider.get("metadata"):
            try:
                with httpx.Client(timeout=10.0) as client:
                    metadata_res = client.get(str(metadata_url))
                    metadata_res.raise_for_status()
                    provider["metadata"] = metadata_res.json()
            except Exception as exc:
                raise HTTPException(status_code=500, detail=f"Failed to load OAuth metadata for {provider_name}") from exc
        return provider

    def _provider_value(self, provider: dict, key: str) -> str:
        if provider.get(key):
            return str(provider[key])
        metadata = provider.get("metadata") or {}
        return str(metadata.get(key) or "")

    def _first_attr(self, attrs: dict, key: str) -> str:
        value = attrs.get(key)
        if isinstance(value, list):
            return str(value[0]) if value else ""
        return str(value or "")


auth_manager = AuthManager()


def login_with_ldap(username: str | None, password: str | None = None) -> tuple[str, User]:
    return auth_manager.login_with_ldap(username, password)


def login_with_oauth(oauth_token: str | None, provider_name: str | None = None) -> tuple[str, User]:
    return auth_manager.login_with_oauth(oauth_token, provider_name)


def get_current_user(request: Request, authorization: str | None = Header(default=None)) -> User:
    return auth_manager.get_current_user(request, authorization)
