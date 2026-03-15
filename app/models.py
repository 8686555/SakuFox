from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    provider: str = Field(pattern="^(ldap|oauth)$")
    username: str | None = None
    oauth_token: str | None = None


class IterateRequest(BaseModel):
    """Start or continue an analysis iteration."""
    sandbox_id: str
    message: str
    session_id: str | None = None
    provider: str | None = Field(default=None, pattern="^(openai|anthropic|mock)$")
    model: str | None = None
    selected_tables: list[str] | None = None
    selected_files: list[str] | None = None
    hypothesis_id: str | None = None  # pick a hypothesis from previous iteration


class FeedbackRequest(BaseModel):
    """User feedback or business knowledge supplement."""
    sandbox_id: str
    session_id: str
    feedback: str
    is_business_knowledge: bool = False


class SaveSkillRequest(BaseModel):
    proposal_id: str
    name: str

class CreateSandboxRequest(BaseModel):
    name: str
    allowed_groups: list[str]

class RenameSandboxRequest(BaseModel):
    name: str
