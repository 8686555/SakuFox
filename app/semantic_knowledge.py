import json
import hashlib
import mimetypes
import re
import shutil
import subprocess
import uuid
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from xml.etree import ElementTree

from sqlalchemy import delete, desc, select

from app.config import load_config
from app.db_models import (
    DBDocumentChunk,
    DBDocumentParseJob,
    DBKnowledgeLintRun,
    DBKnowledgeReviewItem,
    DBKnowledgeWikiPage,
    DBKnowledgeWikiRevision,
    DBUploadedDocument,
)
from app.knowledge_assets import (
    _cosine_similarity,
    _dedupe_non_empty,
    _embed_text,
    _extract_keywords,
    _hash_text,
)


SEMANTIC_PAGE_TYPES = {
    "business_term",
    "metric",
    "dimension",
    "table_semantics",
    "field_semantics",
    "join_rule",
    "filter_rule",
    "analysis_experience",
}

DOCUMENT_EXTENSIONS = {".txt", ".md", ".json", ".log", ".sql", ".yaml", ".yml", ".pdf", ".docx"}
TEXT_EXTENSIONS = {".txt", ".md", ".json", ".log", ".sql", ".yaml", ".yml"}
MINERU_EXTENSIONS = {".pdf", ".docx"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_slug(value: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^\w\u4e00-\u9fff]+", "-", text, flags=re.UNICODE).strip("-")
    return text[:160] or f"page-{uuid.uuid4().hex[:8]}"


def _read_text_file(path: str) -> str:
    file_path = Path(str(path or ""))
    if not file_path.exists():
        return ""
    for encoding in ("utf-8", "utf-8-sig", "gbk", "latin-1"):
        try:
            return file_path.read_text(encoding=encoding)
        except Exception:
            continue
    try:
        return file_path.read_bytes().decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _extract_docx_text(path: str) -> str:
    try:
        with zipfile.ZipFile(path) as archive:
            xml_text = archive.read("word/document.xml")
    except Exception:
        return ""
    try:
        root = ElementTree.fromstring(xml_text)
    except Exception:
        return ""
    namespaces = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    paragraphs: list[str] = []
    for paragraph in root.findall(".//w:p", namespaces):
        texts = [node.text or "" for node in paragraph.findall(".//w:t", namespaces)]
        line = "".join(texts).strip()
        if line:
            paragraphs.append(line)
    return "\n\n".join(paragraphs)


def _split_document_chunks(text: str, chunk_size: int = 1100, overlap: int = 180) -> list[dict[str, Any]]:
    normalized = str(text or "").strip()
    if not normalized:
        return []
    sections: list[tuple[str, str]] = []
    current_heading = ""
    current_lines: list[str] = []
    for raw_line in normalized.splitlines():
        line = raw_line.rstrip()
        heading_match = re.match(r"^\s{0,3}#{1,6}\s+(.+)$", line)
        if heading_match:
            if current_lines:
                sections.append((current_heading, "\n".join(current_lines).strip()))
            current_heading = heading_match.group(1).strip()
            current_lines = [line]
        else:
            current_lines.append(line)
    if current_lines:
        sections.append((current_heading, "\n".join(current_lines).strip()))
    if not sections:
        sections = [("", normalized)]

    output: list[dict[str, Any]] = []
    for heading, section_text in sections:
        if len(section_text) <= chunk_size:
            if section_text.strip():
                output.append({"text": section_text.strip(), "heading": heading})
            continue
        cursor = 0
        step = max(chunk_size - overlap, 1)
        while cursor < len(section_text):
            chunk_text = section_text[cursor : cursor + chunk_size].strip()
            if chunk_text:
                output.append({"text": chunk_text, "heading": heading})
            cursor += step
    return output


def _record_parse_job(store: Any, *, document_id: str, status: str, message: str, stats: dict | None = None) -> str:
    now = _now_iso()
    job_id = f"dpj_{uuid.uuid4().hex[:12]}"
    with store.SessionFactory() as sess:
        sess.add(
            DBDocumentParseJob(
                job_id=job_id,
                document_id=document_id,
                status=status,
                message=message,
                stats=stats or {},
                created_at=now,
                updated_at=now,
            )
        )
        sess.commit()
    return job_id


def _update_document_status(
    store: Any,
    document_id: str,
    *,
    status: str,
    parser: str | None = None,
    error: str = "",
    markdown_path: str = "",
    json_path: str = "",
    metadata: dict | None = None,
) -> None:
    now = _now_iso()
    with store.SessionFactory() as sess:
        doc = sess.get(DBUploadedDocument, document_id)
        if not doc:
            return
        doc.parse_status = status
        if parser is not None:
            doc.parser = parser
        doc.parse_error = error
        if markdown_path:
            doc.parsed_markdown_path = markdown_path
        if json_path:
            doc.parsed_json_path = json_path
        if metadata is not None:
            current = dict(doc.metadata_json or {})
            current.update(metadata)
            doc.metadata_json = current
        doc.updated_at = now
        sess.commit()


def _replace_document_chunks(store: Any, document_id: str, sandbox_id: str, chunks: list[dict[str, Any]]) -> None:
    now = _now_iso()
    with store.SessionFactory() as sess:
        sess.execute(delete(DBDocumentChunk).where(DBDocumentChunk.document_id == document_id))
        for idx, chunk in enumerate(chunks):
            text = str(chunk.get("text") or "").strip()
            if not text:
                continue
            locator = f"document://{document_id}#chunk={idx}"
            sess.add(
                DBDocumentChunk(
                    chunk_id=f"dc_{uuid.uuid4().hex[:12]}",
                    document_id=document_id,
                    sandbox_id=sandbox_id,
                    chunk_index=idx,
                    chunk_text=text,
                    heading=str(chunk.get("heading") or "").strip(),
                    page_number=chunk.get("page_number"),
                    locator=locator,
                    keywords=_extract_keywords(text),
                    embedding=_embed_text(text),
                    content_hash=_hash_text(text),
                    created_at=now,
                    updated_at=now,
                )
            )
        sess.commit()


def _infer_review_page_type(text: str) -> str:
    lowered = str(text or "").lower()
    if any(token in lowered for token in ("指标", "口径", "率", "metric", "kpi", "formula", "公式")):
        return "metric"
    if any(token in lowered for token in ("join", "关联", "连接")):
        return "join_rule"
    if any(token in lowered for token in ("过滤", "排除", "where", "filter")):
        return "filter_rule"
    if any(token in lowered for token in ("字段", "field", "column")):
        return "field_semantics"
    return "business_term"


def _make_review_title(filename: str, chunk: dict[str, Any], page_type: str) -> str:
    heading = str(chunk.get("heading") or "").strip()
    if heading:
        return heading[:120]
    text = str(chunk.get("text") or "").strip()
    line = next((item.strip() for item in text.splitlines() if item.strip()), "")
    return (line or f"{filename} {page_type}")[:120]


def _ensure_review_items_for_document(store: Any, document_id: str, created_by: str | None = None, limit: int = 8) -> None:
    with store.SessionFactory() as sess:
        doc = sess.get(DBUploadedDocument, document_id)
        if not doc:
            return
        existing = sess.execute(
            select(DBKnowledgeReviewItem).where(DBKnowledgeReviewItem.source_document_id == document_id)
        ).scalars().first()
        if existing:
            return
        chunks = sess.execute(
            select(DBDocumentChunk)
            .where(DBDocumentChunk.document_id == document_id)
            .order_by(DBDocumentChunk.chunk_index)
        ).scalars().all()
        now = _now_iso()
        for chunk in chunks[:limit]:
            page_type = _infer_review_page_type(chunk.chunk_text or "")
            title = _make_review_title(doc.filename or "", {"text": chunk.chunk_text, "heading": chunk.heading}, page_type)
            payload = {
                "page_type": page_type,
                "title": title,
                "canonical_name": title,
                "aliases": _dedupe_non_empty([title]),
                "body_markdown": (chunk.chunk_text or "").strip(),
                "frontmatter": {
                    "tables": [],
                    "fields": [],
                    "metric_formula": "",
                    "sql_fragments": [],
                    "source_locator": chunk.locator,
                },
                "confidence": 0.45,
                "source_document_id": document_id,
                "source_chunk_id": chunk.chunk_id,
            }
            sess.add(
                DBKnowledgeReviewItem(
                    review_id=f"kri_{uuid.uuid4().hex[:12]}",
                    sandbox_id=doc.sandbox_id,
                    item_type="semantic_page",
                    status="pending",
                    title=title,
                    proposed_payload=payload,
                    source_document_id=document_id,
                    source_chunk_id=chunk.chunk_id,
                    message="LLM-ready semantic draft generated from uploaded document chunk.",
                    created_by=created_by,
                    created_at=now,
                    updated_at=now,
                )
            )
        sess.commit()


def _find_mineru_outputs(output_dir: Path) -> tuple[str, str]:
    markdown = ""
    json_path = ""
    for path in output_dir.rglob("*"):
        if path.is_file() and path.suffix.lower() == ".md" and not markdown:
            markdown = str(path)
        elif path.is_file() and path.suffix.lower() == ".json" and not json_path:
            json_path = str(path)
    return markdown, json_path


def _run_mineru(source_path: str, output_dir: Path, timeout_seconds: int) -> tuple[str, str, str]:
    cfg = load_config()
    command_text = str(getattr(cfg, "mineru_command", "") or "mineru").strip()
    executable = command_text.split()[0] if command_text else "mineru"
    if not shutil.which(executable):
        raise RuntimeError(f"MinerU command not found: {executable}")
    output_dir.mkdir(parents=True, exist_ok=True)
    command = command_text.split() + ["-p", source_path, "-o", str(output_dir)]
    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=max(timeout_seconds, 1),
        check=False,
    )
    if completed.returncode != 0:
        message = (completed.stderr or completed.stdout or "MinerU failed").strip()
        raise RuntimeError(message[:1000])
    markdown_path, json_path = _find_mineru_outputs(output_dir)
    if not markdown_path:
        raise RuntimeError("MinerU finished but no markdown output was found")
    return _read_text_file(markdown_path), markdown_path, json_path


def parse_uploaded_document(store: Any, document_id: str, created_by: str | None = None) -> None:
    with store.SessionFactory() as sess:
        doc = sess.get(DBUploadedDocument, document_id)
        if not doc:
            return
        source_path = str(doc.source_path or "")
        filename = str(doc.filename or "")
        sandbox_id = str(doc.sandbox_id or "")
    suffix = Path(filename or source_path).suffix.lower()
    _record_parse_job(store, document_id=document_id, status="running", message="Document parsing started")
    try:
        text = ""
        markdown_path = ""
        json_path = ""
        parser = "text"
        if suffix in TEXT_EXTENSIONS:
            text = _read_text_file(source_path)
        elif suffix == ".docx":
            parser = "mineru_local"
            try:
                output_dir = Path("uploads") / "parsed" / document_id
                text, markdown_path, json_path = _run_mineru(
                    source_path,
                    output_dir,
                    int(getattr(load_config(), "document_parse_timeout_seconds", 120) or 120),
                )
            except Exception:
                parser = "docx_fallback"
                text = _extract_docx_text(source_path)
                if not text:
                    raise
        elif suffix == ".pdf":
            parser = "mineru_local"
            output_dir = Path("uploads") / "parsed" / document_id
            text, markdown_path, json_path = _run_mineru(
                source_path,
                output_dir,
                int(getattr(load_config(), "document_parse_timeout_seconds", 120) or 120),
            )
        else:
            text = _read_text_file(source_path)
        chunks = _split_document_chunks(text)
        if not chunks:
            raise RuntimeError("No readable text was extracted from the document")
        _replace_document_chunks(store, document_id, sandbox_id, chunks)
        _update_document_status(
            store,
            document_id,
            status="success",
            parser=parser,
            markdown_path=markdown_path,
            json_path=json_path,
            metadata={"chunk_count": len(chunks), "char_count": len(text)},
        )
        _record_parse_job(
            store,
            document_id=document_id,
            status="success",
            message="Document parsing finished",
            stats={"chunk_count": len(chunks), "char_count": len(text)},
        )
        _ensure_review_items_for_document(store, document_id, created_by=created_by)
        try:
            store.refresh_knowledge_assets()
        except Exception:
            pass
    except Exception as exc:
        _update_document_status(store, document_id, status="failed", error=str(exc), parser="mineru_local" if suffix in MINERU_EXTENSIONS else "text")
        _record_parse_job(store, document_id=document_id, status="failed", message=str(exc), stats={})


def register_uploaded_document(
    store: Any,
    *,
    sandbox_id: str,
    owner_id: str | None,
    filename: str,
    source_path: str,
    content_type: str | None = None,
    parse_immediately: bool = True,
) -> dict[str, Any]:
    now = _now_iso()
    suffix = Path(filename or source_path).suffix.lower()
    parser = "mineru_local" if suffix in MINERU_EXTENSIONS else "text"
    document_id = f"doc_{uuid.uuid4().hex[:12]}"
    if Path(source_path).exists():
        content_hash = hashlib.sha256(Path(source_path).read_bytes()).hexdigest()
    else:
        content_hash = _hash_text(source_path)
    with store.SessionFactory() as sess:
        sess.add(
            DBUploadedDocument(
                document_id=document_id,
                sandbox_id=sandbox_id,
                owner_id=owner_id,
                filename=filename,
                source_path=source_path,
                content_type=content_type or mimetypes.guess_type(filename)[0] or "application/octet-stream",
                parser=parser,
                parse_status="pending",
                parse_error="",
                parsed_markdown_path="",
                parsed_json_path="",
                content_hash=content_hash,
                metadata_json={"extension": suffix},
                created_at=now,
                updated_at=now,
            )
        )
        sess.commit()
    _record_parse_job(store, document_id=document_id, status="pending", message="Document queued for parsing")
    if parse_immediately:
        parse_uploaded_document(store, document_id, created_by=owner_id)
    return get_uploaded_document(store, document_id) or {"document_id": document_id}


def _document_to_dict(row: DBUploadedDocument) -> dict[str, Any]:
    return {
        "document_id": row.document_id,
        "sandbox_id": row.sandbox_id,
        "owner_id": row.owner_id,
        "filename": row.filename,
        "source_path": row.source_path,
        "content_type": row.content_type,
        "parser": row.parser,
        "parse_status": row.parse_status,
        "parse_error": row.parse_error or "",
        "parsed_markdown_path": row.parsed_markdown_path or "",
        "parsed_json_path": row.parsed_json_path or "",
        "content_hash": row.content_hash or "",
        "metadata": row.metadata_json or {},
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def get_uploaded_document(store: Any, document_id: str) -> dict[str, Any] | None:
    with store.SessionFactory() as sess:
        row = sess.get(DBUploadedDocument, document_id)
        return _document_to_dict(row) if row else None


def list_uploaded_documents(store: Any, sandbox_id: str | None = None) -> list[dict[str, Any]]:
    with store.SessionFactory() as sess:
        stmt = select(DBUploadedDocument)
        if sandbox_id:
            stmt = stmt.where(DBUploadedDocument.sandbox_id == sandbox_id)
        rows = sess.execute(stmt.order_by(desc(DBUploadedDocument.updated_at))).scalars().all()
    return [_document_to_dict(row) for row in rows]


def get_document_chunks(store: Any, document_id: str) -> list[dict[str, Any]]:
    with store.SessionFactory() as sess:
        rows = sess.execute(
            select(DBDocumentChunk).where(DBDocumentChunk.document_id == document_id).order_by(DBDocumentChunk.chunk_index)
        ).scalars().all()
    return [
        {
            "chunk_id": row.chunk_id,
            "document_id": row.document_id,
            "sandbox_id": row.sandbox_id,
            "chunk_index": row.chunk_index,
            "chunk_text": row.chunk_text,
            "heading": row.heading or "",
            "page_number": row.page_number,
            "locator": row.locator,
        }
        for row in rows
    ]


def publish_review_item(store: Any, review_id: str, user_id: str | None = None) -> dict[str, Any]:
    now = _now_iso()
    with store.SessionFactory() as sess:
        review = sess.get(DBKnowledgeReviewItem, review_id)
        if not review:
            raise ValueError("Review item not found")
        if review.status != "pending":
            raise ValueError("Review item is already resolved")
        payload = dict(review.proposed_payload or {})
        page_type = str(payload.get("page_type") or review.item_type or "business_term")
        if page_type not in SEMANTIC_PAGE_TYPES:
            page_type = "business_term"
        title = str(payload.get("title") or review.title or page_type).strip()
        slug_base = _safe_slug(str(payload.get("canonical_name") or title))
        slug = slug_base
        counter = 2
        while sess.execute(
            select(DBKnowledgeWikiPage).where(
                DBKnowledgeWikiPage.sandbox_id == review.sandbox_id,
                DBKnowledgeWikiPage.slug == slug,
            )
        ).scalars().first():
            slug = f"{slug_base}-{counter}"
            counter += 1
        page = DBKnowledgeWikiPage(
            page_id=f"kwp_{uuid.uuid4().hex[:12]}",
            sandbox_id=review.sandbox_id,
            page_type=page_type,
            slug=slug,
            title=title,
            canonical_name=str(payload.get("canonical_name") or title).strip(),
            aliases=payload.get("aliases") or [],
            body_markdown=str(payload.get("body_markdown") or "").strip(),
            frontmatter_json=payload.get("frontmatter") or {},
            source_document_id=payload.get("source_document_id") or review.source_document_id,
            source_chunk_id=payload.get("source_chunk_id") or review.source_chunk_id,
            source_asset_ids=payload.get("source_asset_ids") or ([review.source_asset_id] if review.source_asset_id else []),
            confidence=float(payload.get("confidence") or 0.5),
            status="published",
            created_by=user_id or review.created_by,
            published_at=now,
            created_at=now,
            updated_at=now,
        )
        sess.add(page)
        review.status = "published"
        review.resolved_by = user_id
        review.updated_at = now
        sess.add(
            DBKnowledgeWikiRevision(
                revision_id=f"kwr_{uuid.uuid4().hex[:12]}",
                page_id=page.page_id,
                action="publish",
                before_json={},
                after_json=_wiki_page_payload(page),
                reason=f"Published from review item {review_id}",
                operator_id=user_id,
                created_at=now,
            )
        )
        sess.commit()
        return _wiki_page_payload(page)


def dismiss_review_item(store: Any, review_id: str, user_id: str | None = None) -> dict[str, Any]:
    now = _now_iso()
    with store.SessionFactory() as sess:
        review = sess.get(DBKnowledgeReviewItem, review_id)
        if not review:
            raise ValueError("Review item not found")
        review.status = "dismissed"
        review.resolved_by = user_id
        review.updated_at = now
        sess.commit()
        return _review_item_payload(review)


def _review_item_payload(row: DBKnowledgeReviewItem) -> dict[str, Any]:
    return {
        "review_id": row.review_id,
        "sandbox_id": row.sandbox_id,
        "item_type": row.item_type,
        "status": row.status,
        "title": row.title,
        "proposed_payload": row.proposed_payload or {},
        "source_document_id": row.source_document_id,
        "source_chunk_id": row.source_chunk_id,
        "source_asset_id": row.source_asset_id,
        "message": row.message or "",
        "created_by": row.created_by,
        "resolved_by": row.resolved_by,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def list_review_items(store: Any, sandbox_id: str | None = None, status: str | None = None) -> list[dict[str, Any]]:
    with store.SessionFactory() as sess:
        stmt = select(DBKnowledgeReviewItem)
        if sandbox_id:
            stmt = stmt.where(DBKnowledgeReviewItem.sandbox_id == sandbox_id)
        if status:
            stmt = stmt.where(DBKnowledgeReviewItem.status == status)
        rows = sess.execute(stmt.order_by(desc(DBKnowledgeReviewItem.created_at))).scalars().all()
    return [_review_item_payload(row) for row in rows]


def _wiki_page_payload(row: DBKnowledgeWikiPage) -> dict[str, Any]:
    return {
        "page_id": row.page_id,
        "sandbox_id": row.sandbox_id,
        "page_type": row.page_type,
        "slug": row.slug,
        "title": row.title,
        "canonical_name": row.canonical_name,
        "aliases": row.aliases or [],
        "body_markdown": row.body_markdown or "",
        "frontmatter": row.frontmatter_json or {},
        "source_document_id": row.source_document_id,
        "source_chunk_id": row.source_chunk_id,
        "source_asset_ids": row.source_asset_ids or [],
        "confidence": row.confidence,
        "status": row.status,
        "created_by": row.created_by,
        "published_at": row.published_at,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
    }


def list_wiki_pages(store: Any, sandbox_id: str | None = None, status: str | None = None, page_type: str | None = None) -> list[dict[str, Any]]:
    with store.SessionFactory() as sess:
        stmt = select(DBKnowledgeWikiPage)
        if sandbox_id:
            stmt = stmt.where(DBKnowledgeWikiPage.sandbox_id == sandbox_id)
        if status:
            stmt = stmt.where(DBKnowledgeWikiPage.status == status)
        if page_type:
            stmt = stmt.where(DBKnowledgeWikiPage.page_type == page_type)
        rows = sess.execute(stmt.order_by(desc(DBKnowledgeWikiPage.updated_at))).scalars().all()
    return [_wiki_page_payload(row) for row in rows]


def get_wiki_page(store: Any, slug_or_id: str, sandbox_id: str | None = None) -> dict[str, Any] | None:
    with store.SessionFactory() as sess:
        row = sess.get(DBKnowledgeWikiPage, slug_or_id)
        if row is None:
            stmt = select(DBKnowledgeWikiPage).where(DBKnowledgeWikiPage.slug == slug_or_id)
            if sandbox_id:
                stmt = stmt.where(DBKnowledgeWikiPage.sandbox_id == sandbox_id)
            row = sess.execute(stmt).scalars().first()
        return _wiki_page_payload(row) if row else None


def create_experience_review_item(store: Any, skill: dict) -> None:
    layers = skill.get("layers") or {}
    source = (layers.get("context_snapshot") or {}).get("source") or {}
    sandbox_id = str(source.get("sandbox_id") or "").strip()
    if not sandbox_id:
        return
    skill_id = str(skill.get("skill_id") or "").strip()
    if not skill_id:
        return
    with store.SessionFactory() as sess:
        existing_page = next(
            (
                page
                for page in sess.execute(
                    select(DBKnowledgeWikiPage).where(DBKnowledgeWikiPage.page_type == "analysis_experience")
                ).scalars().all()
                if skill_id in (page.source_asset_ids or [])
            ),
            None,
        )
        existing_review = sess.execute(
            select(DBKnowledgeReviewItem).where(DBKnowledgeReviewItem.source_asset_id == skill_id)
        ).scalars().first()
        if existing_page or existing_review:
            return
        knowledge_lines = [str(item).strip() for item in (layers.get("knowledge") or []) if str(item).strip()]
        body = "\n".join(f"- {item}" for item in knowledge_lines) or str(skill.get("description") or "").strip()
        now = _now_iso()
        payload = {
            "page_type": "analysis_experience",
            "title": str(skill.get("name") or skill_id),
            "canonical_name": str(skill.get("name") or skill_id),
            "aliases": [str(skill.get("name") or skill_id)],
            "body_markdown": body,
            "frontmatter": {
                "tags": skill.get("tags") or [],
                "tables": [item.get("table") for item in (layers.get("tables") or []) if isinstance(item, dict)],
                "source_session_id": source.get("session_id", ""),
                "source_proposal_id": source.get("proposal_id", ""),
            },
            "confidence": 0.7,
            "source_asset_ids": [skill_id],
        }
        sess.add(
            DBKnowledgeReviewItem(
                review_id=f"kri_{uuid.uuid4().hex[:12]}",
                sandbox_id=sandbox_id,
                item_type="semantic_page",
                status="pending",
                title=str(skill.get("name") or skill_id),
                proposed_payload=payload,
                source_asset_id=skill_id,
                message="Experience semantic draft generated from published skill.",
                created_by=str(skill.get("owner_id") or "") or None,
                created_at=now,
                updated_at=now,
            )
        )
        sess.commit()


def _score_text(query_text: str, candidate_text: str) -> tuple[float, float, float]:
    query_keywords = set(_extract_keywords(query_text))
    candidate_keywords = set(_extract_keywords(candidate_text))
    overlap = len(query_keywords.intersection(candidate_keywords))
    keyword_score = overlap / max(len(query_keywords), 1)
    vector_score = _cosine_similarity(_embed_text(query_text), _embed_text(candidate_text))
    score = round((0.4 * keyword_score) + (0.6 * max(vector_score, 0.0)), 6)
    return score, round(keyword_score, 6), round(vector_score, 6)


def query_semantic_layer(store: Any, query: str, sandbox_id: str, top_k: int = 5) -> list[dict[str, Any]]:
    query_text = str(query or "").strip()
    if not query_text:
        return []
    with store.SessionFactory() as sess:
        rows = sess.execute(
            select(DBKnowledgeWikiPage).where(
                DBKnowledgeWikiPage.sandbox_id == sandbox_id,
                DBKnowledgeWikiPage.status == "published",
                DBKnowledgeWikiPage.page_type != "analysis_experience",
            )
        ).scalars().all()
    scored: list[dict[str, Any]] = []
    for page in rows:
        text = " ".join(
            [
                str(page.title or ""),
                str(page.canonical_name or ""),
                " ".join(str(item) for item in (page.aliases or [])),
                str(page.body_markdown or ""),
                json.dumps(page.frontmatter_json or {}, ensure_ascii=False),
            ]
        )
        score, keyword_score, vector_score = _score_text(query_text, text)
        if score <= 0 and keyword_score <= 0:
            continue
        scored.append(
            {
                "kind": "semantic",
                "page_id": page.page_id,
                "asset_id": page.page_id,
                "asset_type": "semantic_wiki",
                "page_type": page.page_type,
                "title": page.title,
                "chunk_id": page.source_chunk_id or "",
                "snippet": (page.body_markdown or "")[:500],
                "score": score,
                "source_ref": page.slug,
                "source_path": "",
                "full_document_locator": f"wiki://{page.slug}",
                "keyword_score": keyword_score,
                "vector_score": vector_score,
                "frontmatter": page.frontmatter_json or {},
            }
        )
    scored.sort(key=lambda item: (item["score"], item["keyword_score"], item["vector_score"]), reverse=True)
    return scored[: max(min(int(top_k or 5), 20), 1)]


def query_experience_index(store: Any, query: str, sandbox_id: str, top_k: int = 5) -> list[dict[str, Any]]:
    query_text = str(query or "").strip()
    if not query_text:
        return []
    with store.SessionFactory() as sess:
        rows = sess.execute(
            select(DBKnowledgeWikiPage).where(
                DBKnowledgeWikiPage.sandbox_id == sandbox_id,
                DBKnowledgeWikiPage.status == "published",
                DBKnowledgeWikiPage.page_type == "analysis_experience",
            )
        ).scalars().all()
    scored: list[dict[str, Any]] = []
    for page in rows:
        text = f"{page.title or ''}\n{page.body_markdown or ''}\n{json.dumps(page.frontmatter_json or {}, ensure_ascii=False)}"
        score, keyword_score, vector_score = _score_text(query_text, text)
        if score <= 0 and keyword_score <= 0:
            continue
        scored.append(
            {
                "kind": "experience",
                "page_id": page.page_id,
                "asset_id": page.page_id,
                "asset_type": "analysis_experience",
                "page_type": page.page_type,
                "title": page.title,
                "chunk_id": page.source_chunk_id or "",
                "snippet": (page.body_markdown or "")[:500],
                "score": score,
                "source_ref": page.slug,
                "source_path": "",
                "full_document_locator": f"wiki://{page.slug}",
                "keyword_score": keyword_score,
                "vector_score": vector_score,
                "frontmatter": page.frontmatter_json or {},
            }
        )
    scored.sort(key=lambda item: (item["score"], item["keyword_score"], item["vector_score"]), reverse=True)
    return scored[: max(min(int(top_k or 5), 20), 1)]


def query_document_sources(store: Any, query: str, sandbox_id: str, top_k: int = 5) -> list[dict[str, Any]]:
    query_text = str(query or "").strip()
    if not query_text:
        return []
    with store.SessionFactory() as sess:
        chunks = sess.execute(
            select(DBDocumentChunk).where(DBDocumentChunk.sandbox_id == sandbox_id)
        ).scalars().all()
        docs = {
            row.document_id: row
            for row in sess.execute(
                select(DBUploadedDocument).where(DBUploadedDocument.sandbox_id == sandbox_id)
            ).scalars().all()
        }
    scored: list[dict[str, Any]] = []
    for chunk in chunks:
        doc = docs.get(chunk.document_id)
        if not doc or doc.parse_status != "success":
            continue
        score, keyword_score, vector_score = _score_text(query_text, chunk.chunk_text or "")
        if score <= 0 and keyword_score <= 0:
            continue
        scored.append(
            {
                "kind": "document",
                "document_id": chunk.document_id,
                "asset_id": chunk.document_id,
                "asset_type": "uploaded_document",
                "title": doc.filename,
                "chunk_id": chunk.chunk_id,
                "snippet": (chunk.chunk_text or "")[:500],
                "score": score,
                "source_ref": doc.filename,
                "source_path": doc.source_path or "",
                "full_document_locator": chunk.locator,
                "keyword_score": keyword_score,
                "vector_score": vector_score,
                "heading": chunk.heading or "",
                "page_number": chunk.page_number,
            }
        )
    scored.sort(key=lambda item: (item["score"], item["keyword_score"], item["vector_score"]), reverse=True)
    return scored[: max(min(int(top_k or 5), 20), 1)]


def query_unified_knowledge_index(store: Any, query: str, sandbox_id: str, top_k: int = 5) -> list[dict[str, Any]]:
    from app.knowledge_assets import search_knowledge_index as legacy_asset_search

    limit = max(min(int(top_k or 5), 20), 1)
    semantic_hits = query_semantic_layer(store, query, sandbox_id, top_k=limit)
    experience_hits = query_experience_index(store, query, sandbox_id, top_k=limit)
    document_hits = query_document_sources(store, query, sandbox_id, top_k=limit)
    legacy_hits = legacy_asset_search(store, query=query, sandbox_id=sandbox_id, top_k=limit)
    for hit in legacy_hits:
        hit.setdefault("kind", "asset")
    merged = semantic_hits + experience_hits + document_hits + legacy_hits
    merged.sort(key=lambda item: (float(item.get("score") or 0), float(item.get("keyword_score") or 0), float(item.get("vector_score") or 0)), reverse=True)
    return merged[:limit]


def lint_semantic_wiki(store: Any, sandbox_id: str | None = None) -> dict[str, Any]:
    pages = list_wiki_pages(store, sandbox_id=sandbox_id, status="published")
    findings: list[dict[str, Any]] = []
    seen_names: dict[str, str] = {}
    for page in pages:
        canonical = str(page.get("canonical_name") or page.get("title") or "").strip().lower()
        if not page.get("source_document_id") and not page.get("source_asset_ids"):
            findings.append({"level": "warn", "type": "missing_source", "page_id": page["page_id"], "title": page["title"]})
        if float(page.get("confidence") or 0) < 0.5:
            findings.append({"level": "info", "type": "low_confidence", "page_id": page["page_id"], "title": page["title"]})
        if canonical:
            if canonical in seen_names:
                findings.append({"level": "warn", "type": "duplicate_canonical_name", "page_id": page["page_id"], "other_page_id": seen_names[canonical], "title": page["title"]})
            else:
                seen_names[canonical] = page["page_id"]
    now = _now_iso()
    status = "success"
    with store.SessionFactory() as sess:
        run = DBKnowledgeLintRun(
            lint_id=f"klr_{uuid.uuid4().hex[:12]}",
            sandbox_id=sandbox_id,
            status=status,
            findings=findings,
            created_at=now,
            updated_at=now,
        )
        sess.add(run)
        sess.commit()
        lint_id = run.lint_id
    return {"lint_id": lint_id, "status": status, "findings": findings}
