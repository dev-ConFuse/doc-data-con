"""
Document Management Routes for Data Connector Service
Handles document CRUD operations, file uploads, and analytics
"""

import hashlib
import uuid
from datetime import datetime, timezone
from typing import List, Optional
import structlog
from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    UploadFile,
    File,
    Form,
    BackgroundTasks,
    Request,
)
from pydantic import BaseModel
from pathlib import Path
from app.utils.user import parse_user_id
from app.config import get_settings
from sqlalchemy import select, delete

from app.infra.db.postgres import get_session, Document
from app.services.documents.ingester import trigger_initial_sync

logger = structlog.get_logger()
router = APIRouter(prefix="/api/documents", tags=["documents"])


# --------------- Models ---------------


class CreateDocumentRequest(BaseModel):
    name: str
    doc_type: str
    source: str
    size: Optional[str] = None
    tags: Optional[list[str]] = None


# =============================================================================
# Document CRUD Endpoints
# =============================================================================


@router.get("")
async def list_documents(
    http_request: Request,
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    """List all documents with optional search filtering and pagination."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required.")

    parsed_uid = parse_user_id(user_id)

    async with get_session() as session:
        query = select(Document).where(Document.user_id == parsed_uid).order_by(Document.created_at.desc())

        if search:
            query = query.where(Document.name.ilike(f"%{search}%"))

        result = await session.execute(query)
        docs = result.scalars().all()

        total = len(docs)
        start_idx = (page - 1) * limit
        paginated_docs = docs[start_idx : start_idx + limit]

        return {
            "success": True,
            "message": "Documents retrieved successfully",
            "data": {
                "data": [
                    {
                        "id": str(d.id),
                        "user_id": str(d.user_id),
                        "name": d.name,
                        "doc_type": d.doc_type,
                        "source": d.source,
                        "uri": d.uri,
                        "size": d.size or "Unknown",
                        "size_bytes": d.size_bytes or 0,
                        "tags": d.tags or [],
                        "status": d.status,
                        "created_at": d.created_at.isoformat() if d.created_at else None,
                        "updated_at": d.updated_at.isoformat() if d.updated_at else None,
                        "metadata": d.document_metadata or {},
                    }
                    for d in paginated_docs
                ],
                "total": total,
                "page": page,
                "limit": limit,
                "total_pages": (total + limit - 1) // limit if total > 0 else 1,
            },
        }


@router.post("", status_code=201)
async def create_document(payload: CreateDocumentRequest, http_request: Request):
    """Create a new document entry."""
    user_id = http_request.headers.get("x-user-id")
    if not user_id:
        raise HTTPException(status_code=401, detail="User authentication required.")

    parsed_uid = parse_user_id(user_id)

    async with get_session() as session:
        doc = Document(
            user_id=parsed_uid,
            name=payload.name,
            doc_type=payload.doc_type,
            source=payload.source,
            size=payload.size or "0 KB",
            tags=payload.tags or [],
            status="active",
        )
        session.add(doc)
        await session.commit()
        await session.refresh(doc)

        return {
            "success": True,
            "message": "Document created successfully",
            "data": {
                "id": str(doc.id),
                "user_id": str(doc.user_id),
                "name": doc.name,
                "doc_type": doc.doc_type,
                "source": doc.source,
                "size": doc.size,
                "tags": doc.tags or [],
                "status": doc.status,
                "created_at": doc.created_at.isoformat() if doc.created_at else None,
                "updated_at": doc.updated_at.isoformat() if doc.updated_at else None,
            },
        }


@router.delete("/{doc_id:path}")
async def delete_document(doc_id: str, http_request: Request):
    """Delete a document by ID."""
    user_id = http_request.headers.get("x-user-id")
    source_id_str = doc_id[:36] if len(doc_id) >= 36 else doc_id

    try:
        val_uuid = uuid.UUID(source_id_str)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid document ID")

    async with get_session() as session:
        doc = await session.get(Document, val_uuid)
        if not doc:
            raise HTTPException(status_code=404, detail="Document not found")

        user_id_str = str(doc.user_id) if doc.user_id else "system"
        await session.delete(doc)
        await session.commit()

        # Trigger downstream graph cleanup
        from app.services.client import get_service_client
        client = get_service_client()
        import asyncio
        asyncio.create_task(client.delete_graph_group(str(val_uuid), user_id_str))

        return {"success": True, "message": "Document deleted successfully"}


@router.get("/analytics")
async def get_analytics(http_request: Request):
    """Get document analytics and statistics."""
    user_id = http_request.headers.get("x-user-id")

    async with get_session() as session:
        query = select(Document)
        if user_id:
            try:
                query = query.where(Document.user_id == parse_user_id(user_id))
            except Exception:
                pass

        result = await session.execute(query)
        docs = result.scalars().all()

        total_documents = len(docs)
        total_size_bytes = sum(d.size_bytes or 0 for d in docs)
        by_source = {}
        by_type = {}

        for d in docs:
            src = d.source or "upload"
            by_source[src] = by_source.get(src, 0) + 1

            ftype = d.doc_type or "unknown"
            by_type[ftype] = by_type.get(ftype, 0) + 1

        return {
            "success": True,
            "message": "Analytics retrieved successfully",
            "data": {
                "total_documents": total_documents,
                "total_size_mb": round(total_size_bytes / (1024 * 1024), 2),
                "by_type": by_type,
                "by_source": by_source,
            },
        }


# =============================================================================
# File Upload Endpoints
# =============================================================================


@router.post("/upload")
async def upload_documents(
    http_request: Request,
    background_tasks: BackgroundTasks,
    files: List[UploadFile] = File(...),
    source_name: str = Form(default="local-upload"),
):
    """Upload documents directly and trigger processing."""
    user_id_str = http_request.headers.get("x-user-id")
    if not user_id_str:
        raise HTTPException(status_code=401, detail="User authentication required.")

    user_id = parse_user_id(user_id_str)
    settings = get_settings()
    downloads_base = Path(settings.downloads_folder)

    # Read every file up front so the upload can be identified by content before
    # anything is created. One document per (user, filename): identical bytes are
    # ignored entirely, changed bytes update the existing row in place.
    uploads: list[dict] = []
    total_size = 0
    for file in files:
        file_content = await file.read()
        uploads.append(
            {
                "name": file.filename or "unnamed",
                "content": file_content,
                "content_type": file.content_type,
            }
        )
        total_size += len(file_content)

    if not uploads:
        raise HTTPException(status_code=400, detail="No files were provided.")

    doc_name = (
        source_name
        if source_name != "local-upload"
        else (uploads[0]["name"] if uploads else "uploaded-files")
    )
    doc_type = uploads[0]["name"].split(".")[-1] if "." in uploads[0]["name"] else "txt"

    # Deterministic fingerprint over the request's files. Order-independent, so
    # re-uploading the same set always produces the same hash.
    digest = hashlib.sha256()
    for item in sorted(uploads, key=lambda u: u["name"]):
        digest.update(item["name"].encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(item["content"]).digest())
    content_hash = digest.hexdigest()

    target_dir: Optional[Path] = None
    doc_id = uuid.uuid4()

    try:
        async with get_session() as session:
            # Match on (user, source, name) so a document never appears twice.
            existing_doc = (
                await session.execute(
                    select(Document)
                    .where(
                        Document.user_id == user_id,
                        Document.source == "upload",
                        Document.name == doc_name,
                    )
                    .order_by(Document.created_at.asc())
                )
            ).scalars().first()

            if existing_doc is not None:
                stored_hash = (existing_doc.document_metadata or {}).get("content_hash")
                if stored_hash == content_hash:
                    logger.info(
                        "[UPLOAD] Identical content already stored - reusing existing document",
                        doc_id=str(existing_doc.id),
                        name=doc_name,
                    )
                    return {
                        "success": True,
                        "message": "Document already uploaded - no changes made",
                        "data": {
                            "id": str(existing_doc.id),
                            "source_id": str(existing_doc.id),
                            "duplicate": True,
                            "files_processed": 0,
                            "files_received": len(uploads),
                            "total_size_bytes": total_size,
                        },
                    }
                # Same name, different content: keep the row, re-index it.
                doc_id = existing_doc.id
                logger.info(
                    "[UPLOAD] Content changed - updating existing document in place",
                    doc_id=str(doc_id),
                    name=doc_name,
                )
            else:
                doc_id = uuid.uuid4()
                logger.info("[UPLOAD] New document", doc_id=str(doc_id), name=doc_name)

            target_dir = downloads_base / "docs" / str(doc_id)
            target_dir.mkdir(parents=True, exist_ok=True)

            uploaded_files_info = []
            for item in uploads:
                file_path = target_dir / item["name"]

                if not str(file_path.resolve()).startswith(str(target_dir.resolve())):
                    logger.warning("Potential path traversal attempt blocked", filename=item["name"])
                    continue

                with open(file_path, "wb") as f:
                    f.write(item["content"])

                uploaded_files_info.append(
                    {"name": item["name"], "size_bytes": len(item["content"]), "content_type": item["content_type"]}
                )

            size_str = (
                f"{total_size / 1024:.1f} KB"
                if total_size < 1024 * 1024
                else f"{total_size / (1024 * 1024):.1f} MB"
            )
            doc_metadata = {
                "files": uploaded_files_info,
                "content_hash": content_hash,
                "total_size_bytes": total_size,
                "file_count": len(uploaded_files_info),
                "upload_time": datetime.now(timezone.utc).isoformat(),
            }

            if existing_doc is not None:
                # Update in place - same row and same id, so nothing is listed twice.
                existing_doc.doc_type = doc_type
                existing_doc.uri = f"local://docs/{doc_id}"
                existing_doc.size = size_str
                existing_doc.size_bytes = total_size
                existing_doc.status = "active"
                existing_doc.document_metadata = doc_metadata
                existing_doc.updated_at = datetime.now(timezone.utc)
                session.add(existing_doc)
                is_new = False
            else:
                session.add(
                    Document(
                        id=doc_id,
                        user_id=user_id,
                        name=doc_name,
                        doc_type=doc_type,
                        source="upload",
                        uri=f"local://docs/{doc_id}",
                        size=size_str,
                        size_bytes=total_size,
                        tags=[],
                        status="active",
                        document_metadata=doc_metadata,
                    )
                )
                is_new = True

            await session.commit()

        # Billing counts documents, so only a genuinely new row moves the number.
        if is_new:
            try:
                import httpx

                async with httpx.AsyncClient() as http_client:
                    await http_client.post(
                        f"{settings.auth_url}/billing/internal/update-doc-count",
                        json={"userId": str(user_id), "delta": 1},
                        headers={"X-API-Key": settings.internal_api_key},
                        timeout=10.0,
                    )
            except Exception as e:
                logger.warning("[DOC-CREATE] Failed to update billing count", error=str(e))

        # Trigger background processing
        background_tasks.add_task(trigger_initial_sync, str(doc_id), "upload", doc_metadata)

        return {
            "success": True,
            "message": (
                f"Updated '{doc_name}' and re-triggered processing"
                if not is_new
                else f"Successfully uploaded {len(uploaded_files_info)} file(s) and triggered processing"
            ),
            "data": {
                "id": str(doc_id),
                "source_id": str(doc_id),
                "duplicate": False,
                "updated_existing": not is_new,
                "files_processed": len(uploaded_files_info),
                "files_received": len(files),
                "total_size_bytes": total_size,
            },
        }

    except Exception as e:
        logger.error("Failed to process document upload", error=str(e), exc_info=True)
        if target_dir is not None and target_dir.exists():
            import shutil
            try:
                shutil.rmtree(target_dir)
            except:
                pass

        raise HTTPException(status_code=500, detail=f"Upload processing failed: {str(e)}")
