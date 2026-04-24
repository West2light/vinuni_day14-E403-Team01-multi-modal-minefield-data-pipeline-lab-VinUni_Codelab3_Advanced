"""
schema.py — Data Contract for the Multi-Modal Minefield pipeline.
Role 1: Lead Data Architect

Schema Version History
----------------------
  v1 (initial):  document_id, content, source_type, author, timestamp, source_metadata
  v2 (11:00 AM): Breaking change — field renames + new required tags field.
                 Backwards-compatible migration helper included.

Design principles
-----------------
* Every field that could be absent in a raw source is Optional with a safe default.
* source_metadata is a free-form dict so each Role 2 processor can attach
  source-specific sub-fields (e.g. detected_price_vnd, page_count, tax_rate …)
  without touching this file.
* The two classes share a common root so isinstance() checks keep working after
  migration.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Shared constants — used by all roles to avoid magic strings
# ---------------------------------------------------------------------------
class SourceType:
    PDF        = "PDF"
    VIDEO      = "Video"          # forensic agent checks d['source_type'] == 'Video'
    HTML       = "HTML"
    CSV        = "CSV"
    CODE       = "Code"
    UNKNOWN    = "Unknown"


# ---------------------------------------------------------------------------
# Metadata contract — Role 2 MUST populate these keys in source_metadata.
# Role 3 (quality_check) uses this to detect missing domain data.
# ---------------------------------------------------------------------------
METADATA_KEYS: Dict[str, List[str]] = {
    # source_type  →  required keys in source_metadata
    SourceType.PDF:   ["main_topics"],           # e.g. ["Data Pipeline", "ETL"]
    SourceType.VIDEO: ["detected_price_vnd"],    # int|None — forensic Q2
    SourceType.HTML:  ["product_count"],          # int
    SourceType.CSV:   ["price_usd", "price_vnd"], # float|None each
    SourceType.CODE:  ["business_rules", "tax_discrepancy"],  # list[str], bool
}

# Toxic substrings that quality gate MUST reject (Role 3 reference list)
TOXIC_PATTERNS: List[str] = [
    "Null pointer exception",
    "NullPointerException",
    "Traceback (most recent call last)",
    "ERROR:",
    "FATAL:",
    "undefined",
    "[inaudible]",           # raw noise token that should have been stripped
]


# ---------------------------------------------------------------------------
# v1 Schema   (active until the 11:00 AM incident)
# ---------------------------------------------------------------------------
class UnifiedDocument(BaseModel):
    """
    Canonical document representation consumed by the Knowledge Base.

    Fields
    ------
    document_id   : Globally unique identifier.  Convention per source:
                      PDF      → "pdf-<slug>"
                      Video    → "video-<slug>"
                      HTML     → "html-<slug>"
                      CSV      → "csv-<row_id>"   ← forensic agent checks this prefix
                      Code     → "code-<slug>"
    content       : The cleaned, human-readable body text extracted from the source.
    source_type   : One of the SourceType constants above.
    author        : Original author/speaker/seller when available.
    timestamp     : Publish / sale / recording date (UTC-normalised when possible).
    source_metadata : Bag-of-properties for source-specific data.
                      Processors MUST store domain facts here, e.g.:
                        { "detected_price_vnd": 500000 }   ← required by forensic Q2
                        { "page_count": 12 }
                        { "tax_rate_actual": 0.10, "tax_discrepancy": True }
    tags          : Optional free-form keyword list for downstream retrieval.
    """

    # ------------------------------------------------------------------
    # Core identity
    # ------------------------------------------------------------------
    document_id: str = Field(
        ...,
        description="Unique document identifier with source-prefix (e.g. 'csv-1').",
    )

    # ------------------------------------------------------------------
    # Content
    # ------------------------------------------------------------------
    content: str = Field(
        ...,
        description="Cleaned, structured text extracted from the source.",
        min_length=1,
    )

    # ------------------------------------------------------------------
    # Provenance
    # ------------------------------------------------------------------
    source_type: str = Field(
        ...,
        description="Origin format.  Use SourceType constants.",
    )
    author: Optional[str] = Field(
        default="Unknown",
        description="Author, speaker, or data owner.",
    )
    timestamp: Optional[datetime] = Field(
        default=None,
        description="ISO-8601 UTC datetime of the source event.",
    )

    # ------------------------------------------------------------------
    # Extensible metadata bag (Role 2 processors add keys here)
    # ------------------------------------------------------------------
    source_metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description=(
            "Source-specific key/value pairs.  "
            "Required keys per source:\n"
            "  Video → detected_price_vnd (int|None)\n"
            "  Code  → business_rules (list[str]), tax_discrepancy (bool)\n"
            "  CSV   → price_usd (float|None), price_vnd (float|None)\n"
            "  HTML  → product_count (int)\n"
            "  PDF   → main_topics (list[str]), page_count (int|None)"
        ),
    )

    # ------------------------------------------------------------------
    # Optional extras
    # ------------------------------------------------------------------
    tags: List[str] = Field(
        default_factory=list,
        description="Keyword tags for retrieval / filtering.",
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("source_type")
    @classmethod
    def validate_source_type(cls, v: str) -> str:
        allowed = {
            SourceType.PDF, SourceType.VIDEO, SourceType.HTML,
            SourceType.CSV, SourceType.CODE, SourceType.UNKNOWN,
        }
        if v not in allowed:
            raise ValueError(
                f"source_type '{v}' is not recognised.  "
                f"Use one of: {sorted(allowed)}"
            )
        return v

    @field_validator("document_id")
    @classmethod
    def validate_document_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("document_id must not be empty.")
        return v.strip()

    @field_validator("content")
    @classmethod
    def validate_content(cls, v: str) -> str:
        # Strip leading/trailing whitespace; reject blank strings
        v = v.strip()
        if not v:
            raise ValueError("content must not be blank after stripping.")
        return v

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------
    def to_json_dict(self) -> dict:
        """Return a JSON-serialisable dict (timestamps as ISO strings)."""
        d = self.model_dump()
        if d.get("timestamp") and isinstance(d["timestamp"], datetime):
            d["timestamp"] = d["timestamp"].isoformat()
        return d

    # ------------------------------------------------------------------
    # Factory — Role 4 (orchestrator) uses this instead of calling
    # UnifiedDocument(**raw_dict) directly, because it gives a clear
    # error message when a processor returns a malformed dict.
    # ------------------------------------------------------------------
    @classmethod
    def from_raw_dict(cls, raw: dict, source_label: str = "unknown") -> "UnifiedDocument":
        """
        Safe factory for Role 4.

        Usage in orchestrator.py::

            from schema import UnifiedDocument
            raw = extract_pdf_data(pdf_path)   # returns a dict
            doc = UnifiedDocument.from_raw_dict(raw, source_label="PDF")
            if doc:
                final_kb.append(doc.to_json_dict())

        Returns None (with a warning) instead of raising if the dict is bad.
        """
        if not raw:
            print(f"[WARN] {source_label}: processor returned empty/None — skipping.")
            return None
        if not isinstance(raw, dict):
            print(f"[WARN] {source_label}: expected dict, got {type(raw).__name__} — skipping.")
            return None
        try:
            return cls(**raw)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[WARN] {source_label}: schema validation failed — {exc}")
            return None

    def migrate_to_v2(self) -> "UnifiedDocumentV2":
        """
        Upgrade this v1 document to the v2 schema introduced at 11:00 AM.
        Calling this is idempotent — it is safe to call multiple times.
        """
        return UnifiedDocumentV2(
            # v2 renames -------------------------------------------------
            doc_id=self.document_id,          # document_id → doc_id
            body=self.content,                # content     → body
            # shared fields ----------------------------------------------
            source_type=self.source_type,
            author=self.author,
            timestamp=self.timestamp,
            metadata=self.source_metadata,   # source_metadata → metadata
            tags=self.tags,
            # new v2-only field ------------------------------------------
            schema_version="v2",
        )


# ---------------------------------------------------------------------------
# v2 Schema   (deployed at the 11:00 AM incident)
# ---------------------------------------------------------------------------
class UnifiedDocumentV2(BaseModel):
    """
    v2 of the Data Contract.  Breaking changes from v1:
      - document_id  renamed to  doc_id
      - content      renamed to  body
      - source_metadata renamed to  metadata
      - schema_version field added (required, must be "v2")

    All existing v1 data is migrated via UnifiedDocument.migrate_to_v2().
    """

    doc_id: str = Field(..., description="Unique document identifier (was: document_id).")
    body: str = Field(..., description="Cleaned body text (was: content).")
    source_type: str = Field(..., description="Origin format.  Use SourceType constants.")
    author: Optional[str] = Field(default="Unknown")
    timestamp: Optional[datetime] = Field(default=None)
    metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Source-specific key/value pairs (was: source_metadata).",
    )
    tags: List[str] = Field(default_factory=list)
    schema_version: str = Field(default="v2", description="Schema version tag.")

    # ------------------------------------------------------------------
    # Validators (mirror v1 rules)
    # ------------------------------------------------------------------
    @field_validator("source_type")
    @classmethod
    def validate_source_type(cls, v: str) -> str:
        allowed = {
            SourceType.PDF, SourceType.VIDEO, SourceType.HTML,
            SourceType.CSV, SourceType.CODE, SourceType.UNKNOWN,
        }
        if v not in allowed:
            raise ValueError(f"source_type '{v}' not in {sorted(allowed)}")
        return v

    @field_validator("doc_id")
    @classmethod
    def validate_doc_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("doc_id must not be empty.")
        return v.strip()

    @field_validator("body")
    @classmethod
    def validate_body(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("body must not be blank after stripping.")
        return v

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def to_json_dict(self) -> dict:
        """Return a JSON-serialisable dict (timestamps as ISO strings)."""
        d = self.model_dump()
        if d.get("timestamp") and isinstance(d["timestamp"], datetime):
            d["timestamp"] = d["timestamp"].isoformat()
        return d

    def downgrade_to_v1(self) -> UnifiedDocument:
        """Rollback to v1 if needed (e.g. for backward-compat consumers)."""
        return UnifiedDocument(
            document_id=self.doc_id,
            content=self.body,
            source_type=self.source_type,
            author=self.author,
            timestamp=self.timestamp,
            source_metadata=self.metadata,
            tags=self.tags,
        )


# ---------------------------------------------------------------------------
# Standalone helpers — Role 3 (quality_check.py) can import these directly
# ---------------------------------------------------------------------------
def validate_raw_dict(raw: dict) -> tuple[bool, str]:
    """
    Lightweight pre-flight check for Role 3.
    Returns (True, "ok") or (False, reason_string).

    Usage in quality_check.py::

        from schema import validate_raw_dict, TOXIC_PATTERNS

        def run_quality_gate(document_dict: dict) -> bool:
            ok, reason = validate_raw_dict(document_dict)
            if not ok:
                print(f"[REJECT] {reason}")
                return False
            return True
    """
    if not raw or not isinstance(raw, dict):
        return False, "not a dict"

    # --- required fields ---
    for field in ("document_id", "content", "source_type"):
        if not raw.get(field):
            return False, f"missing required field '{field}'"

    # --- min content length (quality gate rule) ---
    content = str(raw.get("content", "")).strip()
    if len(content) < 20:
        return False, f"content too short ({len(content)} chars, min 20)"

    # --- toxic pattern check (forensic Q3) ---
    for pattern in TOXIC_PATTERNS:
        if pattern in content:
            return False, f"toxic pattern detected: '{pattern}'"

    # --- metadata completeness warning (non-blocking) ---
    src = raw.get("source_type", "")
    required_meta_keys = METADATA_KEYS.get(src, [])
    meta = raw.get("source_metadata", {})
    missing_meta = [k for k in required_meta_keys if k not in meta]
    if missing_meta:
        print(f"[WARN] {raw.get('document_id', '?')}: source_metadata missing keys {missing_meta}")
        # Warning only — do NOT reject; Role 2 may legitimately omit some keys

    return True, "ok"


# ---------------------------------------------------------------------------
# Migration utility
# ---------------------------------------------------------------------------
def migrate_kb_to_v2(kb_v1: list[dict]) -> list[dict]:
    """
    Batch-migrate an in-memory knowledge base (list of JSON dicts) from v1 to v2.

    Usage (in orchestrator.py at 11:00 AM):
        from schema import migrate_kb_to_v2
        final_kb_v2 = migrate_kb_to_v2(final_kb_v1)

    Returns a list of v2 JSON-serialisable dicts.
    """
    migrated = []
    for record in kb_v1:
        try:
            doc_v1 = UnifiedDocument(**record)
            doc_v2 = doc_v1.migrate_to_v2()
            migrated.append(doc_v2.to_json_dict())
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[WARN] migrate_kb_to_v2: skipping record '{record.get('document_id', '?')}': {exc}")
    return migrated


# ---------------------------------------------------------------------------
# Quick self-test (run: python schema.py)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import json

    print("=" * 60)
    print("TEST 1 — v1 happy path")
    print("=" * 60)
    sample_v1 = UnifiedDocument(
        document_id="csv-1",
        content="Laptop VinAI Pro 14 sold for $1200 USD on 2026-01-15.",
        source_type=SourceType.CSV,
        author="S001",
        source_metadata={"price_usd": 1200.0, "price_vnd": None, "currency": "USD"},
        tags=["laptop", "electronics"],
    )
    print(json.dumps(sample_v1.to_json_dict(), ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("TEST 2 — from_raw_dict() factory (Role 4 usage)")
    print("=" * 60)
    raw_good = {
        "document_id": "video-demo",
        "content": "Discussion about Data Pipeline Engineering and Semantic Drift.",
        "source_type": SourceType.VIDEO,
        "source_metadata": {"detected_price_vnd": 500000},
    }
    doc = UnifiedDocument.from_raw_dict(raw_good, source_label="Transcript")
    assert doc is not None, "should succeed"
    print("from_raw_dict OK:", doc.document_id)

    bad_doc = UnifiedDocument.from_raw_dict(
        {"document_id": "bad", "content": "", "source_type": "PDF"},
        source_label="BadProcessor",
    )
    assert bad_doc is None, "empty content should fail"
    print("from_raw_dict correctly rejected blank content")

    print("\n" + "=" * 60)
    print("TEST 3 — validate_raw_dict() (Role 3 usage)")
    print("=" * 60)
    ok, reason = validate_raw_dict(raw_good)
    assert ok, reason
    print(f"[PASS] valid dict: {reason}")

    toxic_raw = {**raw_good, "content": "Fatal error: Null pointer exception in module X"}
    ok, reason = validate_raw_dict(toxic_raw)
    assert not ok
    print(f"[PASS] toxic content correctly rejected: {reason}")

    short_raw = {**raw_good, "content": "short"}
    ok, reason = validate_raw_dict(short_raw)
    assert not ok
    print(f"[PASS] short content correctly rejected: {reason}")

    print("\n" + "=" * 60)
    print("TEST 4 — v1 -> v2 migration")
    print("=" * 60)
    sample_v2 = sample_v1.migrate_to_v2()
    print(json.dumps(sample_v2.to_json_dict(), ensure_ascii=False, indent=2))

    print("\n" + "=" * 60)
    print("TEST 5 — batch migrate_kb_to_v2()")
    print("=" * 60)
    kb = [sample_v1.to_json_dict(), raw_good]
    migrated = migrate_kb_to_v2(kb)
    print(f"Migrated {len(migrated)} records")
    assert len(migrated) == 2

    print("\n[PASS] All schema.py self-tests PASSED.")
