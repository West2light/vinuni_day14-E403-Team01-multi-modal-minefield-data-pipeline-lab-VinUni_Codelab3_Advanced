import json
import os
import re

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional classroom dependency
    load_dotenv = None


def _load_gemini_client():
    if load_dotenv:
        load_dotenv()

    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        return None

    try:
        import google.generativeai as genai
    except ImportError:
        return None

    genai.configure(api_key=api_key)
    return genai


def _clean_json_response(content_text):
    content_text = content_text.strip()
    if content_text.startswith("```json"):
        content_text = content_text[7:]
    if content_text.startswith("```"):
        content_text = content_text[3:]
    if content_text.endswith("```"):
        content_text = content_text[:-3]
    return json.loads(content_text.strip())


def _extract_text_locally(file_path):
    readers = []
    try:
        from pypdf import PdfReader
        readers.append(PdfReader)
    except ImportError:
        pass

    try:
        from PyPDF2 import PdfReader
        readers.append(PdfReader)
    except ImportError:
        pass

    for reader_cls in readers:
        try:
            reader = reader_cls(file_path)
            return "\n".join(page.extract_text() or "" for page in reader.pages), len(reader.pages)
        except Exception:
            continue

    return "", None


def _summarize_text(text):
    sentences = re.split(r"(?<=[.!?])\s+", re.sub(r"\s+", " ", text).strip())
    usable = [sentence for sentence in sentences if len(sentence) > 20]
    summary = " ".join(usable[:3])
    return summary or "PDF lecture notes were ingested, but text extraction returned limited readable content."


def extract_pdf_data(file_path):
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None

    genai = _load_gemini_client()
    if genai:
        model = genai.GenerativeModel("gemini-2.5-flash")

        print(f"Uploading {file_path} to Gemini...")
        try:
            pdf_file = genai.upload_file(path=file_path)
            prompt = """
Analyze this document and extract a summary, author, title, and main topics.
Output exactly as a JSON object matching this exact format:
{
    "document_id": "pdf-lecture-notes",
    "content": "Title: [title]. Summary: [3-sentence summary]",
    "source_type": "PDF",
    "author": "[author name or Unknown]",
    "timestamp": null,
    "source_metadata": {
        "original_file": "lecture_notes.pdf",
        "main_topics": ["Data Pipeline", "ETL"]
    },
    "tags": ["pdf", "lecture-notes"]
}
"""
            print("Generating content from PDF using Gemini...")
            response = model.generate_content([pdf_file, prompt])
            extracted_data = _clean_json_response(response.text)
            extracted_data.setdefault("document_id", "pdf-lecture-notes")
            extracted_data.setdefault("source_type", "PDF")
            extracted_data.setdefault("author", "Unknown")
            extracted_data.setdefault("timestamp", None)
            extracted_data.setdefault("source_metadata", {})
            extracted_data["source_metadata"].setdefault("original_file", os.path.basename(file_path))
            extracted_data["source_metadata"].setdefault("main_topics", ["Data Pipeline", "ETL"])
            extracted_data.setdefault("tags", ["pdf", "lecture-notes"])
            return extracted_data
        except Exception as exc:
            print(f"Gemini PDF extraction failed, falling back to local extraction: {exc}")

    text, page_count = _extract_text_locally(file_path)
    summary = _summarize_text(text)

    return {
        "document_id": "pdf-lecture-notes",
        "content": f"Summary: {summary}",
        "source_type": "PDF",
        "author": "Unknown",
        "timestamp": None,
        "source_metadata": {
            "original_file": os.path.basename(file_path),
            "main_topics": ["Data Pipeline", "ETL"],
            "page_count": page_count,
        },
        "tags": ["pdf", "lecture-notes"],
    }
