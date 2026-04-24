import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Clean the transcript text and extract key information.

def clean_transcript(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        text = f.read()
    # ------------------------------------------

    def extract_price_vnd(raw_text):
        numeric_match = re.search(r"\b(\d{1,3}(?:,\d{3})+)\s*VND\b", raw_text, re.IGNORECASE)
        if numeric_match:
            return int(numeric_match.group(1).replace(",", ""))

        normalized = raw_text.lower()
        phrase_values = {
            "năm trăm nghìn": 500000,
            "nÄƒm trÄƒm nghÃ¬n": 500000,
        }
        for phrase, value in phrase_values.items():
            if phrase in normalized:
                return value
        return None

    detected_price_vnd = extract_price_vnd(text)

    cleaned = re.sub(r"\[\d{2}:\d{2}:\d{2}\]\s*", "", text)
    cleaned = re.sub(
        r"\[(?:music(?:\s+\w+)?|inaudible|laughter)\]",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    cleaned = re.sub(r"\[Speaker\s+\d+\]:\s*", "", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    return {
        "document_id": "video-demo-transcript",
        "content": cleaned,
        "source_type": "Video",
        "author": "Speaker 1",
        "timestamp": None,
        "source_metadata": {
            "detected_price_vnd": detected_price_vnd,
            "original_file": "demo_transcript.txt",
        },
        "tags": ["transcript", "data-pipeline", "semantic-drift"],
    }
