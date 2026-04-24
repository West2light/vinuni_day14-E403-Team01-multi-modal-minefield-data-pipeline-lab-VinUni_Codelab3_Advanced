import json
import time
import os
from datetime import datetime

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(SCRIPT_DIR)
RAW_DATA_DIR = os.path.join(ROOT_DIR, "raw_data")

# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Task: Orchestrate the ingestion pipeline and handle errors/SLA.

def main():
    print("🚀 Starting Data Pipeline Orchestration...")
    start_time = time.time()
    final_kb = []
    
    # --- FILE PATH SETUP ---
    paths = {
        "PDF": os.path.join(RAW_DATA_DIR, "lecture_notes.pdf"),
        "Transcript": os.path.join(RAW_DATA_DIR, "demo_transcript.txt"),
        "HTML": os.path.join(RAW_DATA_DIR, "product_catalog.html"),
        "CSV": os.path.join(RAW_DATA_DIR, "sales_records.csv"),
        "Code": os.path.join(RAW_DATA_DIR, "legacy_pipeline.py"),
    }
    
    output_path = os.path.join(ROOT_DIR, "processed_knowledge_base.json")
    # ----------------------------------------------

    # Define processing tasks
    tasks = [
        ("PDF", extract_pdf_data, paths["PDF"]),
        ("Transcript", clean_transcript, paths["Transcript"]),
        ("HTML", parse_html_catalog, paths["HTML"]),
        ("CSV", process_sales_csv, paths["CSV"]),
        ("Code", extract_logic_from_code, paths["Code"]),
    ]

    for label, process_func, file_path in tasks:
        print(f"\n--- Processing Source: {label} ---")
        if not os.path.exists(file_path):
            print(f"⚠️ Skipping {label}: File not found at {file_path}")
            continue

        try:
            # 1. Extraction (Role 2)
            extracted_data = process_func(file_path)
            
            # Normalize: handle both single dict and list of dicts
            if isinstance(extracted_data, dict):
                entries = [extracted_data] if extracted_data else []
            elif isinstance(extracted_data, list):
                entries = extracted_data
            else:
                print(f"⚠️ {label}: Unexpected data format returned.")
                continue

            for raw_entry in entries:
                # 2. Quality Gate (Role 3)
                if run_quality_gate(raw_entry):
                    # 3. Schema Enforcement (Role 1)
                    doc = UnifiedDocument.from_raw_dict(raw_entry, source_label=label)
                    if doc:
                        final_kb.append(doc.to_json_dict())
                        print(f"✅ {label}: Successfully ingested '{doc.document_id}'")
                else:
                    # quality_check.py handles printing failure reasons
                    pass

        except Exception as e:
            print(f"❌ {label}: Critical error in pipeline step: {e}")

    # 4. Save Final Knowledge Base
    print(f"\n💾 Saving {len(final_kb)} documents to {output_path}...")
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(final_kb, f, ensure_ascii=False, indent=2)
        print("🎉 Pipeline completed successfully!")
    except Exception as e:
        print(f"❌ Failed to save output: {e}")

    # 5. SLA Monitoring
    end_time = time.time()
    duration = end_time - start_time
    print(f"\n⏱️ Execution Statistics:")
    print(f"Total processing time: {duration:.2f} seconds")
    
    if duration > 180: # Example SLA: 3 minutes
        print("🚨 ALERT: Pipeline execution exceeded SLA limit (180s)!")
    else:
        print("🟢 SLA Status: Compliant")


if __name__ == "__main__":
    main()
