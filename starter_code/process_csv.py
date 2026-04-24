import csv
import re
from datetime import datetime

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.

def process_sales_csv(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    # ------------------------------------------

    def clean_price(value):
        text = str(value or "").strip().lower()
        if text in {"", "n/a", "na", "null", "none", "lien he", "liên hệ", "liãªn há»‡"}:
            return None
        if text == "five dollars":
            return 5.0

        text = text.replace("$", "").replace(",", "")
        match = re.search(r"-?\d+(?:\.\d+)?", text)
        if not match:
            return None

        price = float(match.group())
        return price if price >= 0 else None

    def normalize_date(value):
        formats = [
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%B %dth %Y",
            "%B %dst %Y",
            "%B %dnd %Y",
            "%B %drd %Y",
            "%d-%m-%Y",
            "%Y/%m/%d",
            "%d %b %Y",
        ]
        text = str(value or "").strip()
        for fmt in formats:
            try:
                return datetime.strptime(text, fmt).date().isoformat()
            except ValueError:
                continue
        return None

    deduped_rows = []
    seen_ids = set()
    for row in rows:
        row_id = row.get("id", "").strip()
        if row_id in seen_ids:
            continue
        seen_ids.add(row_id)
        deduped_rows.append(row)

    documents = []
    for row in deduped_rows:
        row_id = int(row["id"])
        currency = str(row.get("currency", "")).upper()
        clean_price_value = clean_price(row.get("price"))
        normalized_date = normalize_date(row.get("date_of_sale"))
        price_usd = clean_price_value if currency == "USD" else None
        price_vnd = clean_price_value if currency == "VND" else None
        price_text = (
            f"{clean_price_value:g} {currency}"
            if clean_price_value is not None
            else "price unavailable"
        )

        try:
            stock_value = int(row["stock_quantity"]) if row.get("stock_quantity") else None
        except ValueError:
            stock_value = None

        documents.append(
            {
                "document_id": f"csv-{row_id}",
                "content": (
                    f"Sales record {row_id}: {row['product_name']} in {row['category']} "
                    f"sold by {row['seller_id']} for {price_text} on "
                    f"{normalized_date or 'unknown date'}."
                ),
                "source_type": "CSV",
                "author": row["seller_id"],
                "timestamp": normalized_date,
                "source_metadata": {
                    "row_id": row_id,
                    "product_name": row["product_name"],
                    "category": row["category"],
                    "currency": currency,
                    "price_usd": price_usd,
                    "price_vnd": price_vnd,
                    "stock_quantity": stock_value,
                    "original_price": row.get("price"),
                },
                "tags": ["sales", row["category"].lower()],
            }
        )

    return documents
