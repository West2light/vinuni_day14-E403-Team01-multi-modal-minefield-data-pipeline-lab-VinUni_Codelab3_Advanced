import re
from html.parser import HTMLParser

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - optional classroom dependency
    BeautifulSoup = None

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract product data from the HTML table, ignoring boilerplate.

class CatalogTableParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_catalog = False
        self.in_row = False
        self.in_cell = False
        self.current_cell = []
        self.current_row = []
        self.rows = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "table" and attrs.get("id") == "main-catalog":
            self.in_catalog = True
        elif self.in_catalog and tag == "tr":
            self.in_row = True
            self.current_row = []
        elif self.in_row and tag == "td":
            self.in_cell = True
            self.current_cell = []

    def handle_data(self, data):
        if self.in_cell:
            self.current_cell.append(data)

    def handle_endtag(self, tag):
        if self.in_cell and tag == "td":
            self.in_cell = False
            self.current_row.append(" ".join(self.current_cell).strip())
        elif self.in_row and tag == "tr":
            self.in_row = False
            if self.current_row:
                self.rows.append(self.current_row)
        elif self.in_catalog and tag == "table":
            self.in_catalog = False


def parse_html_catalog(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        html = f.read()
    # ------------------------------------------

    def parse_price_vnd(value):
        text = value.strip().lower()
        unavailable_values = {"", "n/a", "na", "null", "none", "lien he", "liên hệ", "liãªn há»‡"}
        if text in unavailable_values or "liãªn" in text:
            return None

        digits = re.sub(r"[^\d]", "", text)
        return float(digits) if digits else None

    if BeautifulSoup:
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find("table", id="main-catalog")
        if table is None:
            return []
        rows = [
            [cell.get_text(" ", strip=True) for cell in row.find_all("td")]
            for row in table.select("tbody tr")
        ]
    else:
        parser = CatalogTableParser()
        parser.feed(html)
        rows = parser.rows

    product_count = len(rows)
    documents = []

    for row in rows:
        if len(row) < 6:
            continue

        product_id, name, category, price_text, stock_text, rating = row[:6]
        price_vnd = parse_price_vnd(price_text)
        price_display = f"{price_vnd:g} VND" if price_vnd is not None else "price unavailable"

        try:
            stock_quantity = int(stock_text)
        except ValueError:
            stock_quantity = None

        documents.append(
            {
                "document_id": f"html-{product_id.lower()}",
                "content": (
                    f"Catalog product {product_id}: {name} in category {category}. "
                    f"Listed price is {price_display}; stock is {stock_text}; rating is {rating}."
                ),
                "source_type": "HTML",
                "author": "VinShop",
                "timestamp": None,
                "source_metadata": {
                    "product_id": product_id,
                    "product_name": name,
                    "category": category,
                    "price_vnd": price_vnd,
                    "stock_quantity": stock_quantity,
                    "rating": rating,
                    "product_count": product_count,
                },
                "tags": ["catalog", category.lower()],
            }
        )

    return documents
