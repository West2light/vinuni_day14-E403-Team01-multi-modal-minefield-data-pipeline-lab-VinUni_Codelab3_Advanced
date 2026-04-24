import ast
import re

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code.

def extract_logic_from_code(file_path):
    # --- FILE READING (Handled for students) ---
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()
    # ------------------------------------------

    tree = ast.parse(source_code)
    module_docstring = ast.get_docstring(tree)

    function_docs = []
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            docstring = ast.get_docstring(node)
            if docstring:
                function_docs.append(f"{node.name}: {docstring}")

    comment_lines = []
    business_rules = []
    for line in source_code.splitlines():
        stripped = line.strip()
        if not stripped.startswith("#"):
            continue
        comment = stripped.lstrip("#").strip()
        comment_lines.append(comment)
        if re.search(r"business\s+logic\s+rule\s+\d+", comment, re.IGNORECASE):
            business_rules.append(comment)

    # Capture rules embedded in docstrings as well as standalone comments.
    for doc in function_docs:
        business_rules.extend(
            re.findall(
                r"Business Logic Rule\s+\d+:[\s\S]*?(?=\n\s*\n|$)",
                doc,
                flags=re.IGNORECASE,
            )
        )

    tax_comment_rate = None
    tax_code_rate = None
    tax_comment_rates = []
    for comment in comment_lines:
        if re.search(r"tax|vat", comment, re.IGNORECASE):
            tax_comment_rates.extend(int(rate) for rate in re.findall(r"(\d+)\s*%", comment))

    tax_code_match = re.search(r"tax_rate\s*=\s*0\.(\d+)", source_code)
    if tax_code_match:
        tax_code_rate = int(tax_code_match.group(1))
    if tax_comment_rates:
        mismatched_rates = [rate for rate in tax_comment_rates if rate != tax_code_rate]
        tax_comment_rate = mismatched_rates[0] if mismatched_rates else tax_comment_rates[0]

    tax_discrepancy = (
        tax_comment_rate is not None
        and tax_code_rate is not None
        and tax_comment_rate != tax_code_rate
    )

    sections = []
    if module_docstring:
        sections.append(f"Module notes:\n{module_docstring}")
    if function_docs:
        sections.append("Function docstrings:\n" + "\n\n".join(function_docs))
    if comment_lines:
        sections.append("Important comments:\n" + "\n".join(comment_lines))

    if tax_discrepancy:
        sections.append(
            f"Tax discrepancy detected: comment mentions {tax_comment_rate}% "
            f"while code uses {tax_code_rate}%."
        )

    content = "\n\n".join(sections).replace("Da Nang", "DN city").replace("Da-Nang", "DN city")

    return {
        "document_id": "code-legacy-pipeline",
        "content": content,
        "source_type": "Code",
        "author": "Senior Dev (retired)",
        "timestamp": None,
        "source_metadata": {
            "business_rules": business_rules,
            "function_count": sum(isinstance(node, ast.FunctionDef) for node in ast.walk(tree)),
            "tax_comment_rate": tax_comment_rate,
            "tax_code_rate": tax_code_rate,
            "tax_discrepancy": tax_discrepancy,
            "original_file": "legacy_pipeline.py",
        },
        "tags": ["legacy-code", "business-rules"],
    }
