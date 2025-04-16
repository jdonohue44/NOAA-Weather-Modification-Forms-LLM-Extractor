import os
import re
import csv
import fitz  # PyMuPDF

from pdf2image import convert_from_path
import pytesseract

from spellchecker import SpellChecker
import re

def extract_text_pymupdf(filepath):
    try:
        doc = fitz.open(filepath)
        text = doc[0].get_text()
        doc.close()
        return text.strip()
    except Exception:
        return ""

def extract_text_ocr(filepath):
    try:
        images = convert_from_path(filepath, first_page=1, last_page=1)
        if images:
            return pytesseract.image_to_string(images[0], lang='eng').strip()
    except Exception:
        return ""
    return ""

def is_text_readable(text):
    total_chars = len(text)
    alpha_chars = len(re.findall(r"[a-zA-Z]", text))
    alpha_ratio = alpha_chars / total_chars if total_chars > 0 else 0

    words = re.findall(r'\b\w+\b', text)
    total_words = len(words)
    short_words = [w for w in words if len(w) <= 2]
    junk_word_ratio = len(short_words) / total_words if total_words > 0 else 0

    spell = SpellChecker()
    long_words = [w for w in words if len(w) >= 4]
    misspelled = spell.unknown(long_words)
    miss_ratio = len(misspelled) / len(long_words) if long_words else 0

    return (
        total_chars > 200 and
        alpha_ratio > 0.2 and
        junk_word_ratio < 0.4 and
        miss_ratio < 0.5
    )

def process_directory(directory, output_csv="scan_results.csv"):
    rows = []
    for filename in sorted(os.listdir(directory)):
        if not filename.lower().endswith(".pdf"):
            continue

        full_path = os.path.join(directory, filename)
        pymu_text = extract_text_pymupdf(full_path)

        if is_text_readable(pymu_text):
            method = "PyMuPDF"
            final_text = pymu_text
        else:
            ocr_text = extract_text_ocr(full_path)
            method = "OCR" if is_text_readable(ocr_text) else "Unusable"
            final_text = ocr_text if method == "OCR" else ""

        rows.append({
            "filename": filename,
            "method_used": method,
            "first_page_text": final_text[:500].replace('\n', ' ')
        })

    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["filename", "method_used", "first_page_text"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n✅ Results saved to: {output_csv}")

# === Run locally ===
if __name__ == "__main__":
    directory = "../noaa-files"  # or change to your full path
    output_csv = "noaa_scan_method_summary.csv"
    process_directory(directory, output_csv)

