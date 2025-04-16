import os
import re
import csv
import fitz  # PyMuPDF

def analyze_pdf(filepath):
    try:
        doc = fitz.open(filepath)
        num_pages = len(doc)
        total_chars = 0
        total_alpha_chars = 0
        total_weird_chars = 0
        total_words = 0
        total_short_words = 0
        total_lines = 0
        total_repeat_lines = 0

        for page in doc:
            text = page.get_text()
            if not text.strip():
                continue

            total_chars += len(text)
            total_alpha_chars += len(re.findall(r"[a-zA-Z]", text))
            total_weird_chars += len(re.findall(r"[^\x20-\x7E]", text))  # non-ASCII

            words = re.findall(r'\w+', text)
            total_words += len(words)
            total_short_words += len([w for w in words if len(w) <= 2])

            lines = text.splitlines()
            total_lines += len(lines)
            total_repeat_lines += (len(lines) - len(set(lines)))

        doc.close()

        # Avoid divide-by-zero
        avg_chars_per_page = total_chars / num_pages if num_pages > 0 else 0
        avg_alpha_per_page = total_alpha_chars / num_pages if num_pages > 0 else 0
        alpha_ratio = total_alpha_chars / total_chars if total_chars > 0 else 0
        garble_ratio = total_weird_chars / total_chars if total_chars > 0 else 0
        junk_word_ratio = total_short_words / total_words if total_words > 0 else 0
        repeat_ratio = total_repeat_lines / total_lines if total_lines > 0 else 0

        # Heuristics for scan / OCR mess detection
        is_likely_scan = (
            avg_chars_per_page < 200
            or avg_alpha_per_page < 50
            or alpha_ratio < 0.2
            or garble_ratio > 0.05
            or junk_word_ratio > 0.4
            or repeat_ratio > 0.25
        )

        return {
            'is_scan': is_likely_scan,
            'avg_chars': avg_chars_per_page,
            'avg_alpha': avg_alpha_per_page,
            'alpha_ratio': alpha_ratio,
            'garble_ratio': garble_ratio,
            'junk_word_ratio': junk_word_ratio,
            'repeat_ratio': repeat_ratio,
            'pages': num_pages
        }

    except Exception as e:
        print(f"Error processing {filepath}: {e}")
        return {
            'is_scan': True,
            'avg_chars': 0,
            'avg_alpha': 0,
            'alpha_ratio': 0,
            'garble_ratio': 1,
            'junk_word_ratio': 1,
            'repeat_ratio': 1,
            'pages': 0
        }

def scan_check(directory, output_csv="scan_results.csv"):
    summary = []
    total_files = 0
    scan_count = 0

    print(f"\nChecking PDFs in directory: {directory}\n")
    print(f"{'Filename':<50} | {'Status':<6} | {'Chars':>6} | {'Alpha':>6} | {'Alpha%':>6} | {'Garble':>6} | {'Junk':>6} | {'Repeat':>6} | {'Pg':>3}")

    for filename in sorted(os.listdir(directory)):
        if not filename.lower().endswith(".pdf"):
            continue
        full_path = os.path.join(directory, filename)
        stats = analyze_pdf(full_path)

        status = "SCAN" if stats['is_scan'] else "OK"
        print(f"{filename[:50]:<50} | {status:<6} | {stats['avg_chars']:6.1f} | {stats['avg_alpha']:6.1f} | {stats['alpha_ratio']:6.2f} | {stats['garble_ratio']:6.2f} | {stats['junk_word_ratio']:6.2f} | {stats['repeat_ratio']:6.2f} | {stats['pages']:>3}")

        total_files += 1
        if stats['is_scan']:
            scan_count += 1

        summary.append({
            'filename': filename,
            'status': status,
            'avg_chars': stats['avg_chars'],
            'avg_alpha': stats['avg_alpha'],
            'alpha_ratio': stats['alpha_ratio'],
            'garble_ratio': stats['garble_ratio'],
            'junk_word_ratio': stats['junk_word_ratio'],
            'repeat_ratio': stats['repeat_ratio'],
            'pages': stats['pages']
        })

    print("\n--- SUMMARY ---")
    print(f"Total PDF files:    {total_files}")
    print(f"Likely scans:       {scan_count}")
    print(f"Likely digital:     {total_files - scan_count}")

    # Save to CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=summary[0].keys())
        writer.writeheader()
        writer.writerows(summary)
    print(f"\nResults saved to: {output_csv}")

# Run it
scan_check("../noaa-files")

