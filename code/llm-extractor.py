# === MAIN LLM EXTRACTOR FILE === 
# This file reads all NOAA files, processes them in batches, and saves progress.
import random 

# File System
import os
import csv
import time
import sys
from datetime import datetime
from dotenv import load_dotenv

# PDF to Text
import pymupdf
import pytesseract 
from pdf2image import convert_from_path
from unstract.llmwhisperer import LLMWhispererClientV2

# OpenAI
from openai import OpenAI, OpenAIError

# Count PDF conversion usage
from collections import Counter
method_counter = Counter()

# Form 17-4 Key Phrases. All must be present to proceed with PyMuPDF or pytesseract as the text extraction method. 
FORM_17_4_KEY_PHRASES = [
    "initial report on weather modification",
    "project or activity designation",
    "purpose of project or activity",
    "sponsor",
    "operator",
    "target and control areas",
    "target area",
    "control area",
    "dates of project",
    "date first actual weather modification",
    "expected termination date",
    "description of weather modification",
    "affiliation"
]

def select_all_files(directory_path):
    all_files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
    n = len(all_files)
    print(f"\nSelected {n} files from {directory_path}:")
    for i, file in enumerate(all_files, 1):
        print(f"{i}. {file}")
    return all_files

def load_processed_files(checkpoint_file):
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, "r") as f:
            return set(f.read().splitlines())
    return set()

def save_processed_file(checkpoint_file, file_name):
    with open(checkpoint_file, "a") as f:
        f.write(file_name + "\n")

def save_to_csv(results, output_file, fieldnames):
    file_exists = os.path.isfile(output_file)
    with open(output_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(results)

def save_method_counts(counter, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "a") as f:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"\n--- Method counts at {timestamp} ---\n")
        for method, count in counter.items():
            f.write(f"{method}: {count}\n")
    print(f"Method counts appended to {file_path}")

def contains_all_phrases(text):
    text_lower = text.lower()
    missing_phrases = [phrase for phrase in FORM_17_4_KEY_PHRASES if phrase not in text_lower]
    if missing_phrases:
        print("Missing phrases:")
        for phrase in missing_phrases:
            print(f"  - {phrase}")
        return False
    return True

# Extract pdf text using three text extraction technologies via waterfall: (1) PyMuPDF (free, native text) --> (2) pytesseract (free, OCR) --> LLM Whisperer (paid, OCR+native)
def extract_pdf_text(file_path, llm_whisper_client):
    # PyMuPDF
    try:
        doc = pymupdf.open(file_path)
        text = doc[0].get_text().strip() # only process first page
        # DEBUG TEXT LENGTH
        print(len(text))
        if len(text) > 1000 and contains_all_phrases(text):
            method_counter['pymu'] += 1
            return {'pdf_text': text}
        else:
            print('PyMuPDF Failed. Trying OCR.')
    except Exception as e:
        print(f"pymupdf extraction failed: {e}")

    # OCR
    try:
        images = convert_from_path(file_path, first_page=1, last_page=1)
        if images:
            text = pytesseract.image_to_string(images[0], lang='eng').strip() # only process first page
            # DEBUG TEXT LENGTH
            print(len(text))
            if len(text) > 1000 and contains_all_phrases(text):
                method_counter['ocr'] += 1
                return {'pdf_text': text}
            else:
                print('OCR Failed. Trying LLM Whisperer.')
    except Exception as e:
        print(f"OCR failed: {e}")

    # LLM Whisperer
    try:
        result = llm_whisper_client.whisper(
            file_path=file_path,
            pages_to_extract="1", # only process first page
            lang='eng',
            wait_for_completion=True,
            wait_timeout=200
        )
        text = result['extraction'].get('result_text', '[No result_text found]')
        # DEBUG TEXT LENGTH
        print(len(text))
        if len(text) > 500:
            method_counter['llm-whisper'] += 1
            return {'pdf_text': text}
        else:
            print('LLM Whisperer Failed. [No content extracted].')
            method_counter['failed'] += 1
    except Exception as e:
        print(f"LLM Whisperer failed: {e}")

    raise RuntimeError("All PDF text extraction methods failed (PyMuPDF, OCR, LLM Whisperer)")

def parse_gpt_response(response_text):
    data = {
        'project': '',
        'year': '',
        'season': '',
        'state': '',
        'operator_affiliation': '',
        'agent': '',
        'apparatus': '',
        'purpose': '',
        'target_area': '',
        'control_area': '',
        'start_date': '',
        'end_date': ''
    }
    
    keyword_mapping = {
        'project': 'project',
        'year': 'year',
        'season': 'season',
        'state': 'state',
        'operator affiliation': 'operator_affiliation',
        'agent': 'agent',
        'apparatus': 'apparatus',
        'purpose': 'purpose',
        'target area': 'target_area',
        'control area': 'control_area',
        'start date': 'start_date',
        'end date': 'end_date'
    }

    lines = response_text.split('\n')
    for line in lines:
        line = line.strip().lower()
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()
            for keyword, dict_key in keyword_mapping.items():
                if keyword in key:
                    data[dict_key] = value
                    break

    return data

def process_file(file, file_path, llm_whisper_client, gpt_client, llm_variant, llm_prompt):
    print(f"\n=== PROCESSING: {file} ===")
    # STEP 1: CONVERT PDF TO TEXT FOR LLM
    try:
        text_data = extract_pdf_text(file_path, llm_whisper_client)
        pdf_text = f"""
        
        FILENAME: {file}

        === NOAA FORM 17-4: INITIAL REPORT ON WEATHER MODIFICATION ACTIVITIES ===

        {text_data['pdf_text']}

        """
    except Exception as e:
        print(f"Error extracting text from PDF : {str(e)}")
        raise e

    # DEBUG PDF TEXT
    # print(pdf_text)

    # STEP 2: CALL OPEN AI TO EXTRACT KEY INFORMATION
    retries = 2
    backoff = 10
    response_text = None
    for attempt in range(retries):
        try:
            response = gpt_client.chat.completions.create(
                model=llm_variant,
                messages=[
                    {"role": "system", "content": llm_prompt},
                    {"role": "user", "content": pdf_text}
                ]
            )
            response_text = response.choices[0].message.content
            break 
        except OpenAIError as e:
            last_error = e
            print(f"OpenAI API error (attempt {attempt + 1} of {retries}): {str(e)}")
        except Exception as e:
            last_error = e
            print(f"Unexpected error calling OpenAI (attempt {attempt + 1} of {retries}): {str(e)}")
        time.sleep(backoff)
        backoff *= 2 

    if not response_text:
        raise RuntimeError(f"OpenAI failed after {retries} attempts for {file_path}: {last_error}")

    
    # DEBUG LLM RESPONSE
    # print(response_text)

    # STEP 3: PARSE LLM RESPONSE INTO STRUCTURED DATA
    parsed_data = parse_gpt_response(response_text)
    parsed_data['filename'] = os.path.basename(file_path)
    
    # DEBUG PARSED DATA
    # print(parsed_data)

    return parsed_data

def main():
    # INPUT FILES
    input_directory = "../noaa-files"
    output_file = "../dataset/final/cloud_seeding_us_2000_2025.csv"
    checkpoint_file = "../dataset/final/processed-files.txt"

    # LOAD NOAA FILES TO PROCESS
    processed_files = load_processed_files(checkpoint_file)
    all_files = [
        f for f in os.listdir(input_directory)
        if os.path.isfile(os.path.join(input_directory, f)) and f.lower().endswith('.pdf')
    ]
    files_to_process = [f for f in all_files if f not in processed_files]
    fieldnames = [
        'filename', 
        'project', 
        'year', 
        'season', 
        'state', 
        'operator_affiliation',
        'agent', 
        'apparatus', 
        'purpose', 
        'target_area', 
        'control_area', 
        'start_date', 
        'end_date'
    ]

    # OPEN AI
    load_dotenv()  
    api_key = os.getenv("OPENAI_API_KEY")
    gpt_client = OpenAI(api_key=api_key)

    # ORDERED BY PERFORMANCE (notes on cost)
    # llm_variant = 'gpt-4o-mini' # 91.67% accuracy
    # llm_variant = 'gpt-4.1-mini' # 93.33% accuracy
    # llm_variant = 'gpt-4.1' # 93.33% accuracy
    # llm_variant = 'o4-mini' # 95.00% accuracy (BEST VALUE) (~$0.005 per document)
    llm_variant = 'o3' # 96.33% accuracy (BEST ACCURACY) (~$0.01 per document)

    llm_prompt = f"""
# NOAA Weather Modification Report Extraction Expert

You are an expert data extractor specialized in parsing historical NOAA Weather Modification Activity reports. For each report, utilize the PDF-converted text and filename to extract **12** critical fields.

## Instructions

For each field, carefully analyze all available information using a step-by-step reasoning process. Reason step-by-step internally, using evidence from the filename and report content to resolve conflicting or ambiguous information. 

Output only the final extracted fields using the format and rules provided below.

## Fields to Extract

1. **PROJECT OR ACTIVITY DESIGNATION:** Extract the full project name (e.g., kern river cloud seeding program, gunnison river basin cloud seeding program, western uintas cloud seeding program).

2. **YEAR OF WEATHER MODIFICATION ACTIVITY:** Identify the single year when most activity occurred. If the activity spans two calendar years (e.g., winter season), prefer the latter year only. Utilize year information in the filename when present.

3. **SEASON OF WEATHER MODIFICATION ACTIVITY:** Include the season that the weather modification activity took place (winter, spring, summer, or fall) based on project purpose, dates, and type of agent used. If the project spans multiple seasons, list each comma-separated season in lowercase. Use as minimal a list as possible while maintaining correctness.

4. **U.S. STATE THAT WEATHER MODIFICATION ACTIVITY TOOK PLACE IN:** Identify the single U.S. state where the weather modification took place. Utilize state information in the filename and geographic context in the report. Use lowercase and convert USPS codes to full names (e.g., "UT" to "utah").

5. **OPERATOR AFFILIATION:** Extract the company or organization that conducted the seeding operations (e.g. atmospherics inc, north american weather consultants, weather modification llc, deser research inc (DRI), western weather consultants, water enhancement authority). **Never include personal names**. Strip names and titles like “general manager” or “director”. Preserve company suffixes like “inc.”, “llc”, or “consultants”.

6. **TYPE OF CLOUD SEEDING AGENT USED:** Extract chemical or material agents used (e.g., silver iodide, calcium chloride, sodium iodide, ammonium iodide). Provide multiple agents as comma-separated values in lowercase when applicable.

7. **TYPE OF APPARATUS USED TO DEPLOY AGENT:** Classify apparatus as ground, airborne, or "ground, airborne" if both methods were employed to disperse the cloud seeding agent.

8. **PURPOSE OF PROJECT OR ACTIVITY:** Extract the project's main goal (e.g., augment snowpack, increase snowfall, increase rain, enhance precipitation, increase precipitation, increase runoff, suppress hail, research). Provide multiple purposes as comma-separated values in lowercase if applicable.

9. **TARGET AREA LOCATION:** Specify the geographical region targeted for the cloud seeding (e.g., specific county in the state, specific river basin, mountain range, ski resort). Avoid using "see map".

10. **CONTROL AREA LOCATION:** Specify the geographical region used as scientific control, if any (e.g., adjacent areas, river basin, various sites in utah, none, same as target area). Leave blank if not found, or marked as "none" or "NA". Avoid using "see map".

11. **START DATE OF WEATHER MODIFICATION ACTIVITY:** Extract directly from the text or infer from filename. Use **mm/dd/yyyy** format. If start date is not found in report text, infer from the filename.

12. **END DATE OF WEATHER MODIFICATION ACTIVITY:** Extract directly from the text or infer from filename. Use **mm/dd/yyyy** format. If end date is not found in report text, infer from the filename.

## Example Reasoning (Internal)

Use structured internal logic to infer each field. For example:

**PURPOSE:**
- Extracted text explicitly mentions: "purpose of project or activity" to be "Rain Enhancement".
- Clearly identified the purpose.
- Final inference: **enhance rain**

- Extracted text explicitly mentions: "purpose of project or activity" to be "Augment Snowpack".
- Clearly identified the purpose.
- Final inference: **augment snowpack**

**YEAR:**
- Filename: `2018UTNORT-1.pdf`
- Filename starts with `2018`, clearly indicating the year.
- No conflicting year mentioned elsewhere.
- Final inference: **2018**

- Filename: `Eastern Sierra_00-1038_01.01.2000-12.31.2000.pdf`
- Date range ends on `12.31.2000`, confirming the relevant year.
- Final inference: **2000**

**STATE:**
- Filename: `2018UTNORT-1.pdf`
- Segment `UT` matches official USPS state code for Utah.
- No conflicting geographic indicators.
- Final inference: **utah**

- Filename: `Eastern Sierra_00-1038_01.01.2000-12.31.2000.pdf`
- "Eastern Sierra" region is well-known in California.
- Requires geographic domain knowledge.
- Final inference: **california**

**AGENT:**
- Extracted text explicitly mentions: "silver iodide released using ground-based generators."
- Clearly identified agent.
- No additional agents mentioned.
- Final inference: **silver iodide**

**SEASON:**
- Filename: `Eastern Sierra_00-1038_01.01.2000-12.31.2000.pdf`
- Date range spans January to December, but weather modification in the Eastern Sierra typically targets snowpack in winter.
- Winter inference is supported by geographic and program context, as well as silver iodide seeding agent.
- Final inference: **winter**

- Filename: `Southern Ogallala Aquifer Rainfall (soar) Program_04-1241_04.01.2004-08.15.2004.pdf`
- Date range is April to August, aligning with the convective storm season in the Southern Plains of Texas.
- Hail and rainfall augmentation in Texas region is typically conducted in summer.
- Final inference: **summer**

**APPARATUS:**
- Extracted text explicitly mentions: "silver iodide dispersed by aircraft-mounted flares."
- Indicates airborne apparatus.
- Final inference: **airborne**

- Text explicitly mentions: "combination of aircraft and ground-based generators."
- Indicates both apparatus types.
- Final inference: **ground, airborne**

## Final Extracted Fields Format

Present your final extracted results concisely as follows, in lowercase, comma-separating multiple values when applicable.

Do not include commentary, explanations, or placeholders. Leave field blank if truly unknowable after exhausting all inference methods using the filename and text evidence.

PROJECT: [extracted value]
YEAR: [extracted value]  
SEASON: [extracted value]  
STATE: [extracted value]  
OPERATOR AFFILIATION: [extracted value]  
AGENT: [extracted value]  
APPARATUS: [extracted value]  
PURPOSE: [extracted value]  
TARGET AREA: [extracted value]  
CONTROL AREA: [extracted value]  
START DATE: [extracted value]  
END DATE: [extracted value]
"""
    
    # LLM Whisperer Client
    llm_whisper_client = LLMWhispererClientV2()

    # MAIN LOOP
    results = []
    for i, file in enumerate(files_to_process, 1):
        full_path = os.path.join(input_directory, file)
        try:
            result = process_file(file, full_path, llm_whisper_client, gpt_client, llm_variant, llm_prompt)
            if result:
                results.append(result)
                save_processed_file(checkpoint_file, file)
        except Exception as e:
            print(f"\n🛑 Critical error processing {file}: {e}")
            print("→ Saving progress and exiting safely...")
            # Save current batch before exit
            if results:
                save_to_csv(results, output_file, fieldnames)
                print(f"Partial batch saved ({len(results)} files) to {output_file}")
            save_method_counts(method_counter, '../dataset/final/pdf_method_counts.txt')
            sys.exit(1)
        
        if i % 5 == 0 or i == len(files_to_process):
            save_to_csv(results, output_file, fieldnames)
            print(f"Saved {i} processed files to {output_file}")
            print(f"PDF extraction methods used: {dict(method_counter)}")
            save_method_counts(method_counter, '../dataset/final/pdf_method_counts.txt')
            results = []

    if results:
        save_to_csv(results, output_file, fieldnames)
        print(f"Final batch saved ({len(results)} files) to {output_file}")

    print(f"Processing complete. Final results saved to {output_file}")
    print(f"PDF extraction methods used: {dict(method_counter)}")
    save_method_counts(method_counter, '../dataset/final/pdf_method_counts.txt')

if __name__ == "__main__":
    main()
