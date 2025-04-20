# === MAIN LLM EXTRACTOR FILE === 
# This file reads all NOAA files, processes them in batches, and saves progress.

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

# Form 17-4 Key Phrases
FORM_17_4_KEY_PHRASES = [
    "initial report on weather modification",
    "project or activity designation",
    "purpose of project",
    "target",
    "dates of project",
    "description of weather modification"
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
    return all(phrase in text_lower for phrase in FORM_17_4_KEY_PHRASES)

def extract_pdf_text(file_path, llm_whisper_client):
    # PyMuPDF
    try:
        doc = pymupdf.open(file_path)
        text = doc[0].get_text().strip()
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
            text = pytesseract.image_to_string(images[0], lang='eng').strip()
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
            pages_to_extract="1", 
            lang='eng',
            wait_for_completion=True,
            wait_timeout=200
        )
        text = result['extraction'].get('result_text', '[No result_text found]')
        if len(text) > 500:
            method_counter['llm-whisper'] += 1
            return {'pdf_text': text}
        else:
            print('LLM Whisperer Failed. [No content extracted].')
            method_counter['failed'] += 1
    except Exception as e:
        print(f"LLM Whisperer failed: {e}")

    return {'pdf_text': '[No content extracted]'}

def parse_gpt_response(response_text):
    data = {
        'year': '',
        'season': '',
        'state': '',
        'agent': '',
        'apparatus': '',
        'purpose': '',
        'target_area': '',
        'start_date': '',
        'end_date': ''
    }
    
    keyword_mapping = {
        'year': 'year',
        'season': 'season',
        'state': 'state',
        'agent': 'agent',
        'apparatus': 'apparatus',
        'purpose': 'purpose',
        'target area': 'target_area',
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
        return None

    # DEBUG PDF TEXT
    # print(pdf_text)
    # sys.exit()

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
            print(f"OpenAI API error (attempt {attempt + 1} of {retries}): {str(e)}")
        except Exception as e:
            print(f"Unexpected error calling OpenAI (attempt {attempt + 1} of {retries}): {str(e)}")

        time.sleep(backoff)
        backoff *= 2 

    if not response_text:
        print(f"Skipping {file_path} after {retries} failed attempts.")
        return None
    
    # DEBUG LLM RESPONSE
    # print(response_text)

    # STEP 3: PARSE LLM RESPONSE INTO STRUCTURED DATA
    parsed_data = parse_gpt_response(response_text)
    parsed_data['filename'] = os.path.basename(file_path)
    
    # DEBUG PARSED DATA
    # print(parsed_data)
    # sys.exit()

    return parsed_data

def main():
    # FILE SYSTEM
    input_directory = "../accuracy-evals/golden-50"
    output_file = f"../dataset/test/golden-50-o3-prompt-C.csv"
    checkpoint_file = "../dataset/test/processed_files.txt"
    processed_files = load_processed_files(checkpoint_file)
    all_files = [
        f for f in os.listdir(input_directory)
        if os.path.isfile(os.path.join(input_directory, f)) and f.lower().endswith('.pdf')
    ]
    files_to_process = [f for f in all_files if f not in processed_files]
    fieldnames = [
        'filename', 
        'year', 
        'season', 
        'state', 
        'agent', 
        'apparatus', 
        'purpose', 
        'target_area', 
        'start_date', 
        'end_date'
    ]

    # OPEN AI
    load_dotenv()  
    api_key = os.getenv("OPENAI_API_KEY")
    gpt_client = OpenAI(api_key=api_key)

    # llm_variant = 'gpt-4.1' # BEST SO FAR
    # llm_variant = 'gpt-4.1-mini'
    # llm_variant = 'o4-mini'
    llm_variant = 'o3' 

    llm_prompt = f"""
# NOAA Weather Modification Report Extraction Expert

You are an expert data extractor specialized in parsing historical NOAA Weather Modification Activity reports. For each report, utilize the PDF-converted text and filename to extract 9 critical fields.

## Instructions

For each field, carefully analyze all available information using a step-by-step reasoning process. Clearly document your reasoning, resolve conflicting or ambiguous information logically, and then provide your final extracted value using the Final Extracted Fields Format.

## Explanation of Fields

The fields you must extract are detailed below, with guidelines for each:

1. **YEAR OF WEATHER MODIFICATION ACTIVITY:** Identify the single year when most activity occurred. If the activity spans two calendar years (e.g., winter season), prefer the latter year only. Utilize year information in the filename when present.

2. **SEASON OF WEATHER MODIFICATION ACTIVITY:** Determine a single season (winter, spring, summer, or fall) based on dates and project purpose. Choose the season with the most activity.

3. **U.S. STATE THAT WEATHER MODIFICATION ACTIVITY TOOK PLACE IN:** Identify the U.S. state explicitly or from geographic details if necessary. Utilize state information in the filename when present.

4. **TYPE OF CLOUD SEEDING AGENT USED:** Identify chemical or material agents used (e.g., silver iodide). Provide multiple agents as comma-separated values in lowercase when applicable.

5. **TYPE OF APPARATUS USED TO DEPLOY AGENT:** Classify clearly as ground, airborne, or both (e.g., "ground, airborne").

6. **PURPOSE OF PROJECT OR ACTIVITY:** Concisely summarize the project's primary goal (e.g., augment snowpack, increase rain). Provide multiple purposes as comma-separated values in lowercase if applicable.

7. **TARGET AREA LOCATION:** Specify the geographical region targeted (e.g., river basin, mountain range, ski resort).

8. **START DATE OF WEATHER MODIFICATION ACTIVITY:** Extract or infer from the text or filename in mm/dd/yyyy format.

9. **END DATE OF WEATHER MODIFICATION ACTIVITY:** Extract or infer from the text or filename in mm/dd/yyyy format.

## Example Chain-of-Thought Reasoning

**YEAR OF WEATHER MODIFICATION ACTIVITY:**
- Filename: `2018UTNORT-1.pdf`
- Filename starts with `2018`, clearly indicating the year.
- No conflicting year mentioned elsewhere.
- Final inference: **2018**

- Filename: `Eastern Sierra_00-1038_01.01.2000-12.31.2000.pdf`
- Date range ends on `12.31.2000`, confirming the relevant year.
- Final inference: **2000**

**U.S. STATE:**
- Filename: `2018UTNORT-1.pdf`
- Segment `UT` matches official USPS state code for Utah.
- No conflicting geographic indicators.
- Final inference: **utah**

- Filename: `Eastern Sierra_00-1038_01.01.2000-12.31.2000.pdf`
- "Eastern Sierra" region is well-known in California.
- Requires geographic domain knowledge.
- Final inference: **california**

**TYPE OF CLOUD SEEDING AGENT USED:**
- Extracted text explicitly mentions: "silver iodide released using ground-based generators."
- Clearly identified agent.
- No additional agents mentioned.
- Final inference: **silver iodide**

**TYPE OF APPARATUS USED TO DEPLOY AGENT:**
- Extracted text explicitly mentions: "silver iodide dispersed by aircraft-mounted flares."
- Indicates airborne apparatus.
- Final inference: **airborne**

- Text explicitly mentions: "combination of aircraft and ground-based generators."
- Indicates both apparatus types.
- Final inference: **ground, airborne**

[Continue similarly structured reasoning for all fields.]

## Final Extracted Fields Format

Present your final extracted results concisely as follows, in lowercase, comma-separating multiple values when applicable.
Do not include commentary, explanations, or placeholders. Leave fields blank if truly unknowable after exhausting all inference methods.

YEAR: [extracted value]  
SEASON: [extracted value]  
STATE: [extracted value]  
AGENT: [extracted value]  
APPARATUS: [extracted value]  
PURPOSE: [extracted value]  
TARGET AREA: [extracted value]  
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
            print(f"Error processing {file}: {e}")
        
        if i % 5 == 0 or i == len(files_to_process):
            save_to_csv(results, output_file, fieldnames)
            print(f"Saved {i} processed files to {output_file}")
            print(f"PDF extraction methods used: {dict(method_counter)}")
            save_method_counts(method_counter, '../dataset/test/pdf_method_counts.txt')
            results = []

    if results:
        save_to_csv(results, output_file, fieldnames)
        print(f"Final batch saved ({len(results)} files) to {output_file}")

    print(f"Processing complete. Final results saved to {output_file}")
    print(f"PDF extraction methods used: {dict(method_counter)}")
    save_method_counts(method_counter, '../dataset/test/pdf_method_counts.txt')

if __name__ == "__main__":
    main()