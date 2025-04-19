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

# def save_method_counts(counter, file_path):
#     os.makedirs(os.path.dirname(file_path), exist_ok=True)
#     with open(file_path, "w") as f:
#         f.write("PDF extraction methods used:\n")
#         for method, count in counter.items():
#             f.write(f"{method}: {count}\n")
#     print(f"Method counts saved to {file_path}")

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
        'year of weather modification activity': 'year',
        'season of weather modification activity': 'season',
        'u.s. state that weather modification activity is taking place': 'state',
        'type of cloud seeding agent': 'agent',
        'type of apparatus': 'apparatus',
        'purpose of project or activity': 'purpose',
        'target area location': 'target_area',
        'start date of weather modification activity': 'start_date',
        'end date of weather modification activity': 'end_date'
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
    # sys.exit()

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
    output_file = f"../dataset/test/golden-50-gpt-4.1-mini-prompt-A.csv"
    checkpoint_file = "../dataset/test/processed_files.txt"
    processed_files = load_processed_files(checkpoint_file)
    all_files = [f for f in os.listdir(input_directory) if os.path.isfile(os.path.join(input_directory, f))]
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

    # llm_variant = 'gpt-4o-mini' 
    llm_variant = 'gpt-4.1-mini'

    # llm_variant = 'o3-mini'
    # llm_variant = 'o4-mini'
    
    # llm_variant = 'gpt-4o' 
    # llm_variant = 'gpt-4.1' 

    llm_prompt = f"""
# NOAA Weather Modification Report Extraction Expert
You are specialized in extracting structured data from NOAA Weather Modification Activity reports with high precision. 
Your task is to extract 9 specific fields about the weather modification project.

## Input Sources
You will be given:
1. Filename of Weather Modification Report, which will contain year, state, and geographic identifiers.
2. Extracted PDF text of the Weather Modification Report. 

## Required Output Fields
Extract these nine fields precisely and ensure values are lowercase, comma-separated if multiple (e.g. "silver iodide, carbon dioxide"): 

1. YEAR OF WEATHER MODIFICATION ACTIVITY
2. SEASON OF WEATHER MODIFICATION ACTIVITY
3. U.S. STATE THAT WEATHER MODIFICATION ACTIVITY IS TAKING PLACE
4. TYPE OF CLOUD SEEDING AGENT
5. TYPE OF APPARATUS
6. PURPOSE OF PROJECT OR ACTIVITY
7. TARGET AREA LOCATION
8. START DATE OF WEATHER MODIFICATION ACTIVITY
9. END DATE OF WEATHER MODIFICATION ACTIVITY

## Extraction Guidelines
1. Cross-validate information in the report using DESCRIPTION OF WEATHER MODIFICATION APPARATUS, MODIFICATION AGENTS AND THEIR DISPERSAL RATES, TECHNIQUES EMPLOYED, ETC.
2. Use the filename to infer YEAR and STATE information. For example:
   - 2018UTNORT-1.pdf >>> 2018, Utah
   - 2019COCMRB_CenCOMtnRvrBasings17-4.pdf >>> 2019, Colorado
   - 2017IDCCSN[17-1691]ClarkCo_noaa17-18.pdf >>> 2017, Idaho
   - Boise River, Idaho_07-1382_11.01.2007-03.31.2008.pdf >>> 2008, Idaho
   - San Joaquin River_05-1278_01.01.2005-12.31.2005.pdf >>> 2005, California
   - Eastern Sierra_00-1038_01.01.2000-12.31.2000.pdf >>> 2000, California
   - Kings River_06-1327_01.01.2006-12.31.2006.pdf >>> 2006, California
2. For dates:
   - Convert all formats to mm/dd/yyyy
   - When only partial dates appear, infer missing components from context
   - Fiscal years should be converted to calendar date ranges
3. For season determination:
   - winter: Dec-Feb (snow augmentation, etc.)
   - spring: Mar-May (flood mitigation, agriculture)
   - summer: Jun-Aug (hail suppression, rainfall enhancement)
   - fall: Sep-Nov (reservoir recharge, etc.)
4. For agent classification:
   - Primary: silver iodide, sodium chloride
5. For apparatus classification:
   - ground: generators, flares, or dispensers at fixed locations
   - airborne: aircraft-mounted equipment, often flares
   - ground, airborne: when both methods are employed
6. Target area specificity:
   - Include watershed names, mountain ranges, counties, or water bodies
   - Exclude NOAA headquarters addresses (Silver Spring, MD) or operator addresses

## Response Format
Return only the extracted field values in a clean key-value format without explanations, uncertainties, or placeholders.
Ensure values are lowercase.
Comma-separate multiple values (e.g. "silver iodide, carbon dioxide"), (e.g. "augment snowpack, increase rain"), (e.g. "ground, airborne").

YEAR OF WEATHER MODIFICATION ACTIVITY: [extracted value]
SEASON OF WEATHER MODIFICATION ACTIVITY: [extracted value]
U.S. STATE THAT WEATHER MODIFICATION ACTIVITY IS TAKING PLACE: [extracted value]
TYPE OF CLOUD SEEDING AGENT: [extracted value]
TYPE OF APPARATUS: [extracted value]
PURPOSE OF PROJECT OR ACTIVITY: [extracted value]
TARGET AREA LOCATION: [extracted value]
START DATE OF WEATHER MODIFICATION ACTIVITY: [extracted value]
END DATE OF WEATHER MODIFICATION ACTIVITY: [extracted value]

Return only the 9 fields above. Do not include commentary, explanations, or placeholders. Leave fields blank if truly unknowable after exhausting all inference methods.
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