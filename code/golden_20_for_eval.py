# File System
import os
import random
import csv
import time
import sys
from datetime import datetime
from dotenv import load_dotenv

# Converting PDF to Text
import pymupdf4llm 
import pytesseract 
from pdf2image import convert_from_path
import re
import fitz

# Calling OpenAI
from openai import OpenAI, OpenAIError

import os

def select_specific_files(directory_path):
    specific_files = [
        "2025COG-1.pdf",
        "2020TXPGCD Weather Modification Initial Report.pdf",
        "2023UTNU-1.pdf",
        "2020IDCCSN-2_ClarkCo. NOAA Final Report.pdf",
        "2023UTCS-1.pdf",
        "2023UTSCS-1.pdf",
        "2025COSV-1.pdf",
        "2024IDCC-1.pdf",
        "Grand Mesa Project_07-1356_12.01.2006-05.31.2007.pdf",
        "2020TXSWMA_STWMA_2020.pdf",
        "Telluride - San Miguel Drainage Program_02-1114_11.01.2001-03.31.2002.pdf",
        "Purgatory - Western San Juan Program_04-1212_11.01.2003-03.31.2004.pdf",
        "San Gabriel Mountains_15-1675_12.15.2015-04.15.2016.pdf",
        "Walker Basin Project_16-1685_11.01.2015-07.31.2016.pdf",
        "StanislausWeatherMod_6_2018-2019.pdf",
        "2022IDBRB-1.pdf",
        "Snowbird_ Alta, Utah_01-1062_10.15.2000-12.31.2000.pdf",
        "Eatsern Sierra_03-1149_01.01.2003-12.31.2003.pdf",
        "Telluride - San Miguel Drainage Program_04-1211_11.01.2003-03.31.2004.pdf",
        "Tuscarora Project Previous File No. 01-1072_02-1103_10.18.2001-09.30.2002.pdf"
    ]

    all_files = set(os.listdir(directory_path))
    found_files = [f for f in specific_files if f in all_files]
    missing_files = [f for f in specific_files if f not in all_files]

    print(f"\nSelected {len(found_files)} specific files from {directory_path}:")
    for i, file in enumerate(found_files, 1):
        print(f"{i}. {file}")
    
    if missing_files:
        print("\nWarning: The following files were not found in the directory:")
        for f in missing_files:
            print(f" - {f}")

    return found_files

def save_to_csv(results, output_file, fieldnames):
    file_exists = os.path.isfile(output_file)
    with open(output_file, mode='a', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerows(results)

def extract_form_174_and_174A_text(file_path):
    data = {'pymu_text': '', 'ocr_text': ''}

    def check_form_17_4(text):
        keywords = ["initial", "report", "weather", "modification", "noaa", "form"]
        text_lower = text.lower()
        return all(keyword in text_lower for keyword in keywords)
    
    def check_form_17_4_A(text):
        keywords = ["interim", "final", "report", "activity", "weather", "modification", "noaa", "form"]
        text_lower = text.lower()
        return all(keyword in text_lower for keyword in keywords)

    patterns = {
        '17-4': check_form_17_4,
        '17-4A': check_form_17_4_A
    }

    titles = {
        '17-4': "=== NOAA Form 17-4 (Initial Report) ===",
        '17-4A': "=== NOAA Form 17-4A (Final/Interim Report) ==="
    }

    def classify_form(text):
        for key, pattern_fn in patterns.items():
            if pattern_fn(text): 
                return key
        return None

    # --- PyMuPDF ---
    try:
        page_data = pymupdf4llm.to_markdown(doc=file_path, page_chunks=True)
        formatted_chunks = []
        for page_dict in page_data:
            text = page_dict["text"]
            form_type = classify_form(text)
            if form_type:
                formatted_chunks.append(f"{titles[form_type]}\n{text}")
        data['pymu_text'] = '\n\n'.join(formatted_chunks) if formatted_chunks else '[No content extracted]'
    except Exception as e:
        print(f"Error extracting pymu_text: {e}")
        data['pymu_text'] = '[Error extracting PDF-to-text]'

    # --- OCR ---
    try:
        images = convert_from_path(file_path)
        formatted_ocr = []
        for img in images:
            text = pytesseract.image_to_string(img, lang='eng').strip()
            form_type = classify_form(text)
            if form_type:
                formatted_ocr.append(f"{titles[form_type]}\n{text}")
        data['ocr_text'] = '\n\n'.join(formatted_ocr) if formatted_ocr else '[No content extracted]'
    except Exception as e:
        print(f"Error extracting OCR text: {e}")
        data['ocr_text'] = '[Error extracting OCR text]'

    return data

def parse_gpt_response(response_text):
    data = {
        'start_date': '',
        'end_date': '',
        'season': '',
        'target_area': '',
        'year': '',
        'state': '',
        'type_of_agent': '',
        'type_of_apparatus': '',
        'purpose': ''
    }
    
    keyword_mapping = {
        'start date for weather modification activity': 'start_date',
        'end date for weather modification activity': 'end_date',
        'season for weather modification activity': 'season',
        'target area location': 'target_area',
        'year for weather modification activity': 'year',
        'state': 'state',
        'type of agent': 'type_of_agent',
        'type of apparatus': 'type_of_apparatus',
        'purpose of project or activity': 'purpose'
    }

    # Process each line in the response
    lines = response_text.split('\n')
    for line in lines:
        line = line.strip().lower()  # Normalize to lowercase for matching
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()
            # Map to the correct dictionary key if keyword matches
            for keyword, dict_key in keyword_mapping.items():
                if keyword in key:
                    data[dict_key] = value
                    break

    return data

def process_file(file, file_path, client, llm_variant, llm_prompt):
    # STEP 1: CONVERT PDF TO TEXT FOR LLM
    try:
        text_data = extract_form_174_and_174A_text(file_path)
        pdf_text = f"""

        FILENAME: {file}

        === OCR EXTRACTION ===
        {text_data['ocr_text']}

        === PDF-TO-TEXT EXTRACTION ===
        {text_data['pymu_text']}
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
            response = client.chat.completions.create(
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
    input_directory = "../noaa-files"
    output_file = f"../dataset/test/golden-20.csv"
    files = select_specific_files(input_directory)
    fieldnames = [
        'filename', 'start_date', 'end_date', 'season',
        'target_area', 'year', 'state', 'type_of_agent', 'type_of_apparatus',
        'purpose'
    ]

    # OPEN AI
    load_dotenv()  
    api_key = os.getenv("OPENAI_API_KEY")
    client = OpenAI(api_key=api_key)

    # MODEL VARIANTS
    llm_variant = 'gpt-4o-mini' # cheapest
    # llm_variant = 'o3-mini' # second cheapest
    # llm_variant = 'gpt-4o' # third cheapest

    # PROMPT VARIANTS
    # see prompts folder
    llm_prompt = """
You are an expert in structured data extraction from NOAA weather modification reports.

You will be given:
- The filename of a Weather Modification Activity report (NOAA Forms 17-4 and 17-4A) which often contains year, state, and project name.
- Two versions of the report:
  1. Optical Character Recognition (OCR) extraction, using pytesseract.image_to_string
  2. PDF-to-text extraction, using pymupdf4llm.to_markdown

Your task is to extract the following fields as accurately and completely as possible by comparing both text versions, inferring context when values are not explicitly stated, and using the filename:

- START DATE FOR WEATHER MODIFICATION ACTIVITY:
- END DATE FOR WEATHER MODIFICATION ACTIVITY:
- SEASON FOR WEATHER MODIFICATION ACTIVITY:
- TARGET AREA LOCATION:
- YEAR FOR WEATHER MODIFICATION ACTIVITY:
- STATE:
- TYPE OF AGENT:
- TYPE OF APPARATUS:
- PURPOSE OF PROJECT OR ACTIVITY:

Instructions:
1. Resolve any discrepancies using context, logical inference, and cross-referencing both text sources.
2. Use the filename to infer YEAR, STATE, and START/END DATE information. For example:
   - 2018UTNORT-1.pdf >>> 2018, Utah
   - Boise River, Idaho_07-1382_11.01.2007-03.31.2008.pdf >>> 2008, Idaho, 11/01/2007, 03/31/2008
3. Avoid using the NOAA headquarters location in Maryland as the target area location for the weather modification activity.
4. Use START DATE, END DATE, and PURPOSE to infer SEASON based on U.S. norms.
5. Translate dates to consistent formatting: mm/dd/yyy
6. For the YEAR, choose the single year the activity mostly occurred in. Do not use a range.
7. Use the following to classify:
   - AGENT: silver iodide, carbon dioxide, sodium chloride, urea, other, or combinations.
   - APPARATUS: ground, airborne, or combination.
   - SEASON: winter, spring, summer, fall.

Return only the 9 fields above. Do not include commentary or placeholders. Leave fields blank if truly unknowable.
"""

    # MAIN LOOP
    results = []
    for i, file in enumerate(files, 1):
        full_path = os.path.join(input_directory, file)
        try:
            result = process_file(file, full_path, client, llm_variant, llm_prompt)
            if result:
                results.append(result)
        except Exception as e:
            print(f"Error processing {file}: {e}")
    
    if results:
        save_to_csv(results, output_file, fieldnames)

if __name__ == "__main__":
    main()