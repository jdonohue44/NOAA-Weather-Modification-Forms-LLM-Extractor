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

# Calling OpenAI
from openai import OpenAI, OpenAIError

# Used for random sampling to do accuracy evaluations
def select_random_files(directory_path, n):
    all_files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
    n = min(n, len(all_files))
    selected_files = random.sample(all_files, n)
    print(f"\nSelected {n} random files from {directory_path}:")
    for i, file in enumerate(selected_files, 1):
        print(f"{i}. {file}")
    return selected_files

# Used for processing all files 
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

def extract_pdf_text(file_path):
    data = {'pymu_text': '', 'ocr_text': ''}

    # IDEA: Look for page (chunk/img) which contains Form 17-4 and only include this one in the input to LLM

    # PDF-to-text extraction
    try:
        pymu_text = pymupdf4llm.to_markdown(file_path, page_chunks=True)

        extracted = [chunk['text'].strip() for chunk in pymu_text[:3] if len(chunk['text'].strip()) > 100]
        data['pymu_text'] = '\n\n'.join(extracted) if extracted else '[No content extracted]'
    except Exception as e:
        print(f"Error extracting pymu_text: {e}")
        data['pymu_text'] = '[Error extracting PDF-to-text]'

    # OCR extraction
    try:
        images = convert_from_path(file_path)
        if images:
            extracted_text = [pytesseract.image_to_string(img, lang='eng').strip() for img in images[:3]]
            cleaned = [t for t in extracted_text if len(t.strip()) > 100]
            data['ocr_text'] = '\n\n'.join(cleaned) if cleaned else '[No content extracted]'
        else:
            data['ocr_text'] = '[No images found]'
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
        text_data = extract_pdf_text(file_path)
        pdf_text = f"""FILENAME: {file}

        === OCR EXTRACTION (FIRST 3 PAGES) ===
        {text_data['ocr_text']}

        === PDF-TO-TEXT EXTRACTION (FIRST 3 PAGES) ===
        {text_data['pymu_text']}
        """
    except Exception as e:
        print(f"Error extracting text from PDF : {str(e)}")
        return None

    # DEBUG PDF TEXT
    print(pdf_text)
    sys.exit()

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
    input_directory = "../noaa-files/"
    output_file = f"../results/clous_seeding_us_2000_2025.csv"
    checkpoint_file = "../results/processed_files.txt"
    processed_files = load_processed_files(checkpoint_file)
    all_files = [f for f in os.listdir(input_directory) if os.path.isfile(os.path.join(input_directory, f))]
    files_to_process = [f for f in all_files if f not in processed_files]
    fieldnames = [
        'filename', 'start_date', 'end_date', 'season',
        'target_area', 'year', 'state', 'type_of_agent', 'type_of_apparatus',
        'purpose'
    ]

    # OPEN AI
    load_dotenv()  
    api_key = os.getenv("OPENAI_API_KEY")
    llm_variant = 'gpt-4o-mini' 
    llm_prompt = f"""
You are an expert in structured data extraction from NOAA weather modification reports.

You will be given:
- The filename of a Weather Modification Activity report (NOAA Forms 17-4 and 17-4A) which often contains year, state, and project name.
- Two versions of the first 3 pages of the report:
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
   - AGENT: silver iodide, carbon dioxide, sodium chloride, urea, other, or combinations. If other, include what it is.
   - APPARATUS: ground, airborne, or combination.
   - SEASON: winter, spring, summer, fall.

Return only the 9 fields above. Do not include commentary or placeholders. Leave fields blank if truly unknowable.
    """

    client = OpenAI(api_key=api_key)
    
    # MAIN LOOP
    results = []
    for i, file in enumerate(files_to_process, 1):
        full_path = os.path.join(input_directory, file)
        try:
            result = process_file(file, full_path, client, llm_variant, llm_prompt)
            if result:
                results.append(result)
                save_processed_file(checkpoint_file, file)
        except Exception as e:
            print(f"Error processing {file}: {e}")
        
        if i % 5 == 0 or i == len(files_to_process):
            save_to_csv(results, output_file, fieldnames)
            results = []
            print(f"Saved {i} processed files to {output_file}")

    if results:
        save_to_csv(results, output_file, fieldnames)
        print(f"Final batch saved ({len(results)} files) to {output_file}")

    print(f"Processing complete. Final results saved to {output_file}")

if __name__ == "__main__":
    main()