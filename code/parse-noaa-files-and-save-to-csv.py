# File System
import os
import random
import csv
import time
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
    data = {'pymu_text':'','ocr_text':''}
    try:
        pymu_text = pymupdf4llm.to_markdown(file_path, page_chunks=True, write_images=True, image_path='../results/img')
        data['pymu_text'] = '\n'.join(chunk['text'] for chunk in pymu_text[:3])
    except Exception as e:
        print(f"Error extracting pymu_text: {e}")

    try:
        images = convert_from_path(file_path)
        if images:  
            extracted_text = [pytesseract.image_to_string(img, lang='eng') for img in images[:3]]
            data['ocr_text'] = '\n'.join(extracted_text)
    except Exception as e:
        print(f"Error extracting OCR text: {e}")
    return data

def parse_gpt_response(response_text):
    # Create a dictionary to store extracted information    
    data = {
        # Primary Information
        'start_date': '',
        'end_date': '',
        'season': '',
        'target_area': '',
        'year': '',
        'state': '',
        'type_of_agent': '',
        'type_of_apparatus': '',
        # Secondary Information
        'purpose': '',
        'description': '',
        'control_area': '',
        'total_amount_of_agent_used': '',
        'total_mod_days': ''
    }
    
    # Mapping of keywords to dictionary keys
    keyword_mapping = {
        # Primary Information
        'start date for weather modification activity': 'start_date',
        'end date for weather modification activity': 'end_date',
        'season for weather modification activity': 'season',
        'target area location': 'target_area',
        'year for weather modification activity': 'year',
        'state': 'state',
        'type of agent': 'type_of_agent',
        'type of apparatus': 'type_of_apparatus',
        # Secondary Information
        'purpose of project or activity': 'purpose',
        'description of weather modification': 'description',
        'control area location': 'control_area',
        'total amount of agent used': 'total_amount_of_agent_used',
        'total number of modification days': 'total_mod_days'
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

def process_file(file_path, client, llm_variant, llm_prompt):
    # STEP 1: CONVERT PDF TO TEXT FOR LLM
    try:
        text_data = extract_pdf_text(file_path)
        pdf_text = text_data['pymu_text'] +'\n'+ text_data['ocr_text']
    except Exception as e:
        print(f"Error extracting text from PDF : {str(e)}")
        return None

    # STEP 2: CALL OPRN AI TO EXTRACT KEY INFORMATION
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
    
    # STEP 3: PARSE LLM RESPONSE INTO STRUCTURED DATA
    parsed_data = parse_gpt_response(response_text)
    parsed_data['noaa_pdf'] = os.path.basename(file_path)
    parsed_data['file_size(KB)'] = round(os.path.getsize(file_path) / 1024)
    
    return parsed_data

def main():
    # FILE SYSTEM
    input_directory = "../all-files-starting-thru-2024/"
    output_file = f"../results/FINAL.csv"
    checkpoint_file = "../results/processed_files.txt"
    processed_files = load_processed_files(checkpoint_file)
    all_files = [f for f in os.listdir(input_directory) if os.path.isfile(os.path.join(input_directory, f))]
    files_to_process = [f for f in all_files if f not in processed_files]
    fieldnames = [
        'noaa_pdf', 'file_size(KB)', 'start_date', 'end_date', 'season',
        'target_area', 'year', 'state', 'type_of_agent', 'type_of_apparatus',
        'purpose', 'description', 'control_area', 'total_amount_of_agent_used', 'total_mod_days'
    ]

    # OPEN AI
    load_dotenv()  
    api_key = os.getenv("OPENAI_API_KEY")
    llm_variant = 'gpt-4o-mini' 
    llm_prompt = f"""
INSTRUCTIONS:
You are an expert at structured data extraction from historic weather modification reports.  
You will be given unstructured text from NOAA Form 17-4 Weather Modification Reports, and your task is to extract the key information using cross-validation to ensure accuracy.

Use the DESCRIPTION OF WEATHER MODIFICATION APPARATUS, MODIFICATION AGENTS AND THEIR DISPERSAL RATES, THE TECHNIQUES EMPLOYED, ETC. to cross-check and validate information extracted.  
When the Target Area Location references a figure, map, or image, default to the Sponsor City and State as the Target Area Location. Ignore images and characters resulting from image encoding that may be present.
Check the PROJECT OR ACTIVITY DESIGNATION and the DESCRIPTION OF WEATHER MODIFICATION for a location when needed.
Use the location information to infer the U.S. STATE of the weather modification activity.
Use the START DATE FOR WEATHER MODIFICATION ACTIVITY, END DATE FOR WEATHER MODIFICATION ACTIVITY, and other contextual information to infer the YEAR (e.g. 2016) and SEASON (e.g. Winter, Spring, Summer, Fall) of the target weather modification activity. Note that all years will be after 1999.
The type of weather modification agent can include: silver iodide, carbon dioxide, urea, sodium chloride, other, and combinations of each.
The type of weather modification apparatus can include: ground, airborne, or a combination of both.
If you are not confident in the extracted value, you can label it as undetermined.

EXTRACTION SOURCES:
- The NOAA Form 17-4 Weather Modification Report text, using pdf-to-text recognition.
- The same The NOAA Form 17-4 Weather Modification Report text, using optical character recognition (OCR).

EXTRACT the following key information from the report using this format:
- START DATE FOR WEATHER MODIFICATION ACTIVITY: [Insert date] 
- END DATE FOR WEATHER MODIFICATION ACTIVITY: [Insert date] 
- SEASON FOR WEATHER MODIFICATION ACTIVITY: [Insert season] 
- TARGET AREA LOCATION: [Insert target location]  
- YEAR FOR WEATHER MODIFICATION ACTIVITY: [Insert year, yyyy] 
- STATE: [Insert U.S. state] 
- TYPE OF AGENT: [Insert type of agent]
- TYPE OF APPARATUS: [Insert type of weather modification apparatus] 
- PURPOSE OF PROJECT OR ACTIVITY: [Insert purpose] 
- TOTAL AMOUNT OF AGENT USED: [Insert amount in grams]  
- TOTAL NUMBER OF MODIFICATION DAYS: [Insert days]

ANALYSIS INSTRUCTIONS:
1. Carefully review both text extractions.
2. Cross-reference information between extraction methods to improve accuracy.
3. Resolve any discrepancies by using contextual clues.
4. Ensure maximum information preservation.
    """

    client = OpenAI(api_key=api_key)
    
    # MAIN LOOP
    results = []
    for i, file in enumerate(files_to_process, 1):
        full_path = os.path.join(input_directory, file)
        try:
            result = process_file(full_path, client, llm_variant, llm_prompt)
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