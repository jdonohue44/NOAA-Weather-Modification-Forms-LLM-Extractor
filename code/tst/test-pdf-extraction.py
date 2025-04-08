# File System
import os
import random
import csv
import sys
from datetime import datetime
from dotenv import load_dotenv
import re

# Converting PDF to Text
import pymupdf4llm 
import pytesseract 
from pdf2image import convert_from_path

# Used for random sampling for accuracy evaluations
def select_random_files(directory_path, n):
    all_files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
    n = min(n, len(all_files))
    selected_files = random.sample(all_files, n)
    print(f"\nSelected {n} random files from {directory_path}:")
    for i, file in enumerate(selected_files, 1):
        print(f"{i}. {file}")
    return selected_files

def select_all_files(directory_path):
    all_files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
    n = len(all_files)
    print(f"\nSelected {n} files from {directory_path}:")
    for i, file in enumerate(all_files, 1):
        print(f"{i}. {file}")
    return all_files

def clean_extracted_text(text):
    # Remove markdown-style image references and markdown headings
    text = re.sub(r'!\[.*?\]\(.*?\)', '', text)
    text = re.sub(r'#\s{0,6}', '', text)
    
    # Remove sequences with mostly special characters (common OCR junk)
    text = re.sub(r'^[^\w\s]{3,}$', '', text, flags=re.MULTILINE)
    
    # Remove lines with very low alphanumeric density (OCR artifact heuristic)
    text = re.sub(r'^(?:(?:[^\w]*\W{1,}){5,}.*?|.{1,2})$', '', text, flags=re.MULTILINE)
    
    # Remove repeated special character patterns and isolated single characters
    text = re.sub(r'\b[^\w\s]{1,3}\b', '', text)
    
    # Remove lines with excessively complex formatting
    text = re.sub(r'[*/^~|=_\-]{3,}', '', text)
    
    # Remove patterns with backslashes, underscores, or other visual markers
    text = re.sub(r'[_\\]{2,}', '', text)
    
    # Normalize and clean up newlines and excessive spaces
    text = re.sub(r'\n+', '\n', text)
    text = re.sub(r'\s+', ' ', text).strip()

    return text

def extract_pdf_text(file_path):
    data = {
        'pdf_text':'',
        'pdf_type':'' # text or scan
    }
    try: # pymupdf4llm for text-based PDFs
        markdown_text = pymupdf4llm.to_markdown(file_path, page_chunks=True, write_images=True, image_path='../results/img')
        print(markdown_text[0]['text'])
        cleaned_text = clean_extracted_text(markdown_text[0]['text'])
        if cleaned_text and len(cleaned_text.strip()) > 100:
            data['pdf_text'] = cleaned_text
            data['pdf_type'] = 'text'
            # print(markdown_text)
            return data
    except Exception as e:
        print(f"Error: {e}")
    
    # try: # pytesseract for scanned files
    #     images = convert_from_path(file_path)
    #     extracted_text = []
    #     for img in images:
    #         page_text = pytesseract.image_to_string(img, lang='eng')
    #         extracted_text.append(page_text)
    #     full_text = '\n'.join(extracted_text)
    #     if full_text and len(full_text.strip()) > 100:
    #         data['pdf_text'] = full_text
    #         data['pdf_type'] = 'scan'
    #         print(full_text)
    #         return data

    except Exception as e:
        print(f"Error: {e}")

    raise ValueError(f"Could not extract text from PDF: {file_path}")

def process_file(file_path):
    # STEP 1: CONVERT PDF TO TEXT FOR LLM
    try:
        text_data = extract_pdf_text(file_path)
        pdf_text = text_data['pdf_text']
    except Exception as e:
        print(f"Error extracting text from PDF : {str(e)}")

def main():

    directory = "../all-files-starting-thru-2024/"
    num_files = 1
    
    selected_files = select_random_files(directory, num_files)
    
    # Process each file and append results
    for file in selected_files:
        full_path = os.path.join(directory, file)
        process_file(full_path)


if __name__ == "__main__":
    main()