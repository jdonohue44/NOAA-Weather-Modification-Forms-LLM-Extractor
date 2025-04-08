# PDF Text Extraction for Comprehensive LLM Processing
import os
import logging
import re
import json
from dotenv import load_dotenv

# PDF Extraction Libraries
import pymupdf4llm 
import pytesseract 
from pdf2image import convert_from_path
from PIL import Image, ImageEnhance, ImageFilter

def preprocess_image(image):
    """Preprocess image to improve OCR accuracy."""
    image = image.convert('L')
    enhancer = ImageEnhance.Contrast(image)
    image = enhancer.enhance(2.0)
    image = image.filter(ImageFilter.SHARPEN)
    return image

def clean_extracted_text(text):
    """Clean and normalize extracted text."""
    # Remove markdown, special characters, and normalize whitespace
    text = re.sub(r'!\[.*?\]\(.*?\)', '', text)
    text = re.sub(r'#\s{0,6}', '', text)
    text = re.sub(r'^[^\w\s]{3,}$', '', text, flags=re.MULTILINE)
    text = re.sub(r'\b[^\w\s]{1,3}\b', '', text)
    text = re.sub(r'\n+', '\n', text)
    return ' '.join(text.split()).strip()

def extract_pdf_text(file_path):
    """
    Extract text using multiple methods.
    
    Returns:
        dict with extracted texts from different methods
    """
    extraction_results = {
        'pymupdf_text': '',
        'pytesseract_text': '',
        'file_path': file_path
    }
    
    # pymupdf extraction
    try:
        markdown_text = pymupdf4llm.to_markdown(
            file_path, 
            write_images=True, 
            image_path='../results/img'
        )
        extraction_results['pymupdf_text'] = clean_extracted_text(markdown_text)
    except Exception as e:
        logging.warning(f"pymupdf extraction failed: {e}")
    
    # pytesseract extraction
    try:
        images = convert_from_path(file_path)
        extracted_text = []
        
        for img in images:
            preprocessed_img = preprocess_image(img)
            page_text = pytesseract.image_to_string(
                preprocessed_img, 
                lang='eng',
                config='--psm 6 --oem 3'
            )
            extracted_text.append(page_text)
        
        full_text = '\n'.join(extracted_text)
        extraction_results['pytesseract_text'] = clean_extracted_text(full_text)
    except Exception as e:
        logging.error(f"Pytesseract extraction failed: {e}")
    
    return extraction_results

def prepare_llm_prompt(extraction_results):
    """
    Prepare a comprehensive prompt for LLM processing of weather modification reports.
    """
    prompt = f"""INSTRUCTIONS:
You are an expert at structured data extraction from historic weather modification reports.  
You will be given unstructured text from NOAA Form 17-4 Weather Modification Reports, and your task is to extract the key information, ensuring accuracy.
Use the DESCRIPTION OF WEATHER MODIFICATION APPARATUS, MODIFICATION AGENTS AND THEIR DISPERSAL RATES, THE TECHNIQUES EMPLOYED, ETC. to cross-check and validate information extracted.  
When the Target Area Location references a figure, map, or image, default to the Sponsor City and State as the Target Area Location. Also check the PROJECT OR ACTIVITY DESIGNATION and the DESCRIPTION OF WEATHER MODIFICATION for a location if needed.
The type of weather modification agent can include: silver iodide, carbon dioxide, urea, sodium chloride, other, and combinations of each.
The type of apparatus can include: ground, airborne, or a combination of both.

EXTRACTION SOURCES:
Source Document: {extraction_results['file_path']}

Extraction Method 1 (pymupdf):
{extraction_results['pymupdf_text'] or '[No text extracted]'}

Extraction Method 2 (pytesseract OCR):
{extraction_results['pytesseract_text'] or '[No text extracted]'}

EXTRACT the following key information from the report using this format:
- START DATE FOR WEATHER MODIFICATION ACTIVITY: [Insert date] 
- END DATE FOR WEATHER MODIFICATION ACTIVITY: [Insert date] 
- TARGET AREA LOCATION: [Insert target location]  
- TYPE OF AGENT: [Insert type of agent]
- TYPE OF APPARATUS: [Insert type of weather modification apparatus] 
- PURPOSE OF PROJECT OR ACTIVITY: [Insert purpose] 
- DESCRIPTION OF WEATHER MODIFICATION: [Insert detailed description]  
- CONTROL AREA LOCATION: [Insert control location if specified, or say 'not specified']  
- TOTAL AMOUNT OF AGENT USED: [Insert amount in grams]  
- TOTAL NUMBER OF MODIFICATION DAYS: [Insert days]

ANALYSIS INSTRUCTIONS:
1. Carefully review both text extractions
2. Cross-reference information between extraction methods
3. Resolve any discrepancies by using contextual clues
4. Ensure maximum information preservation"""
    return prompt

def process_pdf_for_llm(file_path, llm_processor=None):
    """
    Process PDF for LLM semantic extraction.
    
    Args:
        file_path (str): Path to PDF file
        llm_processor (callable, optional): LLM processing function
    
    Returns:
        dict: Processed text and metadata
    """
    # Extract text using multiple methods
    extraction_results = extract_pdf_text(file_path)
    
    # Prepare LLM prompt
    llm_prompt = prepare_llm_prompt(extraction_results)
    
    # Process with LLM if processor provided
    if llm_processor:
        try:
            extraction_results['llm_processed_text'] = llm_processor(llm_prompt)
        except Exception as e:
            logging.error(f"LLM processing failed: {e}")
    
    return extraction_results

def main():
    """Main execution for PDF text extraction and LLM processing."""
    directory = "../all-files-starting-thru-2024/"
    
    # Example LLM processor (replace with actual implementation)
    def example_llm_processor(prompt):
        # Placeholder for LLM call
        print("\n--- FULL LLM PROMPT ---")
        print(prompt)
        return "Processed text summary would go here"
    
    # Select files
    selected_files = [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))][:1]
    
    for file in selected_files:
        full_path = os.path.join(directory, file)
        result = process_pdf_for_llm(full_path, example_llm_processor)
        
        # Print extraction details
        print(f"\n--- EXTRACTION DETAILS ---")
        print(f"File: {file}")
        print(f"pymupdf extraction length: {len(result['pymupdf_text'])}")
        print(f"pytesseract extraction length: {len(result['pytesseract_text'])}")
        
        # Print raw extractions
        print("\n--- PYMUPDF EXTRACTION ---")
        print(result['pymupdf_text'])
        print("\n--- PYTESSERACT EXTRACTION ---")
        print(result['pytesseract_text'])

if __name__ == "__main__":
    main()