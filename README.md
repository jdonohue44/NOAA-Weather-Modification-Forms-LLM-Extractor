# LLM Extractor for NOAA Weather Modification Forms

## What does this project do?
Extracts key information from 1,000s of NOAA Form 17-4 (Initial Report On Weather Modification Activities) PDF files using LLM integration. Saves extracted structured information into CSV file.

## How do I use it?
View the CSV file containing the key weather modification information extracted in `./results/` folder.

## How do I run it myself?
1. Install required dependencies `pip install python-dotenv pymupdf4llm pytesseract pdf2image openai os random csv time datetime`
2. Run `python ./code/parse-noaa-files-and-save-to-csv.py`
3. View the output in the `./results/` folder.

