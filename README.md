# LLM Extractor for Historical NOAA Weather Modification Forms

## What does this project do?
Extracts key information from 1,025 historical NOAA Form 17-4 (Initial Report On Weather Modification Activities) PDF files using LLM integration. Saves extracted structured information into CSV file.

## Data Source
All NOAA forms are publicly available for download at https://library.noaa.gov/weather-climate/weather-modification-project-reports.

## Usage
1. Download all files you wish to process and save them in `noaa-files/`
2. Navigate to `code/`
3. Install required Python dependencies `pip install requirements.txt`
4. Obtain your own [OpenAI](https://platform.openai.com/docs/overview) and [LLM Whisperer](https://unstract.com/llmwhisperer/) credentials and save your API keys in `.env`
5. Use `python ./file-helpers/move-interim-final-files.py` and manual review to ensure the final Form 17-4 is the first page of the PDF.
6. Run `python llm-extractor.py` to generate the dataset. This will take about 2.5 hours to process all NOAA files (~10-15 seconds per file).
7. Run `python clean-dataset.py` to clean and standardize the dataset.
8. View the generated dataset in `dataset/final/` 
