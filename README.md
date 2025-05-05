# LLM Extractor for Historical NOAA Weather Modification Forms

## What does this project do?
Extracts key information from 1,025 historical NOAA Form 17-4 (Initial Report On Weather Modification Activities) PDF files using LLM integration. Saves extracted structured information into CSV file.

## Usage
1. Navigate to `code/`
2. Install required Python dependencies `pip install requirements.txt`
3. Obtain your own OpenAI credentials and save your API key in `.env`
4. Run `python llm-extractor.py` to generate the dataset. This will take about 2.5 hours.
5. View the generated dataset in `dataset/final/` 
