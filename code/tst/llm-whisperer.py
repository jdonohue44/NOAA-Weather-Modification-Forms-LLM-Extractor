import os
from dotenv import load_dotenv
from unstract.llmwhisperer import LLMWhispererClientV2
from unstract.llmwhisperer.client_v2 import LLMWhispererClientException

load_dotenv()
client = LLMWhispererClientV2()

input_dir = '../accuracy-evals/golden-10'

for filename in os.listdir(input_dir):
    if filename.lower().endswith('.pdf'):
        file_path = os.path.join(input_dir, filename)
        print(f"\nProcessing {filename}...")
        try:
            result = client.whisper(
                file_path=file_path,
                wait_for_completion=True,
                wait_timeout=200,
            )
            output_text = result['extraction'].get('result_text', '[No result_text found]')
        except LLMWhispererClientException as e:
            output_text = f'[Error]: {str(e)}'
        
        # Save to .txt file
        output_filename = filename.replace('.pdf', '.llmwhisper.txt')
        output_path = os.path.join(input_dir, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(output_text)
        print(f"Saved result to {output_filename}")
