import os
import re
import shutil

source_dir = '../noaa-files'
target_dir = os.path.join(source_dir, 'interim-final')
os.makedirs(target_dir, exist_ok=True)

matched_files = []

# Define match criteria
keywords = ['final', 'interim']
suffix_pattern = re.compile(r'-F\.pdf$', re.IGNORECASE)
form_174a_pattern = re.compile(r'17-4A', re.IGNORECASE)

# Check each file
for filename in os.listdir(source_dir):
    file_path = os.path.join(source_dir, filename)
    if os.path.isfile(file_path) and filename.lower().endswith('.pdf'):
        lower_name = filename.lower()
        if (
            any(keyword in lower_name for keyword in keywords) or
            suffix_pattern.search(filename) or
            form_174a_pattern.search(filename)
        ):
            matched_files.append(filename)
            shutil.move(file_path, os.path.join(target_dir, filename))

# Report and save
print(f"Moved {len(matched_files)} files to {target_dir}")

with open('matched_files.txt', 'w') as f:
    for file in matched_files:
        f.write(f"{file}\n")
