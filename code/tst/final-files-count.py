import os
import re

directory = '../noaa-files'
matched_files = []

# Keywords/patterns to match
keywords = ['final', 'interim']
suffix_pattern = re.compile(r'-F\.pdf$', re.IGNORECASE)

# Scan files
for filename in os.listdir(directory):
    if filename.lower().endswith('.pdf'):
        lower_name = filename.lower()
        if any(keyword in lower_name for keyword in keywords) or suffix_pattern.search(filename):
            matched_files.append(filename)

# Print count
print(f"Found {len(matched_files)} matching files.")

# Save to file
with open('matched_files.txt', 'w') as f:
    for file in matched_files:
        f.write(f"{file}\n")

