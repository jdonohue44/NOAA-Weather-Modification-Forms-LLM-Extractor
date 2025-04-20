import os
import random
import shutil

source_dir = '../../noaa-files'
target_dir = '../../accuracy-evals/golden-150'
existing_dir = '../../accuracy-evals/golden-50'
N = 150

# Ensure target directory exists
os.makedirs(target_dir, exist_ok=True)

# Get files already in the existing directory
existing_files = set(os.listdir(existing_dir))

# List all files in the source directory excluding those already in existing_dir
all_files = [f for f in os.listdir(source_dir)
             if os.path.isfile(os.path.join(source_dir, f)) and f not in existing_files]

# Sample N files
sampled_files = random.sample(all_files, min(N, len(all_files)))

# Copy files to the target directory
for file_name in sampled_files:
    src_path = os.path.join(source_dir, file_name)
    dst_path = os.path.join(target_dir, file_name)
    shutil.copy2(src_path, dst_path)
    print(f"Copied: {file_name}")

print(f"\nTotal {len(sampled_files)} files copied to '{target_dir}'")

