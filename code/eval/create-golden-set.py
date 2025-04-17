import os
import random
import shutil

# Parameters
source_dir = '../noaa-files'
target_dir = '../accuracy-evals/golden-10'
N = 10  # Change this number to sample a different number of files

# Ensure target directory exists
os.makedirs(target_dir, exist_ok=True)

# List all files in the source directory
all_files = [f for f in os.listdir(source_dir) if os.path.isfile(os.path.join(source_dir, f))]

# Sample N files
sampled_files = random.sample(all_files, min(N, len(all_files)))

# Copy files to the target directory
for file_name in sampled_files:
    src_path = os.path.join(source_dir, file_name)
    dst_path = os.path.join(target_dir, file_name)
    shutil.copy2(src_path, dst_path)
    print(f"Copied: {file_name}")

print(f"\nTotal {len(sampled_files)} files copied to '{target_dir}'")

