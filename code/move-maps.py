import os
import shutil
import sys

def move_files(source_dir, target_dir):
    # Ensure target directory exists
    if not os.path.exists(target_dir):
        try:
            os.makedirs(target_dir)
            print(f"Created directory: {target_dir}")
        except OSError as e:
            print(f"Error creating directory {target_dir}: {e}")
            sys.exit(1)
    
    # Check if source directory exists
    if not os.path.exists(source_dir):
        print(f"Error: Source directory {source_dir} does not exist!")
        sys.exit(1)
    
    # Counter for moved files
    moved_count = 0
    
    # Go through all files in source directory
    for filename in os.listdir(source_dir):
        if filename.endswith('-M.pdf'):
            source_path = os.path.join(source_dir, filename)
            target_path = os.path.join(target_dir, filename)
            
            try:
                shutil.move(source_path, target_path)
                print(f"Moved: {filename}")
                moved_count += 1
            except Exception as e:
                print(f"Error moving {filename}: {e}")
    
    print(f"\nTotal files moved: {moved_count}")

if __name__ == "__main__":
    # Define source and target directories
    source_directory = "../noaa-files"
    target_directory = "../maps"
    
    print(f"Moving PDF files ending with '-M' from {source_directory} to {target_directory}...")
    move_files(source_directory, target_directory)
