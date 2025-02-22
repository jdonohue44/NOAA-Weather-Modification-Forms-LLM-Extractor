import os
import re
import PyPDF2

def find_and_merge_pdfs(directory):
    """
    Find duplicate PDFs in the directory based on naming patterns and merge them.
    The order of merging is: "-1.pdf" first, then "-F.pdf" (if present), then "-M.pdf" (if present).
    Deletes original files after merging.
    """
    pdf_files = [f for f in os.listdir(directory) if f.endswith(".pdf")]
    
    # Dictionary to store base filenames without -1, -F, or -M
    pdf_groups = {}
    
    pattern = re.compile(r"(.*?)(?:-1|-F|-M)\.pdf$")

    num_merged_files = 0
    
    for pdf in pdf_files:
        match = pattern.match(pdf)
        if match:
            base_name = match.group(1)
            pdf_groups.setdefault(base_name, []).append(pdf)
    
    for base_name, files in pdf_groups.items():
        merge_order = []
        for suffix in ["-1.pdf", "-F.pdf", "-M.pdf"]:  # Correct order
            matching_files = [f for f in files if f.endswith(suffix)]
            if matching_files:
                merge_order.append(os.path.join(directory, matching_files[0]))

        if len(merge_order) >= 2:
            merged_file = os.path.join(directory, base_name + "-merged.pdf")
            merge_pdfs(merge_order, merged_file)
            num_merged_files += 1
            print(f"Merged {', '.join(merge_order)} into {merged_file}")

            # Delete original files after merging
            for file in merge_order:
                os.remove(file)
    
    print(f"Number of merged files: {num_merged_files}")

def merge_pdfs(pdf_list, output_path):
    """
    Merge multiple PDFs into a single file.
    """
    merger = PyPDF2.PdfMerger()
    
    for pdf in pdf_list:
        merger.append(pdf)
    
    merger.write(output_path)
    merger.close()

if __name__ == "__main__":
    import sys    
    directory = '../COPY-all-files-starting-thru-2024/'
    if not os.path.isdir(directory):
        print(f"Error: {directory} is not a valid directory")
        sys.exit(1)
    
    find_and_merge_pdfs(directory)
