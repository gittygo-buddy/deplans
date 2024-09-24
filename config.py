import os

# File paths
file_layout = r"C:\Users\saman\OneDrive\Desktop\project-x\layout.csv"
file_path = r"C:\Users\saman\OneDrive\Desktop\project-x\sample.txt"
header_layout_file = r"C:\Users\saman\OneDrive\Desktop\project-x\header-layout.csv"
metadata_base_path = r"C:\Users\saman\OneDrive\Desktop\project-x\metadata"

# Create output file path
base_name = os.path.splitext(os.path.basename(file_path))[0]
output_file_path = os.path.join(os.path.dirname(file_path), f"{base_name}_syn.txt")
