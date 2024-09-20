import os
import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import run_diagnostic, evaluate_quality

# File paths
file_layout = r"C:\Users\saman\OneDrive\Desktop\project-x\layout.csv"
file_path = r"C:\Users\saman\OneDrive\Desktop\project-x\sample.txt"
base_name = os.path.splitext(os.path.basename(file_path))[0]
output_file_path = os.path.join(os.path.dirname(file_path), f"{base_name}_syn.txt")
metadata_base_path = r"C:\Users\saman\OneDrive\Desktop\project-x\my_metadata"

# Flag for metadata versioning
use_same_metadata_version = True  # Set to True to use the same metadata version, False to allow updates

# Function to detect the latest metadata version
def get_latest_metadata_version(base_path):
    version = 1
    while os.path.exists(f"{base_path}_v{version}.json"):
        version += 1
    return version - 1 if version > 1 else None

# Function to save new metadata with incremented version
def save_new_metadata_version(base_path, metadata):
    latest_version = get_latest_metadata_version(base_path)
    new_version = (latest_version + 1) if latest_version else 1
    new_metadata_path = f"{base_path}_v{new_version}.json"
    metadata.save_to_json(filepath=new_metadata_path)
    print(f"New metadata saved as {new_metadata_path}")

# Step 1: Load the layout CSV file
layout = pd.read_csv(file_layout)
layout.columns = [col.strip() for col in layout.columns]
print("Layout loaded successfully.")

# Step 2: Load the sample data file and remove the first and last rows
with open(file_path, 'r') as infile:
    sample_data = infile.readlines()
first_row = sample_data[0]
last_row = sample_data[-1]
sample_data = sample_data[1:-1]
print("Sample data loaded. First and last rows removed.")

# Step 3: Process sample data using the layout
data_rows = []
for line in sample_data:
    line = line.strip()
    current_position = 0
    row_data = {}
    for _, row in layout.iterrows():
        column_name = row['Column_Name']
        length = row['Length']
        column_data = line[current_position:current_position + length].strip()
        row_data[column_name] = column_data
        current_position += length
    data_rows.append(row_data)
df = pd.DataFrame(data_rows)
print("Data processed into DataFrame.")

# Step 4: Load or create metadata, then fit the synthesizer
metadata = SingleTableMetadata()
latest_version = get_latest_metadata_version(metadata_base_path)

if latest_version and use_same_metadata_version:
    latest_metadata_path = f"{metadata_base_path}_v{latest_version}.json"
    print(f"Using existing metadata: {latest_metadata_path}")
    metadata = SingleTableMetadata.load_from_json(filepath=latest_metadata_path)
else:
    print("No existing metadata found or creating new metadata.")
    metadata.detect_from_dataframe(df)
    save_new_metadata_version(metadata_base_path, metadata)

# Step 5: Generate synthetic data
synthesizer = GaussianCopulaSynthesizer(metadata)
synthesizer.fit(df)

# Preserve original empty values in synthetic data on a row-by-row basis
for column in df.columns:
    df[column] = df[column].where(df[column].notna(), pd.NA)

synthetic_data = synthesizer.sample(num_rows=len(df))
print("Synthetic data generated.")

# Step 6: Run diagnostics and quality checks on the synthetic data
diagnostic = run_diagnostic(real_data=df, synthetic_data=synthetic_data, metadata=metadata)
quality_report = evaluate_quality(df, synthetic_data, metadata)
print("Evaluation complete. Diagnostics and quality checks done.")

# Step 7: Save metadata if there are changes
new_metadata = SingleTableMetadata()
new_metadata.detect_from_dataframe(df)
if new_metadata != metadata:
    print("Metadata has changed. Saving new version...")
    save_new_metadata_version(metadata_base_path, new_metadata)
else:
    print("No changes in metadata. No new version created.")

# Step 8: Write the processed data to the output file, including the first and last rows
with open(output_file_path, 'w') as outfile:
    outfile.write(first_row)
    for _, row in synthetic_data.iterrows():
        transformed_data = ""
        for _, layout_row in layout.iterrows():
            column_name = layout_row['Column_Name']
            length = layout_row['Length']
            if pd.isna(row[column_name]) or row[column_name] == '':
                column_data = ''.ljust(length)  # Maintain empty space
            else:
                column_data = str(row[column_name]).ljust(length)  # Format with padding
            transformed_data += column_data
        outfile.write(transformed_data + '\n')
    outfile.write(last_row)
print(f"Data written to {output_file_path}.")

print("\nProcessing complete.")
