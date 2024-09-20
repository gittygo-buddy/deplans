import os
import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import run_diagnostic, evaluate_quality

# File paths
file_layout = r"C:\Users\saman\OneDrive\Desktop\project-x\layout.csv"
file_path = r"C:\Users\saman\OneDrive\Desktop\project-x\sample.txt"
# Get the base name of the input file without the extension
base_name = os.path.splitext(os.path.basename(file_path))[0]
# Create the output file path with the suffix "_syn"
output_file_path = os.path.join(os.path.dirname(file_path), f"{base_name}_syn.txt")
metadata_base_path = r"C:\Users\saman\OneDrive\Desktop\project-x\my_metadata"

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
def load_layout(file_layout):
    print("Loading the layout...")
    layout = pd.read_csv(file_layout)
    layout.columns = [col.strip() for col in layout.columns]
    print("Layout loaded successfully.")
    return layout

# Step 2: Load the sample data file and remove the first and last rows
def load_sample_data(file_path):
    print("Loading sample data...")
    with open(file_path, 'r') as infile:
        sample_data = infile.readlines()
    first_row = sample_data[0]
    last_row = sample_data[-1]
    # Return first and last rows separately
    sample_data = sample_data[1:-1]
    print("Sample data loaded. First and last rows removed.")
    return sample_data, first_row, last_row

# Step 3: Process sample data using the layout
def process_sample_data(sample_data, layout):
    print("Processing sample data...")
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
    return df

# Step 4: Load or create metadata, then fit the synthesizer
def generate_synthetic_data(df, metadata_base_path, use_same_metadata_version=True):
    print("Generating synthetic data using SDV...")
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

    # Preserve original empty values in synthetic data on a row-by-row basis
    for column in df.columns:
        # Set  values to NA where the original values are NA
        df[column] = df[column].where(df[column].notna(), pd.NA)

    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(df)
    synthetic_data = synthesizer.sample(num_rows=len(df))

    print("Synthetic data generated.")
    return synthetic_data, metadata



# Step 5: Run diagnostics and quality checks on the synthetic data
def evaluate_synthetic_data(df, synthetic_data, metadata):
    print("Evaluating synthetic data quality...")
    diagnostic = run_diagnostic(
        real_data=df,
        synthetic_data=synthetic_data,
        metadata=metadata
    )
    quality_report = evaluate_quality(df, synthetic_data, metadata)
    print("Evaluation complete. Diagnostics and quality checks done.")
    return diagnostic, quality_report

# Step 6: Save metadata if there are changes
def save_if_metadata_changed(df, metadata, metadata_base_path):
    new_metadata = SingleTableMetadata()
    new_metadata.detect_from_dataframe(df)
    if new_metadata != metadata:
        print("Metadata has changed. Saving new version...")
        save_new_metadata_version(metadata_base_path, new_metadata)
    else:
        print("No changes in metadata. No new version created.")

def write_output_file(output_file_path, synthetic_data, layout, first_row, last_row):
    print("Writing output to file...")
    with open(output_file_path, 'w') as outfile:
        # Write original first row
        outfile.write(first_row)
        # Write synthetic data
        for _, row in synthetic_data.iterrows():
            transformed_data = ""
            current_position = 0
            for _, layout_row in layout.iterrows():
                column_name = layout_row['Column_Name']
                length = layout_row['Length']
                # Preserve original empty values in the output
                if pd.isna(row[column_name]) or row[column_name] == '':
                    column_data = ''.ljust(length)  # Maintain empty space
                else:
                    column_data = str(row[column_name]).ljust(length)  # Format with padding
                transformed_data += column_data
            outfile.write(transformed_data + '\n')
        # Write original last row
        outfile.write(last_row)
    print(f"Data written to {output_file_path}.")

# Main function to run all steps
def main(use_same_metadata_version=True):
    layout = load_layout(file_layout)
    sample_data, first_row, last_row = load_sample_data(file_path)
    df = process_sample_data(sample_data, layout)
    
    synthetic_data, metadata = generate_synthetic_data(df, metadata_base_path, use_same_metadata_version)
    diagnostic, quality_report = evaluate_synthetic_data(df, synthetic_data, metadata)
    
    if not use_same_metadata_version:
        save_if_metadata_changed(df, metadata, metadata_base_path)
    
    write_output_file(output_file_path, synthetic_data, layout, first_row, last_row)
    print("\nProcessing complete.")

# Run the main function with the flag to control metadata versioning
if __name__ == "__main__":
    main(use_same_metadata_version=True)  # Set to False to allow metadata updates
