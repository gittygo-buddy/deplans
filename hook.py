import os
import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import run_diagnostic, evaluate_quality

# File paths
file_layout = r"C:\Users\saman\OneDrive\Desktop\project-x\layout.csv"
file_path = r"C:\Users\saman\OneDrive\Desktop\project-x\sample.txt"
header_layout_file = r"C:\Users\saman\OneDrive\Desktop\project-x\header-layout.csv"
metadata_base_path = r"C:\Users\saman\OneDrive\Desktop\project-x\metadata"

# Get the base name of the input file without the extension
base_name = os.path.splitext(os.path.basename(file_path))[0]
# Create the output file path with the suffix "_syn"
output_file_path = os.path.join(os.path.dirname(file_path), f"{base_name}_syn.txt")

# Function to detect the latest metadata version
def get_latest_metadata_version(base_path, metadata_type):
    version = 1
    while os.path.exists(f"{base_path}_{metadata_type}_v{version}.json"):
        version += 1
    return version - 1 if version > 1 else None

# Function to save new metadata with incremented version and metadata type (header or data)
def save_new_metadata_version(base_path, metadata, metadata_type):
    latest_version = get_latest_metadata_version(base_path, metadata_type)
    new_version = (latest_version + 1) if latest_version else 1
    new_metadata_path = f"{base_path}_{metadata_type}_v{new_version}.json"
    metadata.save_to_json(filepath=new_metadata_path)
    print(f"New {metadata_type} metadata saved as {new_metadata_path}")

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
    sample_data = sample_data[1:-1]  # Remove first and last rows
    print("Sample data loaded. First and last rows removed.")
    return sample_data, first_row

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

# Step 4: Generate synthetic data for both header and data, with distinct metadata
def generate_synthetic_data(df, metadata_base_path, metadata_type, use_same_metadata_version=True):
    print(f"Generating synthetic data for {metadata_type} using SDV...")
    metadata = SingleTableMetadata()
    latest_version = get_latest_metadata_version(metadata_base_path, metadata_type)

    metadata_saved = False

    if latest_version and use_same_metadata_version:
        latest_metadata_path = f"{metadata_base_path}_{metadata_type}_v{latest_version}.json"
        print(f"Using existing {metadata_type} metadata: {latest_metadata_path}")
        metadata = SingleTableMetadata.load_from_json(filepath=latest_metadata_path)
    else:
        print(f"No existing {metadata_type} metadata found or creating new metadata.")
        metadata.detect_from_dataframe(df)
        save_new_metadata_version(metadata_base_path, metadata, metadata_type)
        metadata_saved = True

    # Preserve original empty values in synthetic data on a row-by-row basis
    for column in df.columns:
        df[column] = df[column].where(df[column].notna(), pd.NA)

    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(df)
    synthetic_data = synthesizer.sample(num_rows=len(df))

    print(f"Synthetic {metadata_type} data generated.")
    return synthetic_data, metadata, metadata_saved

# Step 5: Run diagnostics and quality checks on the synthetic data
def evaluate_synthetic_data(df, synthetic_data, metadata):
    print("Evaluating synthetic data quality...")
    diagnostic = run_diagnostic(real_data=df, synthetic_data=synthetic_data, metadata=metadata)
    quality_report = evaluate_quality(df, synthetic_data, metadata)
    print("Evaluation complete. Diagnostics and quality checks done.")
    return diagnostic, quality_report

# Step 6: Save metadata if there are changes
def build_page_trailer(df):
    record_count = len(df)
       
    # Check if the columns exist before summing
    df['net_amount_due'] = pd.to_numeric(df['net_amount_due'], errors='coerce')
    net_amount_due_sum = df['net_amount_due'].sum() if 'net_amount_due' in df.columns else 0
    gross_amount_due_sum = df['gross_amount_due'].sum() if 'gross_amount_due' in df.columns else 0
    pat_paid_amount_sum = df['patient_pay_amount'].sum() if 'patient_pay_amount' in df.columns else 0

    # Construct the trailer with correct padding and fixed 'A' and 'D' at positions 24 and 48
    trailer_data = (
        "PT" +
        str(record_count).zfill(10) +
        str(int(net_amount_due_sum)).zfill(11) +
        "A" +
        str(int(gross_amount_due_sum)).zfill(11) +
        "G" +
        str(int(pat_paid_amount_sum)).zfill(11) + "D"
    )

    # Ensure the length is exactly 48 characters
    trailer_data = trailer_data.ljust(48)[:48]

    return trailer_data

# Write output to file
def write_output_file(output_file_path, synthetic_data, layout, synthetic_header, header_layout):
    print("Writing output to file...")
    with open(output_file_path, 'w') as outfile:
        # Write synthetic header
        for _, row in synthetic_header.iterrows():
            transformed_data = ""
            for _, layout_row in header_layout.iterrows():
                column_name = layout_row['Column_Name']
                length = layout_row['Length']
                column_data = str(row[column_name]).ljust(length)
                transformed_data += column_data
            outfile.write(transformed_data + '\n')
        
        # Write synthetic data
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
        
        # Build and write page trailer
        trailer_record = build_page_trailer(synthetic_data)
        outfile.write(trailer_record + '\n')
          
    print(f"Data written to {output_file_path}.")

# Main function to run all steps, including header
def main(use_same_metadata_version=True):
    layout = load_layout(file_layout)
    header_layout = load_layout(header_layout_file)
    
    # Step 1: Load and process header
    sample_data, first_row = load_sample_data(file_path)
    header_df = process_sample_data([first_row], header_layout)

    print("Header DataFrame:")
    print(header_df)
    
    # Step 2: Generate synthetic data for the header
    synthetic_header, header_metadata, header_metadata_saved = generate_synthetic_data(
        header_df, metadata_base_path, metadata_type="header", use_same_metadata_version=use_same_metadata_version
    )

    # Step 3: Process sample data and generate synthetic data for data
    df = process_sample_data(sample_data, layout)
    synthetic_data, data_metadata, data_metadata_saved = generate_synthetic_data(
        df, metadata_base_path, metadata_type="data", use_same_metadata_version=use_same_metadata_version
    )

    # Evaluate synthetic data for both header and data
    evaluate_synthetic_data(header_df, synthetic_header, header_metadata)
    evaluate_synthetic_data(df, synthetic_data, data_metadata)
    
    # Save metadata if changes were made
    if not use_same_metadata_version:
        if not header_metadata_saved:
            save_if_metadata_changed(header_df, header_metadata, metadata_base_path, "header")
        if not data_metadata_saved:
            save_if_metadata_changed(df, data_metadata, metadata_base_path, "data")

    # Write the synthetic output, combining header and data
    write_output_file(output_file_path, synthetic_data, layout, synthetic_header, header_layout)
    print("\nProcessing complete.")

# Run the main function with the flag to control metadata versioning
if __name__ == "__main__":
    main(use_same_metadata_version=True)  # Set to False to allow metadata updates
