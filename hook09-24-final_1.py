import os
import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import run_diagnostic, evaluate_quality

#############################################################################
# File paths
#############################################################################
file_layout = r"C:\Users\saman\OneDrive\Desktop\project-x\layout.csv"
file_path = r"C:\Users\saman\OneDrive\Desktop\project-x\sample.txt"
header_layout_file = r"C:\Users\saman\OneDrive\Desktop\project-x\header-layout.csv"
metadata_base_path = r"C:\Users\saman\OneDrive\Desktop\project-x\metadata"

# Create output file path
base_name = os.path.splitext(os.path.basename(file_path))[0]
output_file_path = os.path.join(os.path.dirname(file_path), f"{base_name}_syn.txt")

#############################################################################
# Metadata versioning functions
#############################################################################
def get_latest_metadata_version(base_path, metadata_type):
    version = 1
    while os.path.exists(f"{base_path}_{metadata_type}_v{version}.json"):
        version += 1
    return version - 1 if version > 1 else None

def save_new_metadata_version(base_path, metadata, metadata_type):
    latest_version = get_latest_metadata_version(base_path, metadata_type)
    new_version = (latest_version + 1) if latest_version else 1
    new_metadata_path = f"{base_path}_{metadata_type}_v{new_version}.json"
    metadata.save_to_json(filepath=new_metadata_path)
    print(f"New {metadata_type} metadata saved as {new_metadata_path}")

#############################################################################
# Load layout and sample data
#############################################################################
def load_layout(file_layout):
    print("Loading the layout...")
    layout = pd.read_csv(file_layout).rename(columns=lambda x: x.strip())
    print("Layout loaded successfully.")
    return layout

def load_sample_data(file_path):
    print("Loading sample data...")
    with open(file_path, 'r') as infile:
        sample_data = infile.readlines()
    print("Sample data loaded.")
    return sample_data

#############################################################################
# Header processing functions
#############################################################################
def process_header_data(header_row, layout):
    print("Processing header data...")
    current_position = 0
    row_data = {}
    for _, row in layout.iterrows():
        column_name = row['Column_Name']
        length = row['Length']
        row_data[column_name] = header_row[current_position:current_position + length].strip()
        current_position += length

    df = pd.DataFrame([row_data])  # Create DataFrame from header data
    print("Header data processed into DataFrame.")
    return df

def generate_synthetic_header(header_df, metadata_base_path, use_same_metadata_version=True):
    print("Generating synthetic header data using SDV...")
    metadata = SingleTableMetadata()
    latest_version = get_latest_metadata_version(metadata_base_path, "header")

    if latest_version and use_same_metadata_version:
        latest_metadata_path = f"{metadata_base_path}_header_v{latest_version}.json"
        print(f"Using existing header metadata: {latest_metadata_path}")
        metadata = SingleTableMetadata.load_from_json(filepath=latest_metadata_path)
    else:
        print("No existing header metadata found or creating new metadata.")
        metadata.detect_from_dataframe(header_df)
        save_new_metadata_version(metadata_base_path, metadata, "header")

    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(header_df)
    synthetic_header = synthesizer.sample(num_rows=len(header_df))
    print("Synthetic header data generated.")
    return synthetic_header, metadata

#############################################################################
# Sample data processing functions
#############################################################################
def process_sample_data(sample_data, layout, date_columns=[]):
    print("Processing sample data...")
    data_rows = []
    for line in sample_data:
        current_position = 0
        row_data = {}
        for _, row in layout.iterrows():
            column_name = row['Column_Name']
            length = row['Length']
            row_data[column_name] = line[current_position:current_position + length].strip()
            current_position += length
        data_rows.append(row_data)

    df = pd.DataFrame(data_rows)

    # Handle date columns
    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors='coerce')  # Convert to datetime, invalids to NaT
            df[column] = df[column].fillna(pd.NaT)  # Ensure blanks are treated as NaT

    print("Data processed into DataFrame.")
    return df

def generate_synthetic_data(df, metadata_base_path, use_same_metadata_version=True):
    print("Generating synthetic sample data using SDV...")
    metadata = SingleTableMetadata()
    latest_version = get_latest_metadata_version(metadata_base_path, "data")

    if latest_version and use_same_metadata_version:
        latest_metadata_path = f"{metadata_base_path}_data_v{latest_version}.json"
        print(f"Using existing data metadata: {latest_metadata_path}")
        metadata = SingleTableMetadata.load_from_json(filepath=latest_metadata_path)
    else:
        print("No existing data metadata found or creating new metadata.")
        metadata.detect_from_dataframe(df)
        save_new_metadata_version(metadata_base_path, metadata, "data")

    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(df)
    synthetic_data = synthesizer.sample(num_rows=len(df))
    print("Synthetic sample data generated.")
    return synthetic_data, metadata

#############################################################################
# Page trailer processing functions
#############################################################################
def build_page_trailer(df):
    print("Building page trailer...")
    record_count = len(df)

    # Ensure numeric columns are correctly processed
    df['net_amount_due'] = 0#pd.to_numeric(df['net_amount_due'], errors='coerce')
    net_amount_due_sum = 0#df['net_amount_due'].sum() if 'net_amount_due' in df.columns else 0
    gross_amount_due_sum = 0#df['gross_amount_due'].sum() if 'gross_amount_due' in df.columns else 0
    pat_paid_amount_sum = 0#df['patient_pay_amount'].sum() if 'patient_pay_amount' in df.columns else 0

    trailer_data = (
        "PT" +
        str(record_count).zfill(10) +
        str(int(net_amount_due_sum)).zfill(11) +
        "A" +
        str(int(gross_amount_due_sum)).zfill(11) +
        "G" +
        str(int(pat_paid_amount_sum)).zfill(11) + "D"
    )

    print("Page trailer built.")
    return trailer_data.ljust(48)[:48]  # Ensure length is exactly 48 characters

#############################################################################
# File writing function
#############################################################################
def write_output_file(output_file_path, synthetic_data, layout, synthetic_header, header_layout, date_columns=[]):
    print("Writing output to file...")
    with open(output_file_path, 'w') as outfile:
        # Write synthetic header
        for _, row in synthetic_header.iterrows():
            transformed_data = "".join(str(row[layout_row['Column_Name']]).ljust(layout_row['Length'])
                                       for _, layout_row in header_layout.iterrows())
            outfile.write(transformed_data + '\n')

        # Write synthetic sample data
        for _, row in synthetic_data.iterrows():
            transformed_data = ""
            for column_name, length in zip(layout['Column_Name'], layout['Length']):
                value = row[column_name]
                if pd.notna(value):
                    # Format date columns
                    if column_name in date_columns:
                        value = value.strftime('%Y%m%d')  # Format date to YYYYMMDD
                    transformed_data += str(value).ljust(length)
                else:
                    transformed_data += ''.ljust(length)
            outfile.write(transformed_data + '\n')

        # Write page trailer
        trailer_record = build_page_trailer(synthetic_data)
        outfile.write(trailer_record + '\n')

    print(f"Data written to {output_file_path}.")

#############################################################################
# Main function
#############################################################################
def main(use_same_metadata_version=True):
    layout = load_layout(file_layout)
    header_layout = load_layout(header_layout_file)
    
    sample_data = load_sample_data(file_path)

    # Process header (first row)
    header_row = sample_data[0]  # First row is header
    header_df = process_header_data(header_row, header_layout)

    # Generate synthetic header
    synthetic_header, header_metadata = generate_synthetic_header(
        header_df, metadata_base_path, use_same_metadata_version=use_same_metadata_version
    )

    # Process data (everything after the header)
    data_rows = sample_data[1:]  # Remaining rows are data
    date_columns = ['da']  # Replace with actual date column names
    df = process_sample_data(data_rows, layout, date_columns)  # Pass date_columns here

    # Generate synthetic data
    synthetic_data, data_metadata = generate_synthetic_data(
        df, metadata_base_path, use_same_metadata_version=use_same_metadata_version
    )

    # Write output
    write_output_file(output_file_path, synthetic_data, layout, synthetic_header, header_layout, date_columns)

    print("\nProcessing complete.")

#############################################################################
# Execution entry point
#############################################################################
if __name__ == "__main__":
    main(use_same_metadata_version=True)  # Set to False if you want to generate new metadata each time
