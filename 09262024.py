import os
import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import run_diagnostic, evaluate_quality
import util as ut
import random

# File paths
file_layout = r"C:\Users\saman\OneDrive\Desktop\project-x\layout.csv"
file_path = r"C:\Users\saman\OneDrive\Desktop\project-x\sample.txt"
header_layout_file = r"C:\Users\saman\OneDrive\Desktop\project-x\header-layout.csv"
metadata_base_path = r"C:\Users\saman\OneDrive\Desktop\project-x\metadata"

# Create output file path based on the input file name, appending '_syn' for synthetic
base_name = os.path.splitext(os.path.basename(file_path))[0]
output_file_path = os.path.join(os.path.dirname(file_path), f"{base_name}_syn.txt")



############################ Metadata Functions ##############################
# Get the latest version of metadata for a specific type
def get_latest_metadata_version(base_path, metadata_type):
    version = 1
    while os.path.exists(f"{base_path}_{metadata_type}_v{version}.json"):
        version += 1
    return version - 1 if version > 1 else None

# Save a new version of metadata if the previous version exists
def save_new_metadata_version(base_path, metadata, metadata_type):
    latest_version = get_latest_metadata_version(base_path, metadata_type)
    new_version = (latest_version + 1) if latest_version else 1
    new_metadata_path = f"{base_path}_{metadata_type}_v{new_version}.json"
    metadata.save_to_json(filepath=new_metadata_path)
    print(f"New {metadata_type} metadata saved as {new_metadata_path}")

############################ Load Layout & Data ###############################
# Load layout from CSV
def load_layout(file_layout):
    print("Loading the layout...")
    layout = pd.read_csv(file_layout).rename(columns=lambda x: x.strip())
    print("Layout loaded successfully.")
    return layout

# Load sample data from the given file path
def load_sample_data(file_path):
    print("Loading sample data...")
    with open(file_path, 'r') as infile:
        sample_data = infile.readlines()
    print("Sample data loaded.")
    return sample_data

####################### Process CD Records ####################################
# Process CE records by keeping them unchanged as a single column
def process_cd_records(sample_data):
    print("Processing CE records...")
    ce_rows = []
    
    for line in sample_data:
        if line.startswith("CD100"):  # Assuming CE records start with "CE"
            row_data = {"CD_Record": line.strip()}  # Store the entire line as a single column
            ce_rows.append(row_data)

    df_ce = pd.DataFrame(ce_rows)
    print("CE records processed into DataFrame.")
    return df_ce

###################### Process Main Sample Data ##############################
# Process the main sample data and convert it to a DataFrame based on the layout
def process_sample_data(sample_data, layout, date_columns=[]):
    print("Processing sample data...")
    data_rows = []
    for line in sample_data:
        if not line.startswith("CD"):
            current_position = 0
            row_data = {}
            for _, row in layout.iterrows():
                column_name = row['Column_Name']
                length = row['Length']
                row_data[column_name] = line[current_position:current_position + length].strip()
                current_position += length
            data_rows.append(row_data)
        df = pd.DataFrame(data_rows)

    # Handle date columns, converting to datetime format
    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors='coerce')  # Convert to datetime, invalids to NaT
            df[column] = df[column].fillna(pd.NaT)  # Ensure blanks are treated as NaT

    print("Data processed into DataFrame.")
    return df

##################sentivice columns block apply custom functions##############
def generate_random_number(length):
    # Check if the length is None or blank
    if length is None or length == '':
        return None  # Return None or you can use '' for blank

    # Validate that length is a positive integer
    if length <= 0:
        raise ValueError("Length must be a positive integer.")
    
    min_value = 10 ** (length - 1)  # Minimum value for the specified length
    max_value = 10 ** length - 1  # Maximum value for the specified length
    return random.randint(min_value, max_value)

################### Generate Synthetic Data ##################################
# Generate synthetic data using SDV and metadata
def generate_synthetic_data(df, metadata_base_path, metadata_type, use_same_metadata_version=True):
    print(f"Generating synthetic data for {metadata_type} using SDV...")
    metadata = SingleTableMetadata()
    latest_version = get_latest_metadata_version(metadata_base_path, metadata_type)

    if latest_version and use_same_metadata_version:
        latest_metadata_path = f"{metadata_base_path}_{metadata_type}_v{latest_version}.json"
        print(f"Using existing {metadata_type} metadata: {latest_metadata_path}")
        metadata = SingleTableMetadata.load_from_json(filepath=latest_metadata_path)
    else:
        print(f"No existing {metadata_type} metadata found or creating new metadata.")
        metadata.detect_from_dataframe(df)
        save_new_metadata_version(metadata_base_path, metadata, metadata_type)

    # Preserve original empty values
    for column in df.columns:
        df[column] = df[column].where(df[column].notna(), pd.NA)

    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(df)
    synthetic_data = synthesizer.sample(num_rows=len(df))
    print(f"Synthetic {metadata_type} data generated.")
    return synthetic_data, metadata

################### Evaluate Synthetic Data ##################################
# Evaluate the quality of the synthetic data
def evaluate_synthetic_data(df, synthetic_data, metadata):
    print("Evaluating synthetic data quality...")
    diagnostic = run_diagnostic(real_data=df, synthetic_data=synthetic_data, metadata=metadata)
    quality_report = evaluate_quality(df, synthetic_data, metadata)
    print("Evaluation complete.")
    return diagnostic, quality_report

########################### Build Trailer ###################################
# Build page trailer based on calculated sums and record count
def build_page_trailer(df):
    record_count = len(df)

    # Apply get_return_value() to each relevant column before summing
    df['net_amount_due'] = df['net_amount_due'].apply(ut.get_return_value)
    df['gross_amount_due'] = 0#df['gross_amount_due'].apply(ut.get_return_value)
    df['patient_pay_amount'] = 0#df['patient_pay_amount'].apply(ut.get_return_value)

    # Ensure numeric columns are correctly processed
    df['net_amount_due'] = pd.to_numeric(df['net_amount_due'], errors='coerce')
    net_amount_due_sum = df['net_amount_due'].sum() if 'net_amount_due' in df.columns else 0
    gross_amount_due_sum = 0#df['gross_amount_due'].sum() if 'gross_amounst_due' in df.columns else 0
    pat_paid_amount_sum = 0#df['patient_pay_amount'].sum() if 'patient_pay_amount' in df.columns else 0

    # Build the trailer data with sums and formatting
    trailer_data = (
        "PT" +
        str(record_count).zfill(10) +                         # Record count
        str(int(net_amount_due_sum)).zfill(11) +              # Net amount due sum
        "A" +
        str(int(gross_amount_due_sum)).zfill(11) +            # Gross amount due sum
        "G" +
        str(int(pat_paid_amount_sum)).zfill(11) + "D"         # Patient paid amount sum
    )

    # Ensure trailer data fits into the 48-character limit
    return trailer_data.ljust(48)[:48]

########################### Write File ######################################
# Write the synthetic header, synthetic data, and CE records to the output file
def write_output_file(output_file_path, synthetic_data, df_ce, layout, synthetic_header, header_layout, date_columns=[]):
    print("Writing output to file...")
    with open(output_file_path, 'w') as outfile:
        # Write synthetic header
        for _, row in synthetic_header.iterrows():
            transformed_data = "".join(str(row[layout_row['Column_Name']]).ljust(layout_row['Length'])
                                       for _, layout_row in header_layout.iterrows())
            outfile.write(transformed_data + '\n')

        # Write synthetic data
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

        # Write synthetic data for CE records
        for _, row in df_ce.iterrows():
            transformed_data = row['CD_Record'].strip()  #entire CD record set is stored in this column
            outfile.write(transformed_data + '\n')

        # Build and write page trailer
        trailer_record = build_page_trailer(synthetic_data)
        outfile.write(trailer_record + '\n')

    print(f"Data written to {output_file_path}.")

############################ Main Function ##################################
# Main function to load data, generate synthetic data, and write the output file
def main(use_same_metadata_version=True):
    layout = load_layout(file_layout)
    header_layout = load_layout(header_layout_file)
    sample_data = load_sample_data(file_path)
    header_df = process_sample_data([sample_data[0]], header_layout)
    
    print("Header DataFrame:")
    print(header_df)

    synthetic_header, header_metadata = generate_synthetic_data(
        header_df, metadata_base_path, metadata_type="header", use_same_metadata_version=use_same_metadata_version
    )
    
    df_ce = process_cd_records(sample_data)
    df_sample = process_sample_data(sample_data[1:-1], layout)
    

    row_size = df_sample.shape[0]
    # Apply function to generate random numbers based on the length of each entry in 'Test2'
    df_sample['Test2'] = df_sample['Test2'].apply(lambda x: generate_random_number(len(x)))
   

    synthetic_sample, sample_metadata = generate_synthetic_data(
        df_sample, metadata_base_path, metadata_type="sample", use_same_metadata_version=use_same_metadata_version
    )


    diagnostic_report, quality_report = evaluate_synthetic_data(df_sample, synthetic_sample, sample_metadata)
    print(f"Diagnostics Report: {diagnostic_report}")
    print(f"Quality Report: {quality_report}")

    write_output_file(
        output_file_path, synthetic_sample, df_ce, layout, synthetic_header, header_layout
    )

if __name__ == '__main__':
    main()
