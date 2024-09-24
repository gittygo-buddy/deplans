import os
import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import run_diagnostic, evaluate_quality
import config  # Import the paths from config.py

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

def load_layout(file_layout):
    print("Loading the layout...")
    layout = pd.read_csv(file_layout).rename(columns=lambda x: x.strip())
    print("Layout loaded successfully.")
    return layout

def load_sample_data(file_path):
    print("Loading sample data...")
    with open(file_path, 'r') as infile:
        sample_data = infile.readlines()[1:-1]  # Remove first and last rows
    print("Sample data loaded. First and last rows removed.")
    return sample_data

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

def evaluate_synthetic_data(df, synthetic_data, metadata):
    print("Evaluating synthetic data quality...")
    diagnostic = run_diagnostic(real_data=df, synthetic_data=synthetic_data, metadata=metadata)
    quality_report = evaluate_quality(df, synthetic_data, metadata)
    print("Evaluation complete.")
    return diagnostic, quality_report

########################### Build Trailer ###################################
def build_page_trailer(df):
    record_count = len(df)

    # Ensure numeric columns are correctly processed
    df['net_amount_due'] = pd.to_numeric(df['net_amount_due'], errors='coerce')
    net_amount_due_sum = df['net_amount_due'].sum() if 'net_amount_due' in df.columns else 0
    gross_amount_due_sum = df['gross_amount_due'].sum() if 'gross_amount_due' in df.columns else 0
    pat_paid_amount_sum = df['patient_pay_amount'].sum() if 'patient_pay_amount' in df.columns else 0

    trailer_data = (
        "PT" +
        str(record_count).zfill(10) +
        str(int(net_amount_due_sum)).zfill(11) +
        "A" +
        str(int(gross_amount_due_sum)).zfill(11) +
        "G" +
        str(int(pat_paid_amount_sum)).zfill(11) + "D"
    )

    return trailer_data.ljust(48)[:48]  # Ensure length is exactly 48 characters
#############################################################################
########################### Write file ######################################
def write_output_file(output_file_path, synthetic_data, layout, synthetic_header, header_layout, date_columns=[]):
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

        # Build and write page trailer
        trailer_record = build_page_trailer(synthetic_data)
        outfile.write(trailer_record + '\n')

    print(f"Data written to {output_file_path}.")
#############################################################################

def main(use_same_metadata_version=True):
    layout = load_layout(config.file_layout)  # Use the imported config paths
    header_layout = load_layout(config.header_layout_file)
    
    sample_data = load_sample_data(config.file_path)
    header_df = process_sample_data([sample_data[0]], header_layout)
    
    print("Header DataFrame:")
    print(header_df)

    synthetic_header, header_metadata = generate_synthetic_data(
        header_df, config.metadata_base_path, metadata_type="header", use_same_metadata_version=use_same_metadata_version
    )
   
    date_columns = ['da']  # Replace with actual column names that are dates
    df = process_sample_data(sample_data, layout, date_columns)  # Pass date_columns here

    synthetic_data, data_metadata = generate_synthetic_data(
        df, config.metadata_base_path, metadata_type="data", use_same_metadata_version=use_same_metadata_version
    )

    evaluate_synthetic_data(header_df, synthetic_header, header_metadata)
    evaluate_synthetic_data(df, synthetic_data, data_metadata)

    write_output_file(config.output_file_path, synthetic_data, layout, synthetic_header, header_layout, date_columns)

    print("\nProcessing complete.")

if __name__ == "__main__":
    main(use_same_metadata_version=True)  # Set to False to allow metadata updates
