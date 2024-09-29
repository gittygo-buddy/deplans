import os
import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import run_diagnostic, evaluate_quality
import util as ut
import random
import time


# File paths
file_layout = r"C:\Users\saman\OneDrive\Desktop\project-x\file_layout_pd.csv"
file_path = r"C:\Users\saman\OneDrive\Desktop\project-x\sample - pd.txt"
metadata_base_path = r"C:\Users\saman\OneDrive\Desktop\project-x\metadata"


# Create output file path
base_name = os.path.splitext(os.path.basename(file_path))[0]
output_file_path = os.path.join(os.path.dirname(file_path), f"{base_name}_syn.txt")

# ################## Custom Functions ##############
def generate_random_number(length):
    # Generate a random number with the specified length
    if length is None or length == '' or length <= 0:
        return 0
    min_value = 10 ** (length - 1)
    max_value = 10 ** length - 1
    return random.randint(min_value, max_value)

def get_latest_metadata_version(base_path, metadata_type):
    # Get the latest version number of the specified metadata type
    version = 1
    while os.path.exists(f"{base_path}_{metadata_type}_v{version}.json"):
        version += 1
    return version - 1 if version > 1 else None

def save_new_metadata_version(base_path, metadata, metadata_type):
    # Save the new version of the metadata to a JSON file
    latest_version = get_latest_metadata_version(base_path, metadata_type)
    new_version = (latest_version + 1) if latest_version else 1
    new_metadata_path = f"{base_path}_{metadata_type}_v{new_version}.json"
    metadata.save_to_json(filepath=new_metadata_path)
    print(f"New {metadata_type} metadata saved as {new_metadata_path}")

def read_file_layout(file_layout):
    # Load the layout CSV file into a DataFrame
    print("Loading the file layout...")
    layout = pd.read_csv(file_layout).rename(columns=lambda x: x.strip())
    print("File layout loaded successfully.")
    return layout

def read_file_data(file_path):
    # Load the sample data from the specified file
    print("Loading file data...")
    with open(file_path, 'r') as infile:
        raw_data = infile.readlines()
    print("File data loaded successfully.")
    return raw_data

def process_file_data(data, layout, date_columns=[]):
    # Process the sample data according to the specified layout
    print("Processing data...")
    data_rows = []
    
    for line in data:
        current_position = 0
        row_data = {}
        for _, row in layout.iterrows():
            column_name = row['Column_Name']
            length = row['Length']
            # Extract and strip data based on the length defined in the layout
            row_data[column_name] = line[current_position:current_position + length].strip()
            current_position += length
        data_rows.append(row_data)

    # Convert the list of rows into a DataFrame
    df = pd.DataFrame(data_rows)

    # Handle date columns
    for column in date_columns:
        if column in df.columns:
            # Convert columns to datetime, coerce errors to NaT (Not a Time)
            df[column] = pd.to_datetime(df[column], errors='coerce')
            # Fill any invalid dates (NaT) with NaT explicitly (optional)
            df[column] = df[column].fillna(pd.NaT)

    print("Data processed into DataFrame.")
    return df

def generate_synthetic_data(df, metadata_base_path, metadata_type, use_same_metadata_version=True):
    # Generate synthetic data using the SDV library
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
    # Evaluate the quality of the synthetic data
    print("Evaluating synthetic data quality...")
    diagnostic = run_diagnostic(real_data=df, synthetic_data=synthetic_data, metadata=metadata)
    quality_report = evaluate_quality(df, synthetic_data, metadata)
    print("Evaluation complete.")
    return diagnostic, quality_report

# ########################### Write file ######################################
def write_output_file(output_file_path, hdr_df, bhd_df, det_df, btr_df, tlr_df, layout_dict, date_columns=[]):
    # Write the synthetic data, header, and trailer to the output file
    print("Writing output to file...")
    with open(output_file_path, 'w') as outfile:
        
        # Function to write DataFrame to file with a given layout
        def write_dataframe(df, layout):
            for _, row in df.iterrows():
                transformed_data = ""
                for column_name, length in zip(layout['Column_Name'], layout['Length']):
                    value = row[column_name]
                    if pd.notna(value):
                        if column_name in date_columns:
                            value = value.strftime('%Y%m%d')  # Format date as needed
                        transformed_data += str(value).ljust(length)
                    else:
                        transformed_data += ''.ljust(length)
                outfile.write(transformed_data + '\n')
        
        # Write the synthetic header (hdr_df)
        print("Writing HDR data...")
        write_dataframe(hdr_df, layout_dict['hdr_layout'])
        
        # Write the synthetic bhd data
        print("Writing BHD data...")
        write_dataframe(bhd_df, layout_dict['bhd_layout'])
        
        # Write the synthetic det data
        print("Writing DET data...")
        write_dataframe(det_df, layout_dict['det_layout'])
        
        # Write the synthetic btr data
        print("Writing BTR data...")
        write_dataframe(btr_df, layout_dict['btr_layout'])
        
        # Write the synthetic tlr data
        print("Writing TLR data...")
        write_dataframe(tlr_df, layout_dict['tlr_layout'])
        
    print(f"Data written to {output_file_path}.")
# #############################################################################

def main(use_same_metadata_version=True):
    # Track the total execution time
    total_start_time = time.time()  # Start the total timer
    print('############################################################################')

    # Time tracking for reading file layout
    start_time = time.time()
    file_layout_df = read_file_layout(file_layout)
    hdr_file_layout = file_layout_df[file_layout_df['Type'] == 'HDR']
    bhd_file_layout = file_layout_df[file_layout_df['Type'] == 'BHD']
    det_file_layout = file_layout_df[file_layout_df['Type'] == 'DET']
    btr_file_layout = file_layout_df[file_layout_df['Type'] == 'BTR']
    tlr_file_layout = file_layout_df[file_layout_df['Type'] == 'TLR']
    print(f"Time taken to read file layout to dataframe: {time.time() - start_time:.2f} seconds")
    print('############################################################################')

    # Time tracking for loading file data
    start_time = time.time()
    raw_df = read_file_data(file_path)
    print(f"Time taken to load file data: {time.time() - start_time:.2f} seconds")
    print('############################################################################')

    # Time tracking for processing header and trailer
    start_time = time.time()
    # Process first two rows (rows 0 and 1)
    hdr_df = process_file_data([raw_df[0]], hdr_file_layout)
    bhd_df = process_file_data([raw_df[1]], bhd_file_layout)

    # Process second-to-last and last rows (use negative indexing)
    btr_df = process_file_data([raw_df[-2]], btr_file_layout)  # Second-to-last row
    tlr_df = process_file_data([raw_df[-1]], tlr_file_layout)  # Last row
    
    det_df = process_file_data(raw_df[2:-2], det_file_layout)
    print(f"Time taken to process header and trailer: {time.time() - start_time:.2f} seconds")

    # Time tracking for generating synthetic data for each section
    date_columns = []

    start_time = time.time()
    synthetic_hdr_df, hdr_metadata_df = generate_synthetic_data(
        hdr_df, metadata_base_path, metadata_type="hdr", use_same_metadata_version=use_same_metadata_version
    )
    print(f"Time taken to generate synthetic header data: {time.time() - start_time:.2f} seconds")

    start_time = time.time()
    synthetic_bhd_df, bhd_metadata_df = generate_synthetic_data(
        bhd_df, metadata_base_path, metadata_type="bhd", use_same_metadata_version=use_same_metadata_version
    )
    print(f"Time taken to generate synthetic BHD data: {time.time() - start_time:.2f} seconds")

    start_time = time.time()
    synthetic_det_df, det_metadata_df = generate_synthetic_data(
        det_df, metadata_base_path, metadata_type="det", use_same_metadata_version=use_same_metadata_version
    )
    print(f"Time taken to generate synthetic DET data: {time.time() - start_time:.2f} seconds")

    start_time = time.time()
    synthetic_btr_df, btr_metadata_df = generate_synthetic_data(
        btr_df, metadata_base_path, metadata_type="btr", use_same_metadata_version=use_same_metadata_version
    )
    print(f"Time taken to generate synthetic BTR data: {time.time() - start_time:.2f} seconds")

    start_time = time.time()
    synthetic_tlr_df, tlr_metadata_df = generate_synthetic_data(
        tlr_df, metadata_base_path, metadata_type="tlr", use_same_metadata_version=use_same_metadata_version
    )
    print(f"Time taken to generate synthetic TLR data: {time.time() - start_time:.2f} seconds")

    # Time tracking for writing the output file
    start_time = time.time()
    write_output_file(
        output_file_path=output_file_path,            # Path to the output file
        hdr_df=synthetic_hdr_df,                       # Synthetic header data (hdr_df)
        bhd_df=synthetic_bhd_df,                       # Synthetic BHD data (hdr_df)
        det_df=synthetic_det_df,                       # Synthetic data (DET)
        btr_df=synthetic_btr_df,                       # Synthetic BTR data
        tlr_df=synthetic_tlr_df,                       # Synthetic TLR data
        layout_dict={                                  # Dictionary containing layouts for each data section
            'hdr_layout': hdr_file_layout,             # Header layout
            'bhd_layout': bhd_file_layout,             # BHD layout (header1)
            'det_layout': det_file_layout,             # DET layout
            'btr_layout': btr_file_layout,             # BTR layout
            'tlr_layout': tlr_file_layout              # TLR layout
        },
        date_columns=date_columns                       # List of date columns (if applicable)
    )
    print(f"Time taken to write output file: {time.time() - start_time:.2f} seconds")


    #Evaluate the quality of synthetic data
    start_time = time.time()
    evaluate_synthetic_data(hdr_df, synthetic_hdr_df, hdr_metadata_df)
    evaluate_synthetic_data(bhd_df, synthetic_bhd_df, bhd_metadata_df)
    evaluate_synthetic_data(det_df, synthetic_det_df, det_metadata_df)
    evaluate_synthetic_data(btr_df, synthetic_btr_df, btr_metadata_df)
    evaluate_synthetic_data(tlr_df, synthetic_tlr_df, tlr_metadata_df)
    print(f"Time taken for data evaluation: {time.time() - start_time:.2f} seconds")
    print(f"Total execution time: {time.time() - total_start_time:.2f} seconds")

if __name__ == "__main__":
    main(use_same_metadata_version=True)


