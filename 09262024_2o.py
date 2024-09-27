import os
import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import run_diagnostic, evaluate_quality
import util as ut
import random
import time
# File paths



# File paths
file_layout = r"C:\Users\saman\OneDrive\Desktop\project-x\file_layout.csv"
file_path = r"C:\Users\saman\OneDrive\Desktop\project-x\sample.txt"
# header_layout_file = r"C:\Users\saman\OneDrive\Desktop\project-x\header-layout.csv"
metadata_base_path = r"C:\Users\saman\OneDrive\Desktop\project-x\metadata"


# Create output file path
base_name = os.path.splitext(os.path.basename(file_path))[0]
output_file_path = os.path.join(os.path.dirname(file_path), f"{base_name}_syn.txt")

################## Custom Functions ##############
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

def process_cd_records(sample_data):
    # Process records that start with "CD100" into a DataFrame
    print("Processing CE records...")
    ce_rows = []
    
    for line in sample_data:
        if line.startswith("CD100"):  
            row_data = {"CD_Record": line.strip()}  
            ce_rows.append(row_data)

    df_ce = pd.DataFrame(ce_rows)
    print("CE records processed into DataFrame.")
    return df_ce

def process_file_data(data, layout, date_columns=[]):
    # Process the sample data according to the specified layout
    print("Processing data...")
    data_rows = []
    for line in data:
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

    # Handle date columns
    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors='coerce')  
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

########################### Build Trailer ###################################
def build_page_trailer(df):
    # Build the page trailer with summaries of the relevant columns
    record_count = len(df)

    df['net_amount_due'] = 0#df['net_amount_due'].apply(ut.get_return_value)
    df['gross_amount_due'] = 0 
    df['patient_pay_amount'] = 0 

    df['net_amount_due'] = pd.to_numeric(df['net_amount_due'], errors='coerce')
    net_amount_due_sum = df['net_amount_due'].sum() if 'net_amount_due' in df.columns else 0
    gross_amount_due_sum = 0 
    pat_paid_amount_sum = 0 

    trailer_data = (
        "PT" +
        str(record_count).zfill(10) +                         
        str(int(net_amount_due_sum)).zfill(11) +              
        "A" +
        str(int(gross_amount_due_sum)).zfill(11) +            
        "G" +
        str(int(pat_paid_amount_sum)).zfill(11) + "D"         
    )

    return trailer_data.ljust(48)[:48]

########################### Write file ######################################
def write_output_file(output_file_path, synthetic_data, df_ce, layout, synthetic_header, header_layout, date_columns=[]):
    # Write the synthetic data, header, and trailer to the output file
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
                    if column_name in date_columns:
                        value = value.strftime('%Y%m%d')  
                    transformed_data += str(value).ljust(length)
                else:
                    transformed_data += ''.ljust(length)
            outfile.write(transformed_data + '\n')

        # Write synthetic data for CE records
        for _, row in df_ce.iterrows():
            transformed_data = row['CD_Record'].strip()  
            outfile.write(transformed_data + '\n')

        # Build and write page trailer
        trailer_record = build_page_trailer(synthetic_data)
        outfile.write(trailer_record + '\n')

    print(f"Data written to {output_file_path}.")
#############################################################################

import time  # Add this import at the top of your script

def main(use_same_metadata_version=True):
    # Track the total execution time
    total_start_time = time.time()  # Start the total timer
    print('############################################################################')
    start_time = time.time()
    file_layout_df = read_file_layout(file_layout)
    hdr_file_layout = file_layout_df[file_layout_df['Type'] == 'HDR']
    de_file_layout = file_layout_df[file_layout_df['Type'] == 'DE']
    print(f"Time taken to read file layout to dataframe: {time.time() - start_time:.2f} seconds")
    print('############################################################################')
    start_time = time.time()
    raw_df = read_file_data(file_path)
    print(f"Time taken to load file data: {time.time() - start_time:.2f} seconds")
    print('############################################################################')
    start_time = time.time()
    header_df = process_file_data([raw_df[0]], hdr_file_layout)
    print(f"Time taken to process header data: {time.time() - start_time:.2f} seconds")
    
    start_time = time.time()
    synthetic_header_df, header_metadata_df = generate_synthetic_data(
        header_df, metadata_base_path, metadata_type="header", use_same_metadata_version=use_same_metadata_version
    )
    print(f"Time taken to generate synthetic header data: {time.time() - start_time:.2f} seconds")
    print('############################################################################')
    
    #date_columns = ['date_of_birth','date_of_service','billing_cycle_end_date','check_date','date_prescription_written','invoiced_date','cardholder_date_of_birth','adjudication_date']  # Replace with actual column names that are dates  # Replace with actual date column names
    date_columns = ['da']  # Replace with actual column names that are dates  # Replace with actual date column names
    
    start_time = time.time()
    tabluar_df = process_file_data(raw_df[1:], de_file_layout, date_columns)
    print(f"Time taken to process data into Dataframe: {time.time() - start_time:.2f} seconds")
    

    start_time = time.time()
    cd_df = process_cd_records(tabluar_df)
    print(f"Time taken to process CD records into Dataframe: : {time.time() - start_time:.2f} seconds")

    # Write the DataFrame to a CSV file in the same path
    tabluar_df.to_csv('dataframe_output_presdv.csv', index=False,mode='w')
    print('Write dataframe with header completed successfully')


    #Apply function to generate random numbers based on the length of each entry in 
    # start_time = time.time()
    # print(f"Random number generation started for senstive columns")
    # tabluar_df['cardholder_id'] = tabluar_df['cardholder_id'].apply(lambda x: generate_random_number(len(x) if x else 0))
    # tabluar_df['patient_id'] = tabluar_df['patient_id'].apply(lambda x: generate_random_number(len(x)))
    # tabluar_df['payment_reference_id'] = tabluar_df['payment_reference_id'].apply(lambda x: generate_random_number(len(x)))
    # tabluar_df['claim_reference_id'] = tabluar_df['claim_reference_id'].apply(lambda x: generate_random_number(len(x)))
    # tabluar_df['prior_authorization_number_submitted'] = tabluar_df['prior_authorization_number_submitted'].apply(lambda x: generate_random_number(len(x)))
    # tabluar_df['prior_authorization_number_assigned'] = tabluar_df['prior_authorization_number_assigned'].apply(lambda x: generate_random_number(len(x)))
    # tabluar_df['transaction_id_cross_reference'] = tabluar_df['transaction_id_cross_reference'].apply(lambda x: generate_random_number(len(x)))
    # tabluar_df['transaction_id'] = tabluar_df['transaction_id'].apply(lambda x: generate_random_number(len(x)))
    # tabluar_df['vendor_batch_code'] = tabluar_df['vendor_batch_code'].apply(lambda x: generate_random_number(len(x)))
    # tabluar_df['vendor_batch_code_cross_reference'] = tabluar_df['vendor_batch_code_cross_reference'].apply(lambda x: generate_random_number(len(x)))
    # tabluar_df['cardholder_id_alternate'] = tabluar_df['cardholder_id_alternate'].apply(lambda x: generate_random_number(len(x)))
    # print(f"Time taken to generate random numbers for sensitive columns: {time.time() - start_time:.2f} seconds")

    start_time = time.time()
    synthetic_data, data_metadata = generate_synthetic_data(
        tabluar_df, metadata_base_path, metadata_type="data", use_same_metadata_version=use_same_metadata_version
    )
    print(f"Time taken to generate synthetic data: {time.time() - start_time:.2f} seconds")

    # Write the DataFrame to a CSV file in the same path
    synthetic_data.to_csv('synthetic_data.csv', index=False)
    print('Write synthetic data with headers done')

    print('############################################################################')

    start_time = time.time()
    evaluate_synthetic_data(header_df, synthetic_header_df, header_metadata_df)
    evaluate_synthetic_data(tabluar_df, synthetic_data, data_metadata)
    print(f"Time taken to evaluate synthetic data: {time.time() - start_time:.2f} seconds")

    print('############################################################################')

    start_time = time.time()
    write_output_file(output_file_path, synthetic_data, cd_df, file_layout_df, synthetic_header_df, de_file_layout, date_columns)
    print(f"Time taken to write output file: {time.time() - start_time:.2f} seconds")
    


    #  write_output_file(
    #     output_file_path, synthetic_data, df_ce, layout, synthetic_header_df, header_layout, date_columns
    # )
    
    # Calculate total execution time
    # total_time_taken = time.time() - total_start_time
    # print(f"\nTotal time taken for the entire process: {total_time_taken:.2f} seconds")

    # print("\nProcessing complete.")
    print('############################################################################')

if __name__ == "__main__":
    main(use_same_metadata_version=True) 



