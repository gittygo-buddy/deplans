import os
import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import run_diagnostic, evaluate_quality
import util as ut
import random
import time

# File paths
file_layout = r"C:\Users\saman\OneDrive\Desktop\project-x\layout.csv"
file_path = r"C:\Users\saman\OneDrive\Desktop\project-x\sample.txt"
header_layout_file = r"C:\Users\saman\OneDrive\Desktop\project-x\header-layout.csv"
metadata_base_path = r"C:\Users\saman\OneDrive\Desktop\project-x\metadata"


# Create output file path
base_name = os.path.splitext(os.path.basename(file_path))[0]
output_file_path = os.path.join(os.path.dirname(file_path), f"{base_name}_syn.txt")

################## Custom Functions ##############
def generate_random_number(length):
    if length is None or length == '' or length <= 0:
        return 0
    min_value = 10 ** (length - 1)
    max_value = 10 ** length - 1
    return random.randint(min_value, max_value)

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
    layout = pd.read_csv(file_layout).rename(columns=lambda x: x.strip())
    return layout

def load_sample_data(file_path):
    with open(file_path, 'r') as infile:
        sample_data = infile.readlines()
    return sample_data

def process_cd_records(sample_data):
    ce_rows = []
    for line in sample_data:
        if line.startswith("CD100"):
            row_data = {"CD_Record": line.strip()}
            ce_rows.append(row_data)
    df_ce = pd.DataFrame(ce_rows)
    return df_ce

def process_sample_data(sample_data, layout, date_columns=[]):
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

    for column in date_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors='coerce')
            df[column] = df[column].fillna(pd.NaT)
    
    return df

def generate_synthetic_data(df, metadata_base_path, metadata_type, use_same_metadata_version=True):
    metadata = SingleTableMetadata()
    latest_version = get_latest_metadata_version(metadata_base_path, metadata_type)

    if latest_version and use_same_metadata_version:
        latest_metadata_path = f"{metadata_base_path}_{metadata_type}_v{latest_version}.json"
        metadata = SingleTableMetadata.load_from_json(filepath=latest_metadata_path)
    else:
        metadata.detect_from_dataframe(df)
        save_new_metadata_version(metadata_base_path, metadata, metadata_type)

    for column in df.columns:
        df[column] = df[column].where(df[column].notna(), pd.NA)

    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(df)
    synthetic_data = synthesizer.sample(num_rows=len(df))
    return synthetic_data, metadata

def evaluate_synthetic_data(df, synthetic_data, metadata):
    diagnostic = run_diagnostic(real_data=df, synthetic_data=synthetic_data, metadata=metadata)
    quality_report = evaluate_quality(df, synthetic_data, metadata)
    return diagnostic, quality_report

########################### Build Trailer ###################################
def build_page_trailer(df):
    record_count = len(df)
    df['net_amount_due'] = 0  
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
    with open(output_file_path, 'w') as outfile:
        for _, row in synthetic_header.iterrows():
            transformed_data = "".join(str(row[layout_row['Column_Name']]).ljust(layout_row['Length'])
                                       for _, layout_row in header_layout.iterrows())
            outfile.write(transformed_data + '\n')

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

        for _, row in df_ce.iterrows():
            transformed_data = row['CD_Record'].strip()
            outfile.write(transformed_data + '\n')

        trailer_record = build_page_trailer(synthetic_data)
        outfile.write(trailer_record + '\n')
#############################################################################
import time

def main(use_same_metadata_version=True):
    total_start_time = time.time()  
    
    layout = load_layout(file_layout)
    header_layout = load_layout(header_layout_file)
    sample_data = load_sample_data(file_path)
    header_df = process_sample_data([sample_data[0]], header_layout)
    
    synthetic_header, header_metadata = generate_synthetic_data(
        header_df, metadata_base_path, metadata_type="header", use_same_metadata_version=use_same_metadata_version
    )

    # date_columns = [
    #     'date_of_birth', 'date_of_service', 'billing_cycle_end_date', 'check_date',
    #     'date_prescription_written', 'invoiced_date', 'cardholder_date_of_birth', 'adjudication_date'
    # ]
    
    date_columns = ['da']

    df = process_sample_data(sample_data[1:], layout, date_columns)
    df_ce = process_cd_records(sample_data)

    df.to_csv('dataframe_output_presdv.csv', index=False, mode='w')

    # df['cardholder_id'] = df['cardholder_id'].apply(lambda x: generate_random_number(len(x) if x else 0))
    # df['patient_id'] = df['patient_id'].apply(lambda x: generate_random_number(len(x)))
    # df['payment_reference_id'] = df['payment_reference_id'].apply(lambda x: generate_random_number(len(x)))
    # df['claim_reference_id'] = df['claim_reference_id'].apply(lambda x: generate_random_number(len(x)))
    # df['prior_authorization_number_submitted'] = df['prior_authorization_number_submitted'].apply(lambda x: generate_random_number(len(x)))

    synthetic_data, data_metadata = generate_synthetic_data(
        df, metadata_base_path, metadata_type="data", use_same_metadata_version=use_same_metadata_version
    )

    write_output_file(
        output_file_path, synthetic_data, df_ce, layout, synthetic_header, header_layout, date_columns
    )

    total_time = time.time() - total_start_time
    print(f"Total time taken for the process: {total_time:.2f} seconds")

if __name__ == "__main__":
    main()
