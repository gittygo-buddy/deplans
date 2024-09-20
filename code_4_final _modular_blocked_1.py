import os
import pandas as pd
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import run_diagnostic, evaluate_quality

# File paths
file_layout = r"C:\Users\saman\OneDrive\Desktop\project-x\layout.csv"
file_path = r"C:\Users\saman\OneDrive\Desktop\project-x\generated_file.txt"
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

# Step 1: Load the layout CSV file and group columns by Block_ID (without including Block_ID in data)
def load_layout(file_layout):
    print("Loading the layout...")
    layout = pd.read_csv(file_layout)
    layout.columns = [col.strip() for col in layout.columns]
    
    # Group columns by block (Block_ID is only used for grouping, not included in the data)
    block_columns = {}
    for _, row in layout.iterrows():
        block_id = row['Block_ID']
        column_name = row['Column_Name']
        if block_id not in block_columns:
            block_columns[block_id] = []
        block_columns[block_id].append(column_name)
    
    print("Layout loaded successfully.")
    return layout, block_columns

# Step 2: Load sample data from the text file
def load_sample_data(file_path):
    print("Loading sample data...")
    with open(file_path, 'r') as f:
        lines = f.readlines()
    first_row = lines[0].strip()
    last_row = lines[-1].strip()
    data = lines[1:-1]  # Exclude first and last row
    print("Sample data loaded.")
    return data, first_row, last_row

# Step 3: Process sample data using the layout (without using Block_ID in the DataFrame)
def process_sample_data(sample_data, layout):
    print("Processing sample data...")
    data_rows = []
    block_ids = []  # Store block IDs for appending later
    for line in sample_data:
        line = line.strip()
        current_position = 0
        row_data = {}
        for _, row in layout.iterrows():
            column_name = row['Column_Name']  # Only use the column names for data extraction
            length = row['Length']
            block_id = row['Block_ID']  # Capture the Block_ID
            column_data = line[current_position:current_position + length].strip()
            row_data[column_name] = column_data
            current_position += length
        data_rows.append(row_data)
        block_ids.append(block_id)  # Keep track of the block IDs for each row
    df = pd.DataFrame(data_rows)
    print("Data processed into DataFrame.")
    return df, block_ids

# Step 4: Generate synthetic data (Block_ID is not included in the DataFrame)
def generate_synthetic_data(df, metadata_base_path, block_columns, use_same_metadata_version=True, ignored_blocks=None):
    print("Generating synthetic data using SDV...")

    # Set default for ignored_blocks if not provided
    if ignored_blocks is None:
        ignored_blocks = []

    # Create a new DataFrame excluding ignored blocks
    columns_to_ignore = []
    for block in ignored_blocks:
        if block in block_columns:
            columns_to_ignore.extend(block_columns[block])

    filtered_df = df.drop(columns=columns_to_ignore, errors='ignore')

    # Create a dictionary to hold the masks for each column (preserve column-level missing values)
    empty_value_masks = {}
    for column in filtered_df.columns:
        empty_value_masks[column] = filtered_df[column].isna() | (filtered_df[column] == '')  # Mask for empty values

    metadata = SingleTableMetadata()
    latest_version = get_latest_metadata_version(metadata_base_path)

    if latest_version and use_same_metadata_version:
        latest_metadata_path = f"{metadata_base_path}_v{latest_version}.json"
        print(f"Using existing metadata: {latest_metadata_path}")
        metadata = SingleTableMetadata.load_from_json(filepath=latest_metadata_path)
    else:
        print("No existing metadata found or creating new metadata.")
        metadata.detect_from_dataframe(filtered_df)  # Use filtered DataFrame
        save_new_metadata_version(metadata_base_path, metadata)

    synthesizer = GaussianCopulaSynthesizer(metadata)
    synthesizer.fit(filtered_df)
    synthetic_data = synthesizer.sample(num_rows=len(filtered_df))

    # Post-processing: Re-apply the empty values from the original data to the synthetic data column-by-column
    for column, mask in empty_value_masks.items():
        synthetic_data.loc[mask, column] = ''  # Apply the empty mask for each column individually

    print("Synthetic data generated.")
    return synthetic_data, metadata

# Step 5: Evaluate synthetic data
def evaluate_synthetic_data(df, synthetic_data, metadata):
    print("Evaluating synthetic data quality...")
    
    # Generate a diagnostic report
    diagnostic_report = run_diagnostic(synthetic_data, df, metadata)
    
    # Evaluate quality using evaluate_quality with the required metadata
    quality_report = evaluate_quality(synthetic_data, df, metadata)
    
    print("Evaluation completed.")
    return diagnostic_report, quality_report


# Step 6: Write output file and append Block_IDs back to synthetic data
def write_output_file(output_file_path, synthetic_data, layout, first_row, last_row, block_ids):
    print("Writing output to file...")

    with open(output_file_path, 'w') as outfile:
        # Write original first row
        outfile.write(first_row + "\n")
        # Write synthetic data with Block_IDs appended back
        for index, row in synthetic_data.iterrows():
            transformed_data = ""
            current_position = 0
            for _, layout_row in layout.iterrows():
                column_name = layout_row['Column_Name']
                length = layout_row['Length']
                block_id = block_ids[index]  # Get the Block_ID for the current row
                # Preserve original empty values in the output
                if pd.isna(row[column_name]) or row[column_name] == '':
                    column_data = ''.ljust(length)  # Maintain empty space
                else:
                    column_data = str(row[column_name]).ljust(length)  # Format with padding
                transformed_data += column_data
            outfile.write(f"{block_ids[index]} {transformed_data}\n")  # Append Block_ID before writing data
        # Write original last row
        outfile.write(last_row)
    print(f"Data written to {output_file_path}.")

# Main function to run all steps
def main(use_same_metadata_version=True, ignored_blocks=None):
    layout, block_columns = load_layout(file_layout)
    sample_data, first_row, last_row = load_sample_data(file_path)
    df, block_ids = process_sample_data(sample_data, layout)  # Capture block_ids
    
    synthetic_data, metadata = generate_synthetic_data(df, metadata_base_path, block_columns, use_same_metadata_version, ignored_blocks)
    diagnostic, quality_report = evaluate_synthetic_data(df, synthetic_data, metadata)
    
    if not use_same_metadata_version:
        save_new_metadata_version(metadata_base_path, metadata)
    
    write_output_file(output_file_path, synthetic_data, layout, first_row, last_row, block_ids)  # Pass block_ids for appending

# Run the main function with the flag to control metadata versioning and specify blocks to ignore
if __name__ == "__main__":
    ignored_blocks = [1]  # Specify the block IDs to ignore
    main(use_same_metadata_version=False, ignored_blocks=ignored_blocks)
