import csv

# Define field specifications (field name, length)
fields = [
    ("claim_number", 10),
    ("patient_id", 8),
    ("provider_id", 6),
    ("service_date", 10),
    ("admission_date", 10),
    ("diagnosis_code", 7),
    ("procedure_code", 5),
    ("amount_due", 10),
    ("claim_status", 1),
    ("payer_id", 6),
    ("date_of_birth", 10),
    ("discharge_date", 10),
    ("net_amount_due", 10),
    ("social_security_number", 9),
    ("credit_card_number", 16),
    ("phone_number", 10),
]

def parse_record(record):
    """Parse a single positional record into a list of values."""
    parsed_record = []
    start = 0
    
    for field_name, length in fields:
        # Extract the field using its length
        field_value = record[start:start + length].strip()  # Remove padding spaces
        parsed_record.append(field_value)
        start += length
    
    return parsed_record

def positional_to_csv(input_file_path, output_file_path):
    """Convert a positional health claims data file to a CSV format."""
    with open(input_file_path, 'r') as input_file, open(output_file_path, 'w', newline='') as output_file:
        csv_writer = csv.writer(output_file)
        
        # Write the header (column names)
        csv_writer.writerow([field_name for field_name, length in fields])
        
        # Write each parsed record to the CSV
        for record in input_file:
            parsed_record = parse_record(record)
            csv_writer.writerow(parsed_record)

# File paths
input_file_path = r'C:\Users\saman\OneDrive\Desktop\project-x\health_claims_data_sensitive_50000.txt'  # Input positional data file (generated earlier)
output_file_path = 'health_claims_data_sensitive.csv'       # Output CSV file

# Convert positional data to CSV
positional_to_csv(input_file_path, output_file_path)

print(f"Positional data converted to CSV: {output_file_path}")
