import random
import datetime

# Define field specifications (field name, length, type)
fields = [
    ("claim_number", 10, 'string'),           # Alphanumeric claim number
    ("patient_id", 8, 'string'),              # Alphanumeric patient ID
    ("provider_id", 6, 'string'),             # Alphanumeric provider ID
    ("service_date", 10, 'date'),             # Date of service
    ("admission_date", 10, 'date'),           # Admission date
    ("diagnosis_code", 7, 'string'),          # Alphanumeric diagnosis code
    ("procedure_code", 5, 'string'),          # Alphanumeric procedure code
    ("amount_due", 10, 'amount'),             # Amount due, right aligned
    ("claim_status", 1, 'string'),            # Claim status (1 character)
    ("payer_id", 6, 'string'),                # Alphanumeric payer ID
    ("date_of_birth", 10, 'date'),            # Patient's date of birth
    ("discharge_date", 10, 'date'),           # Discharge date
    ("net_amount_due", 10, 'amount'),         # Net amount due, right aligned
    ("social_security_number", 9, 'ssn'),     # 9-digit SSN
    ("credit_card_number", 16, 'credit_card'),# 16-digit credit card number
    ("phone_number", 10, 'phone'),            # 10-digit phone number
]

def random_string(length):
    """Generate a random alphanumeric string."""
    return ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789', k=length))

def random_date(start_year=2000, end_year=2023):
    """Generate a random date between the given range in YYYYMMDD format."""
    start = datetime.date(start_year, 1, 1)
    end = datetime.date(end_year, 12, 31)
    return (start + (end - start) * random.random()).strftime('%Y%m%d')

def random_ssn():
    """Generate a random 9-digit Social Security Number (SSN)."""
    return f"{random.randint(100000000, 999999999)}"

def random_credit_card():
    """Generate a random 16-digit credit card number."""
    return ''.join(random.choices('0123456789', k=16))

def random_phone_number():
    """Generate a random 10-digit phone number."""
    return f"{random.randint(1000000000, 9999999999)}"

def generate_record():
    """Generate a single health claim record with valid formats, including sensitive data."""
    record = []
    
    for field_name, length, field_type in fields:
        if field_type == 'string':
            value = random_string(length)
        elif field_type == 'date':
            value = random_date()
        elif field_type == 'amount':
            value = f"{random.randint(1, 100000):>{length}}"  # Right aligned
        elif field_type == 'ssn':
            value = random_ssn()
        elif field_type == 'credit_card':
            value = random_credit_card()
        elif field_type == 'phone':
            value = random_phone_number()
        else:
            value = ''.join(' ' for _ in range(length))  # Empty string if unknown
        
        # Ensure the value is exactly the required length
        record.append(f"{value[:length]:<{length}}")  # Pad with spaces if too short
    
    return ''.join(record)

def generate_file(file_path, num_records=50000):
    """Generate a positional health claims data file with sensitive data."""
    with open(file_path, 'w') as file:
        for _ in range(num_records):
            record = generate_record()
            file.write(record + '\n')

# Generate 50,000 records and write them to a file
output_file_path = 'health_claims_data_sensitive_50000.txt'
generate_file(output_file_path, num_records=50000)

print(f"Health claims data file with sensitive columns generated: {output_file_path}")
