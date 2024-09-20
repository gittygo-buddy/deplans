import os

# File paths
output_file_path = r"C:\Users\saman\OneDrive\Desktop\project-x\generated_file.txt"

# First row, middle sample rows, and last row
first_row = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"
last_row = "CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC\n"

# Function to generate a middle row based on some pattern
def generate_middle_row(row_num):
    patterns = [
        "AB  X\n",
        "DBZKX\n",
        "DBZK \n"
    ]
    # Cycle through patterns
    return patterns[row_num % len(patterns)]

# Generate the file with 50,000 rows between the first and last row
def generate_file(output_file_path, num_rows=50000):
    print(f"Generating file with {num_rows} rows between the first and last line...")
    with open(output_file_path, 'w') as outfile:
        # Write the first row
        outfile.write(first_row)
        
        # Generate and write 50,000 rows
        for i in range(num_rows):
            outfile.write(generate_middle_row(i))
        
        # Write the last row
        outfile.write(last_row)

    print(f"File generated and saved to: {output_file_path}")

# Main function
if __name__ == "__main__":
    generate_file(output_file_path)
