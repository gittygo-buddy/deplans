import random

# Function to generate a random number based on the length
def generate_number_by_length(length):
    if length <= 0:
        raise ValueError("Length must be greater than zero.")
    
    # Calculate the minimum and maximum number for the given length
    min_value = 10**(length - 1)  # Minimum value with the specified number of digits
    max_value = 10**length - 1    # Maximum value with the specified number of digits
    
    # Generate a random number within this range
    random_number = random.randint(min_value, max_value)
    return random_number

# Example usage:
length = 10
generated_number = generate_number_by_length(length)
print(f"Generated {length}-digit number: {generated_number}")