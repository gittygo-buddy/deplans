def get_return_value(inp_L):
    # Extract the last character from the input
    last_char = inp_L[-1]

    # Mapping logic from SQL to Python
    if last_char == '}':
        return float(inp_L.replace('}', '0')) * -1
    elif last_char == '{':
        return float(inp_L.replace('{', '0')) * 1
    elif last_char == 'A':
        return float(inp_L.replace('A', '1')) * -1
    elif last_char == 'B':
        return float(inp_L.replace('B', '2')) * -1
    elif last_char == 'C':
        return float(inp_L.replace('C', '3')) * -1
    elif last_char == 'D':
        return float(inp_L.replace('D', '4')) * -1
    elif last_char == 'E':
        return float(inp_L.replace('E', '5')) * -1
    elif last_char == 'F':
        return float(inp_L.replace('F', '6')) * -1
    elif last_char == 'G':
        return float(inp_L.replace('G', '7')) * -1
    elif last_char == 'H':
        return float(inp_L.replace('H', '8')) * -1
    elif last_char == 'I':
        return float(inp_L.replace('I', '9')) * -1
    elif last_char == 'J':
        return float(inp_L.replace('J', '1')) * 1
    elif last_char == 'K':
        return float(inp_L.replace('K', '2')) * 1
    elif last_char == 'L':
        return float(inp_L.replace('L', '3')) * 1
    elif last_char == 'M':
        return float(inp_L.replace('M', '4')) * 1
    elif last_char == 'N':
        return float(inp_L.replace('N', '5')) * 1
    elif last_char == 'O':
        return float(inp_L.replace('O', '6')) * 1
    elif last_char == 'P':
        return float(inp_L.replace('P', '7')) * 1
    elif last_char == 'Q':
        return float(inp_L.replace('Q', '8')) * 1
    elif last_char == 'R':
        return float(inp_L.replace('R', '9')) * 1
    elif last_char in '0123456789':
        return float(inp_L)
    elif inp_L.isnumeric():
        return float(inp_L) * -1
    else:
        return 0.0

# # Example usage
# input_value = "0015083H"
# result = get_return_value(input_value)/100
# print(result)  # Expected output: -839.0
