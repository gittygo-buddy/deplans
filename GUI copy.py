import tkinter as tk
from tkinter import messagebox
import requests
from getguisecret import fetch_credentials  # Import the fetch_credentials function

# Global variable to hold the access token
access_token = None

# Function to handle login
def login():
    api_key = entry_api_key.get()

    # Fetch username and password using the provided API key
    credentials = fetch_credentials(api_key)
    if not credentials:
        messagebox.showerror("Error", "Invalid API Key")
        return

    username = credentials["username"]
    password = credentials["password"]

    # Send login request
    try:
        response = requests.post("http://localhost:8000/token", data={
            'username': username,  # Use fetched username
            'password': password   # Use fetched password
        })

        if response.status_code == 200:
            data = response.json()
            global access_token
            access_token = data["access_token"]
            messagebox.showinfo("Login Successful", "Login Successful")

            # Show fields to submit RE Number and RE Name
            show_report_fields()
        else:
            error_data = response.json()
            messagebox.showerror("Login Failed", error_data.get("detail", "Unknown error"))

    except requests.exceptions.RequestException as e:
        messagebox.showerror("Request Failed", str(e))

# Function to show fields for report submission
def show_report_fields():
    # Clear previous widgets if they exist
    for widget in app.grid_slaves():
        if (widget.grid_info()["row"] in ['2', '3', '4']):
            widget.grid_forget()

    # Create and place labels and entry fields for RE Number and RE Name
    tk.Label(app, text="RE Number:").grid(row=2, column=0, padx=10, pady=5)
    entry_re_number = tk.Entry(app)
    entry_re_number.grid(row=2, column=1, padx=10, pady=5)

    tk.Label(app, text="RE Name:").grid(row=3, column=0, padx=10, pady=5)
    entry_re_name = tk.Entry(app)
    entry_re_name.grid(row=3, column=1, padx=10, pady=5)

    # Create and place the submit button
    submit_button = tk.Button(app, text="Submit Report", command=lambda: submit_report(entry_re_number.get(), entry_re_name.get()))
    submit_button.grid(row=4, columnspan=2, padx=10, pady=10)

# Function to submit the report
def submit_report(re_number, re_name):
    if not re_number or not re_name:
        messagebox.showerror("Error", "Please fill in all fields.")
        return

    # Send the report request
    try:
        response = requests.post("http://localhost:8000/generate_report", headers={
            'Authorization': f'Bearer {access_token}'
        }, params={
            're_number': re_number,
            're_name': re_name
        })

        if response.status_code == 200:
            messagebox.showinfo("Success", "Report submitted successfully.")
        else:
            error_data = response.json()
            messagebox.showerror("Submission Failed", error_data.get("detail", "Unknown error"))

    except requests.exceptions.RequestException as e:
        messagebox.showerror("Request Failed", str(e))

# Create the main application window
app = tk.Tk()
app.title("API Authentication")

# Create and place labels and entry fields for API Key
tk.Label(app, text="API Key:").grid(row=0, column=0, padx=10, pady=5)
entry_api_key = tk.Entry(app)
entry_api_key.grid(row=0, column=1, padx=10, pady=5)

# Create and place the login button
login_button = tk.Button(app, text="Login", command=login)
login_button.grid(row=1, columnspan=2, padx=10, pady=10)

# Start the application
app.mainloop()
