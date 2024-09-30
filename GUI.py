import tkinter as tk
from tkinter import messagebox
import requests
from getguisecret import fetch_credentials  # Import the fetch_credentials function
import threading  # Import threading module
import os
import sys

# Global variable to hold the access token
access_token = None

# Function to check if the application is already running
def check_existing_instance():
    # If the application is already running, close the new instance
    if len(tk._default_root.winfo_children()) > 0:
        messagebox.showerror("Error", "Application is already running.")
        sys.exit()

# Function to handle login
def login():
    api_key = entry_api_key.get()

    # Fetch username and password using the provided API key
    credentials = fetch_credentials(api_key)
    if not credentials:
        messagebox.showerror("Error", "Invalid API Key", parent=app)
        return

    username = credentials["username"]
    password = credentials["password"]

    # Start a new thread for the login request
    threading.Thread(target=login_request, args=(username, password)).start()

def login_request(username, password):
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
            messagebox.showinfo("Login Successful", "Login Successful", parent=app)

            # Update instruction label
            instruction_label.config(text="Please enter your RE Number and RE Name below.")

            # Hide the login section and show report fields
            login_frame.grid_forget()
            show_report_fields()
        else:
            error_data = response.json()
            messagebox.showerror("Login Failed", error_data.get("detail", "Unknown error"), parent=app)

    except requests.exceptions.RequestException as e:
        messagebox.showerror("Request Failed", str(e), parent=app)

# Function to show fields for report submission
def show_report_fields():
    # Create and place labels and entry fields for RE Number and RE Name
    tk.Label(report_frame, text="RE Number:").grid(row=0, column=0, padx=10, pady=5)
    entry_re_number = tk.Entry(report_frame)
    entry_re_number.grid(row=0, column=1, padx=10, pady=5)

    tk.Label(report_frame, text="RE Name:").grid(row=1, column=0, padx=10, pady=5)
    entry_re_name = tk.Entry(report_frame)
    entry_re_name.grid(row=1, column=1, padx=10, pady=5)

    # Create and place the submit button
    submit_button = tk.Button(report_frame, text="Submit Report", command=lambda: submit_report(entry_re_number.get(), entry_re_name.get()))
    submit_button.grid(row=2, columnspan=2, padx=10, pady=10)

    # Show the report frame
    report_frame.grid(row=2, columnspan=2, padx=10, pady=10)

# Function to submit the report
def submit_report(re_number, re_name):
    if not re_number or not re_name:
        messagebox.showerror("Error", "Please fill in all fields.", parent=app)
        return

    # Start a new thread for the report submission request
    threading.Thread(target=submit_report_request, args=(re_number, re_name)).start()

def submit_report_request(re_number, re_name):
    # Send the report request
    try:
        response = requests.post("http://localhost:8000/generate_report", headers={
            'Authorization': f'Bearer {access_token}'
        }, params={
            're_number': re_number,
            're_name': re_name
        })

        if response.status_code == 200:
            # Get the response data
            response_data = response.json()
            messagebox.showinfo("Success", f"Report submitted successfully.\nResponse: {response_data}", parent=app)
        else:
            error_data = response.json()
            messagebox.showerror("Submission Failed", error_data.get("detail", "Unknown error"), parent=app)

    except requests.exceptions.RequestException as e:
        messagebox.showerror("Request Failed", str(e), parent=app)

# Create the main application window
app = tk.Tk()
app.title("API Authentication")
app.geometry("600x300")  # Set the window width and height

# Check if the application is already running
check_existing_instance()

# Create and place an instruction label
instruction_text = "Please enter your API Key to log in. After logging in, you can submit your RE Number and RE Name."
instruction_label = tk.Label(app, text=instruction_text, wraplength=580, justify="left")
instruction_label.grid(row=0, columnspan=2, padx=10, pady=10)

# Create a frame for login widgets
login_frame = tk.Frame(app)
login_frame.grid(row=1, columnspan=2, padx=10, pady=10)

# Create and place labels and entry fields for API Key
tk.Label(login_frame, text="API Key:").grid(row=0, column=0, padx=10, pady=5)
entry_api_key = tk.Entry(login_frame)
entry_api_key.grid(row=0, column=1, padx=10, pady=5)

# Create and place the login button
login_button = tk.Button(login_frame, text="Login", command=login)
login_button.grid(row=1, columnspan=2, padx=10, pady=10)

# Create a frame for report submission fields
report_frame = tk.Frame(app)

# Start the application
app.mainloop()
