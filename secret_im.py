from passlib.context import CryptContext

# Create a password context for hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# This module fetches secrets from your local secret server
def fetch_secrets():
    # Replace this with your actual secret-fetching logic
    username = "alice"  # You would fetch this from the secret server
    password = "password"  # Same for the password

    # Hash the password
    hashed_password = pwd_context.hash(password)
    
    return {
        "username": username,
        "full_name": "Alice Wonderland",
        "email": "alice@example.com",
        "hashed_password": hashed_password,
        "disabled": False,
    }
