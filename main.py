from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from datetime import timedelta, datetime
from typing import Optional
import secrets
from secret_im import fetch_secrets  # Update here

# Secret key and settings
SECRET_KEY = secrets.token_urlsafe(32)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Fetch user data from the secret server
fake_users_db = {
    "alice": fetch_secrets()
}

# Helper function to verify password
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

# Helper function to retrieve user
def get_user(db, username: str):
    if username in db:
        return db[username]

# Function to authenticate the user
def authenticate_user(db, username: str, password: str):
    user = get_user(db, username)
    if not user:
        return False  # User not found
    if not verify_password(password, user["hashed_password"]):
        return False  # Password does not match
    return user  # User authenticated successfully

# Function to create JWT token
def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

# FastAPI instance
app = FastAPI()

# Token endpoint
@app.post("/token")
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user["username"]}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

# Protected route
@app.get("/users/me")
async def read_users_me(token: str = Depends(oauth2_scheme)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    except JWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    user = get_user(fake_users_db, username)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")
    return user

# New endpoint to generate and write a report
@app.post("/generate_report")
async def generate_report(re_number: str, re_name: str, current_user: dict = Depends(read_users_me)):
    # Define the report content based on the authenticated user
    report_content = f"User Report:\n\n" \
                     f"Username: {current_user['username']}\n" \
                     f"RE Number: {re_number}\n" \
                     f"RE Name: {re_name}\n" \
                     f"Report generated at: {datetime.utcnow()}\n"

    # Write the report to a file
    report_file_path = f"report_{current_user['username']}.txt"
    with open(report_file_path, "w") as report_file:
        report_file.write(report_content)

    return {"message": "Report generated successfully", "file_path": report_file_path}


#uvicorn main:app --reload
