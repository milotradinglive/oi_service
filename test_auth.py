import os, sys
print("Python:", sys.version)
p = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
print("GAC =", p)

from google.oauth2 import service_account as sa
scopes = [
  "https://www.googleapis.com/auth/spreadsheets",
  "https://www.googleapis.com/auth/drive"
]
creds = sa.Credentials.from_service_account_file(p, scopes=scopes)
print("Credenciales OK. Service account:", creds.service_account_email)
