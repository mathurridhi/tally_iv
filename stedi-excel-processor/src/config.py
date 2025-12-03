import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    EXCEL_FILE_1 = os.getenv("EXCEL_FILE_1", "NTX IV_11272025.xlsx")
    EXCEL_FILE_2 = os.getenv("EXCEL_FILE_2", "P1 IV_11272025.xlsx")
    OUTPUT_FILE = os.getenv("OUTPUT_FILE", "output.xlsx")
    STEDI_API_URL = os.getenv("STEDI_API_URL", "https://api.stedi.com/v1/records")
    STEDI_API_KEY = os.getenv("STEDI_API_KEY")  # Ensure this is set in your environment
    MAX_WORKERS = int(os.getenv("MAX_WORKERS", 5))  # Default to 5 concurrent workers