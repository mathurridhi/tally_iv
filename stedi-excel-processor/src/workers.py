from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from src.excel_io import load_excel_data, save_responses_to_excel
from src.stedi_client import StediClient

def process_record(record):
    client = StediClient()
    payload = build_payload(record)
    response = client.send_request(payload)
    return response

def build_payload(record):
    # Implement the logic to build the payload from the record
    return {
        "field1": record["Field1"],
        "field2": record["Field2"],
        # Add more fields as necessary
    }

def process_excel_files(file_paths):
    records = []
    for file_path in file_paths:
        data = load_excel_data(file_path)
        records.extend(data.to_dict(orient='records'))

    with ThreadPoolExecutor() as executor:
        responses = list(executor.map(process_record, records))

    return responses

def main():
    file_paths = ['NTX IV_11272025.xlsx', 'P1 IV_11272025.xlsx']
    responses = process_excel_files(file_paths)
    save_responses_to_excel(responses, 'output.xlsx')

if __name__ == "__main__":
    main()