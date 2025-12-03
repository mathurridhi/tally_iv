# Stedi Excel Processor

This project processes records from Excel files concurrently, builds payloads, sends requests to the Stedi API, and writes responses to an output Excel file.

## Project Structure

```
stedi-excel-processor
├── src
│   ├── main.py               # Entry point of the application
│   ├── processor.py          # Logic for processing Excel records
│   ├── excel_io.py           # Functions for reading/writing Excel files
│   ├── stedi_client.py       # Client for interacting with the Stedi API
│   ├── workers.py            # Worker functions for concurrent processing
│   ├── config.py             # Configuration management
│   ├── models
│   │   └── __init__.py       # Data models
│   ├── schemas
│   │   └── __init__.py       # Payload validation schemas
│   └── utils
│       └── __init__.py       # Utility functions
├── tests
│   ├── test_processor.py      # Unit tests for Processor class
│   └── test_stedi_client.py   # Unit tests for StediClient class
├── requirements.txt           # Project dependencies
├── pyproject.toml            # Project metadata and build requirements
├── .env.example               # Example environment variables
└── README.md                  # Project documentation
```

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd stedi-excel-processor
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Set up your environment variables by copying `.env.example` to `.env` and filling in the necessary values.

## Usage

To run the application, execute the following command:
```
python src/main.py
```

This will start processing the specified Excel files concurrently.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License

This project is licensed under the MIT License. See the LICENSE file for details.