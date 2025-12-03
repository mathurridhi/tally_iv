from processor import Processor
from config import Config
import os
import asyncio


async def main():
    config = Config()

    # Process each Excel file separately
    excel_files = [
        ("D:\\test\\Apex_IV.xlsx", "Apex_IV_output.xlsx")
    ]

    for excel_file, output_file in excel_files:
        if not os.path.exists(excel_file):
            print(f"Warning: {excel_file} not found, skipping...")
            continue

        print(f"\n{'='*60}")
        print(f"Processing: {os.path.basename(excel_file)}")
        print(f"{'='*60}")

        # Create processor for this file
        processor = Processor(
            excel_file=excel_file,
            output_file=output_file,
            api_key=config.STEDI_API_KEY,
            api_url=config.STEDI_API_URL,
            max_concurrent=config.MAX_WORKERS
        )

        # Process the file asynchronously
        await processor.process_file()

    print(f"\n{'='*60}")
    print("All files processed successfully!")
    print(f"{'='*60}")


if __name__ == "__main__":
    asyncio.run(main())