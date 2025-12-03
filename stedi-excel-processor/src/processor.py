from excel_io import read_excel
from payer_lookup import payer_lookup
from utils.json_flattener import flatten_response_list
import os
import asyncio
import aiohttp
from typing import List, Dict, Any, Tuple


class Processor:
    def __init__(self, excel_file, output_file, api_key, api_url, max_concurrent=50):
        self.excel_file = excel_file
        self.output_file = output_file
        self.api_key = api_key
        self.api_url = api_url
        self.max_concurrent = max_concurrent

    async def process_file(self):
        """Process a single Excel file and save responses"""
        print(f"Processing {os.path.basename(self.excel_file)}...")

        records = self.load_records()
        print(f"Loaded {len(records)} records")

        payloads = self.build_payloads(records)
        print(f"Built {len(payloads)} payloads")

        responses = await self.send_requests_concurrent(payloads)
        print(f"Received {len(responses)} responses")

        self.write_responses(responses)
        print(f"Results saved to {self.output_file}")

    def load_records(self):
        """Load records from the Excel file"""
        df = read_excel(self.excel_file)
        return df.to_dict('records')

    def build_payloads(self, records):
        payloads = []
        for record in records:
            payload = self.create_payload(record)
            payloads.append(payload)
        return payloads

    def create_payload(self, record):
        """
        Create eligibility inquiry payload from Excel record.
        Looks up tradingPartnerServiceId from database using payor name and ECS ID.
        """
        import pandas as pd

        # Helper function to safely convert values
        def safe_value(value):
            if pd.isna(value):
                return None
            if isinstance(value, pd.Timestamp):
                return value.strftime('%Y%m%d')
            if isinstance(value, float):
                return str(int(value)) if value == int(value) else str(value)
            return str(value).strip()

        # Get payor information (handle different column names)
        payor_name = safe_value(record.get('Payor Name') or record.get('payer name', ''))
        ecs_id = safe_value(record.get('ECS ID') or record.get('payer id', ''))

        # Lookup trading partner service ID
        trading_partner_id = ''
        if ecs_id and len(str(ecs_id)) >= 5:
            trading_partner_id = str(ecs_id)
        else:
            # Lookup from database using payor name and ECS ID
            trading_partner_id = payer_lookup.get_trading_partner_id(payor_name, ecs_id)

        # Build eligibility inquiry payload
        payload = {
            "tradingPartnerServiceId": trading_partner_id or "",
            "externalPatientId": safe_value(record.get('externalPatientId', '')),
            "subscriber": {
                "memberId": safe_value(record.get('Member ID', '')),
                "firstName": safe_value(record.get('First Name') or record.get('Sub First Name', '')),
                "lastName": safe_value(record.get('Last Name') or record.get('Sub Last Name', '')),
                "dateOfBirth": safe_value(record.get('Sub DOB', ''))
            },
            "provider": {
                "organizationName": safe_value(record.get('organizationName', '')),
                "npi": safe_value(record.get('Org npi', ''))
            }
        }

        # Add service type codes if available
        service_types = safe_value(record.get('Service Type codes', ''))
        if service_types:
            payload["serviceTypeCodes"] = [service_types]

        return payload

    async def _make_request(
        self, session: aiohttp.ClientSession, payload: Dict[str, Any], index: int
    ) -> Tuple[int, Dict[str, Any]]:
        """Make a single async API request"""
        try:
            async with session.post(self.api_url, json=payload) as response:
                response_data = await response.json()
                return (index, {
                    "status_code": response.status,
                    "success": response.status == 200,
                    "response": response_data
                })
        except aiohttp.ClientError as e:
            return (index, {
                "status_code": 0,
                "success": False,
                "error": str(e)
            })
        except Exception as e:
            return (index, {
                "status_code": 0,
                "success": False,
                "error": f"Unexpected error: {str(e)}"
            })

    async def send_requests_concurrent(self, payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Send API requests concurrently with rate limiting"""
        headers = {
            "Authorization": self.api_key,
            "Content-Type": "application/json"
        }

        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def bounded_request(session, payload, index):
            async with semaphore:
                return await self._make_request(session, payload, index)

        # Create aiohttp session with timeout
        timeout = aiohttp.ClientTimeout(total=300)  # 5 minutes timeout
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            # Create tasks for all requests
            tasks = [
                bounded_request(session, payload, i)
                for i, payload in enumerate(payloads)
            ]

            # Execute all tasks and gather results
            print(f"Submitting {len(payloads)} requests with max {self.max_concurrent} concurrent connections...")
            completed_results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process results and maintain order
            responses = [None] * len(payloads)
            for result in completed_results:
                if isinstance(result, Exception):
                    print(f"Request resulted in exception: {str(result)}")
                else:
                    index, response = result
                    responses[index] = response

            # Replace any None values with error responses
            for i, response in enumerate(responses):
                if response is None:
                    responses[i] = {
                        "status_code": 0,
                        "success": False,
                        "error": "Request failed"
                    }

            return responses

    def write_responses(self, responses):
        """Write flattened responses to CSV file"""
        # Flatten all JSON responses into columns
        df = flatten_response_list(responses)

        # Change extension to CSV
        output_file = self.output_file.replace('.xlsx', '.csv')

        # Write to CSV (no column limit)
        df.to_csv(output_file, index=False)
        print(f"Written {len(df)} rows with {len(df.columns)} columns to {output_file}")