from functools import lru_cache
import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Tuple
from datetime import datetime
import re
import asyncio
import aiohttp
from app.database import payer_obj
from .converter_service import X12ClaimParser


class ClaimService:
    def __init__(self):
        self.stedi_api_url = "https://healthcare.us.stedi.com/change/medicalnetwork/claimstatus/v2"
        self.max_concurrent_requests = 50  # Default to 20, can be changed

    async def _read_eclaims_patients(self) -> pd.DataFrame:
        """
        Private method to read the eclaims-patients.csv file from the docs folder.

        Returns:
            pd.DataFrame: DataFrame containing patient claims data

        Raises:
            FileNotFoundError: If the CSV file doesn't exist
            pd.errors.EmptyDataError: If the CSV file is empty
        """
        # Get the project root directory (where app folder is located)
        # current_dir = Path(__file__).resolve().parent
        # project_root = current_dir.parent.parent.parent
        csv_path ="app/docs/baca.csv"

        # if not csv_path.exists():
        #     raise FileNotFoundError(f"CSV file not found at: {csv_path}")

        # Read and return the CSV file
        df = pd.read_csv(csv_path)
        return df.iloc[0:2]

    async def _read_stedi_payers(self, payor_name, payer_id):
        """
        Private method to read the stedi_payers data from the database.
        Results are cached using LRU cache in the payer_obj.

        Returns:
            pd.DataFrame: DataFrame containing stedi payers data
        """
        # Get payers from database (cached)
        payers_data = payer_obj.get_all_payers(payer_id)

        # Convert to DataFrame for compatibility with existing code
        df = pd.DataFrame(payers_data)
        return df

    def _normalize_name(self, name: str) -> str:
        """
        Normalize a name by removing special characters, extra spaces, and converting to uppercase.

        Args:
            name: The name to normalize

        Returns:
            str: Normalized name
        """
        # Remove special characters and extra spaces
        normalized = re.sub(r"[^\w\s]", " ", str(name))
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip().upper()

    def _extract_keywords(self, name: str) -> set:
        """
        Extract meaningful keywords from a name (excluding common words).

        Args:
            name: The name to extract keywords from

        Returns:
            set: Set of keywords
        """
        # Common words to exclude
        common_words = {
            "OF",
            "THE",
            "AND",
            "A",
            "AN",
            "IN",
            "FOR",
            "ON",
            "AT",
            "TO",
            "BY",
        }

        normalized = self._normalize_name(name)
        words = normalized.split()

        # Keep words that are at least 2 characters and not common words
        keywords = {w for w in words if len(w) >= 2 and w not in common_words}
        return keywords

    async def get_trading_partner_id(self, payor_name: str, payer_id) -> str:
        """
        Get the trading partner service ID (Primary Payer ID) for a given payer name.
        Uses multiple matching strategies: exact match, partial match, keyword match, and regex.
        Searches in both DisplayName and Aliases columns.
        Only returns payers where ClaimStatusInquiry is True.

        Args:
            payor_name: The name of the payer to search for
            payer_id: The payer ID to filter by

        Returns:
            str: The Primary Payer ID (PrimaryPayerId) or empty string if not found

        Raises:
            FileNotFoundError: If the stedi_payers CSV file doesn't exist
        """
        df = await self._read_stedi_payers(payor_name, payer_id)

        if df.empty:
            return ""

        # Normalize the input payer name
        normalized_payor = self._normalize_name(payor_name)
        payor_keywords = self._extract_keywords(payor_name)

        # Strategy 1: Exact match in DisplayName or Aliases
        for col in ['DisplayName', 'Aliases']:
            if col in df.columns:
                exact_matches = df[df[col].apply(
                    lambda x: self._normalize_name(str(x)) == normalized_payor if pd.notna(x) else False
                )]
                if not exact_matches.empty:
                    return str(exact_matches.iloc[0]['PrimaryPayerId'])

        # Strategy 2: Partial match - check if payer_name is contained in DisplayName or Aliases
        for col in ['DisplayName', 'Aliases']:
            if col in df.columns:
                partial_matches = df[df[col].apply(
                    lambda x: normalized_payor in self._normalize_name(str(x)) if pd.notna(x) else False
                )]
                if not partial_matches.empty:
                    return str(partial_matches.iloc[0]['PrimaryPayerId'])

        # Strategy 3: Keyword match - find rows with most keyword matches
        def calculate_keyword_score(value, keywords):
            if pd.isna(value):
                return 0
            value_keywords = self._extract_keywords(str(value))
            return len(keywords & value_keywords)

        # Calculate scores for both DisplayName and Aliases
        df['match_score'] = 0
        for col in ['DisplayName', 'Aliases']:
            if col in df.columns:
                df['match_score'] += df[col].apply(
                    lambda x: calculate_keyword_score(x, payor_keywords)
                )

        # Get rows with highest score
        best_matches = df[df['match_score'] == df['match_score'].max()]
        if not best_matches.empty and best_matches.iloc[0]['match_score'] > 0:
            return str(best_matches.iloc[0]['PrimaryPayerId'])

        # No match found
        return ""




    def _create_claim_payload_from_csv(
        self, row: pd.Series, trading_partner_service_id: str
    ) -> Dict[str, Any]:
        """
        Private method to create a JSON payload from a CSV row in the specified format.

        Args:
            row: A pandas Series representing a row from eclaims-patients.csv
            trading_partner_service_id: The trading partner service ID

        Returns:
            Dict containing the formatted claim payload
        """

        # Parse date format from MM/DD/YYYY to YYYYMMDD
        def _parse_date(date_str: str) -> str:
            try:
                if pd.notna(date_str):
                    dt = pd.to_datetime(date_str)
                    return dt.strftime("%Y%m%d")
            except:
                pass
            return ""

        # Extract names
        last_name = str(row.get("Last Name", "")).strip()
        first_name = str(row.get("First Name", "")).strip()

        # Get dates
        from_dos = _parse_date(row.get("From DOS", ""))
        to_dos = _parse_date(row.get("To DOS", ""))
        dob = _parse_date(row.get("DOB", ""))

        # Get NPI
        npi_value = row.get("NPI", "")
        if pd.notna(npi_value) and npi_value != "":
            npi = str(int(npi_value)).strip()
        else:
            npi = ""

        # Get member/insured ID
        member_id = str(row.get('"Insured ID"', "")).strip()
        if not member_id:
            member_id = str(row.get("Insured ID", "")).strip()

        # Get payor name for organization
        payor_name = str(row.get('"Payor Name"', "")).strip()
        if not payor_name:
            payor_name = str(row.get("Payor Name", "")).strip()

        # provider_type = str(row.get("Provider Type", "BillingProvider")).strip()

        # Build the payload
        payload = {
            "encounter": {
                "beginningDateOfService": from_dos,
                "endDateOfService": to_dos,
            },
            "providers": [
                {
                    "npi": npi,
                    "organizationName": payor_name,
                    "providerType": "BillingProvider",
                }
            ],
            "subscriber": {
                "dateOfBirth": dob,
                "firstName": first_name,
                "lastName": last_name,
                "memberId": member_id,
            },
            "tradingPartnerServiceId": trading_partner_service_id,
        }

        return payload

    async def generate_payloads_from_csv(self) -> Tuple[List[Dict[str, Any]], pd.DataFrame]:
        """
        Read eclaims-patients.csv and generate JSON payloads for all rows.
        Automatically looks up trading partner service ID from stedi_payers based on payer name.

        Returns:
            Tuple of (List of dictionaries containing formatted claim payloads, DataFrame with claim_status column)
        """
        df = await self._read_eclaims_patients()

        # Add claim_status column with empty default values
        df['claim_status'] = ''
        df['denial_category'] = ''
        df['denial_code'] = ''
        df['denial_reason'] = ''
        df['final_steps'] = ''
        payloads = []

        for _, row in df.iterrows():
            # Get payor name from the row
            payor_name = str(row.get('"payer name"', "")).strip()
            if not payor_name:
                payor_name = str(row.get("payer name", "")).strip()

            # Look up trading partner service ID
            # trading_partner_service_id = await self.get_trading_partner_id(payor_name)

            ecs_id = row.get("ECS PAYOR ID", "")
            # Handle NaN, None, or empty values
            if pd.isna(ecs_id):
                trading_partner_service_id = ""
            else:
                trading_partner_service_id = str(ecs_id).strip()

            if len(trading_partner_service_id) < 5:

                trading_partner_service_id = await self.get_trading_partner_id(payor_name, trading_partner_service_id)
                df.loc[_, 'ECS PAYOR ID'] = trading_partner_service_id

            payload = self._create_claim_payload_from_csv(
                row, trading_partner_service_id
            )
            payloads.append(payload)

        # print(f"Generated {len(payloads)} payloads")
        return payloads, df

    async def _make_claim_status_request(
        self, session: aiohttp.ClientSession, payload: Dict[str, Any], index: int
    ) -> Tuple[int, Dict[str, Any], Any]:
        """
        Private method to make a single claim status request.

        Args:
            session: aiohttp client session
            payload: The claim payload to send
            index: Index of the request for tracking

        Returns:
            Tuple of (index, payload, response_data)
        """
        try:
            async with session.post(self.stedi_api_url, json=payload) as response:
                response_data = await response.json()
                return (index, payload, {
                    "status_code": response.status,
                    "success": response.status == 200,
                    "data": response_data
                })
        except aiohttp.ClientError as e:
            return (index, payload, {
                "status_code": None,
                "success": False,
                "error": str(e)
            })
        except Exception as e:
            return (index, payload, {
                "status_code": None,
                "success": False,
                "error": f"Unexpected error: {str(e)}"
            })

    async def submit_claim_status_requests(
        self,
        payloads: List[Dict[str, Any]],
        df: pd.DataFrame,
        max_concurrent: int = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], pd.DataFrame]:
        """
        Submit claim status requests to Stedi API in parallel batches.

        Args:
            payloads: List of claim payloads to submit
            df: DataFrame to update with claim_status values
            api_key: Stedi API key for authentication
            max_concurrent: Maximum number of concurrent requests (default: 20)

        Returns:
            Tuple of (results list, failed_requests list, updated DataFrame)
        """
        if max_concurrent is None:
            max_concurrent = self.max_concurrent_requests

        headers = {
            "Authorization": f"EDarlwg.B8pZBRCJWhr9JwGNkhoxuLAj",
            "Content-Type": "application/json"
        }
        denial_info = self.get_denial_reasons()
        results = []
        failed_requests = []

        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(max_concurrent)

        async def bounded_request(session, payload, index):
            async with semaphore:
                return await self._make_claim_status_request(session, payload, index)

        # Create aiohttp session with timeout
        timeout = aiohttp.ClientTimeout(total=300)  # 5 minutes timeout
        async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
            # Create tasks for all requests
            tasks = [
                bounded_request(session, payload, i)
                for i, payload in enumerate(payloads)
            ]

            # Execute all tasks and gather results
            print(f"Submitting {len(payloads)} requests with max {max_concurrent} concurrent connections...")
            completed_results = await asyncio.gather(*tasks, return_exceptions=True)


            denial_data = {}

            # Process results
            for result in completed_results:

                if isinstance(result, Exception):
                    print(f"Request resulted in exception: {str(result)}")
                    failed_requests.append({
                        "error": str(result),
                        "status_code": None
                    })
                else:
                    index, payload, response = result
                    print(payload)
                    print("---------------------------------------------")
                    # Only process requests with 200 status code
                    if response.get("status_code") != 200:
                        print(f"Request failed with status code: {response.get('status_code')}")
                        failed_requests.append({
                            "request_payload": payload,
                            "error": response.get('data', {}).get("message", response.get("error", "Unknown error")),
                            "status_code": response.get("status_code", None)
                        })
                        df.iloc[index, df.columns.get_loc('claim_status')] = "Payer not supported"
                        continue

                    # Check if x12 data exists
                    x12_data = response.get('data', {}).get("x12", {})
                    if not x12_data:
                        print(f"No x12 data in response for index {index}")
                        failed_requests.append({
                            "request_payload": payload,
                            "error": "No x12 data in response",
                            "status_code": response.get("status_code", None)
                        })
                        continue

                    # Process successful response with x12 data
                    parser = X12ClaimParser(x12_data)
                    parser.parse()
                    claim_status_value = str(parser.format_output())


                    claims_data = response['data'].get("claims", [])
                    if claims_data and len(claims_data) > 0:
                        data = claims_data[0].get("claimStatus", {})
                        denial_code = data.get('statusCode', '')

                        denial_code_str = str(denial_code).strip()

                        # Filter for matching denial code
                        matching_row = denial_info[denial_info['DenialCode'].astype(str).str.strip() == denial_code_str]

                        if not matching_row.empty:
                            # Return the first matching row as a dictionary
                            denial_data = matching_row.iloc[0].to_dict()

                        df.iloc[index, df.columns.get_loc('denial_code')] = denial_code
                        df.iloc[index, df.columns.get_loc('denial_category')] = denial_data.get('DenialCategory', '')
                        df.iloc[index, df.columns.get_loc('denial_reason')] = denial_data.get('DenialReason', '')
                        df.iloc[index, df.columns.get_loc('final_steps')] = denial_data.get('FinalSteps', '')

                    # Update DataFrame with claim status at the correct row
                    df.iloc[index, df.columns.get_loc('claim_status')] = claim_status_value

                    results.append({
                        "request_payload": payload,
                        "claim_status": claim_status_value
                    })
                    # index, payload, response = result
                    # results.append({
                    #     # "index": index,
                    #     "request_payload": payload,
                    #     "response": response
                    #     # "trading_partner_id": payload.get("tradingPartnerServiceId", ""),
                    #     # "subscriber": payload.get("subscriber", {})
                    # })

        # Print summary
        # successful = sum(1 for r in results if r.get("response", {}).get("success", False))

        # failed = len(results) - successful
        # print(f"Completed: {successful} successful, {failed} failed")
        # print(results)
        return results, failed_requests, df

    async def process_claims_from_csv(
        self,
        max_concurrent: int = None
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], pd.DataFrame]:
        """
        End-to-end processing: Read CSV, generate payloads, and submit to Stedi API.

        Args:
            api_key: Stedi API key for authentication
            max_concurrent: Maximum number of concurrent requests (default: 20)

        Returns:
            Tuple of (results list, failed_results list, updated DataFrame with claim_status)
        """
        # Generate payloads from CSV
        payloads, df = await self.generate_payloads_from_csv()

        if not payloads:
            print("No payloads generated. Check if CSV file has valid data.")
            return [], [], df

        # Submit requests to API
        print("stating to submit batch claim status requests..." + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        results, failed_results, df = await self.submit_claim_status_requests(payloads, df, max_concurrent)
        print("completed submitting batch claim status requests..." + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        # print(df)
        df.to_csv('updated_claims.csv', index=False)
        return results, failed_results

    async def get_claim_status(self, claim_id: str):
        """
        Get the status of a claim by claim ID.

        Args:
            claim_id: The claim identifier

        Returns:
            Dict containing claim status information
        """
        # Placeholder for actual claim status retrieval logic
        return {"claim_id": claim_id, "status": "Processed"}
    
    @lru_cache(maxsize=128)
    def get_denial_reasons(self):
        """
        Get the denial reason details for a given denial code from Code_Mapping.csv.

        Args:
            denial_code: The denial code to look up

        Returns:
            Dict containing the matching row data or empty dict if not found
        """
        csv_path = "app/docs/Code_Mapping.csv"

        try:
            df = pd.read_csv(csv_path)
            # Convert denial_code to string for comparison
            return df

        except FileNotFoundError:
            print(f"CSV file not found at: {csv_path}")
            return {}
        except Exception as e:
            print(f"Error reading denial reasons: {str(e)}")
            return {}


claim_service = ClaimService()
