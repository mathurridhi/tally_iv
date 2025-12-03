import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Tuple
from datetime import datetime
import re
import asyncio
import aiohttp
from sqlalchemy import text
from app.database import payer_obj, db_session
from .converter_service import X12ClaimParser


class ClaimService:
    def __init__(self):
        self.stedi_api_url = "https://healthcare.us.stedi.com/change/medicalnetwork/claimstatus/v2"
        self.max_concurrent_requests = 50  # Default to 20, can be changed

    async def _read_eclaims_patients(self) -> pd.DataFrame:
        """
        Private method to read the eclaims-patients.csv file from the docs folder,
        create a temporary table, and insert all CSV data into it using bulk insert.

        Returns:
            pd.DataFrame: DataFrame containing patient claims data

        Raises:
            FileNotFoundError: If the CSV file doesn't exist
            pd.errors.EmptyDataError: If the CSV file is empty
        """
        csv_path = "app/docs/claim-status.csv"

        # Read the CSV file
        df = pd.read_csv(csv_path)

        # Create temporary table and insert data
        try:
            # Drop temp table if it exists
            db_session.execute(text("DROP TABLE IF EXISTS temp_eclaims_patients"))

            # Create temp table with appropriate columns
            create_temp_table_sql = text("""
                CREATE TABLE temp_eclaims_patients (
                    payor_name VARCHAR(255),
                    ecs_id VARCHAR(50),
                    payor_address VARCHAR(255),
                    payor_city VARCHAR(100),
                    payor_state VARCHAR(10),
                    payor_zip VARCHAR(20),
                    npi VARCHAR(20),
                    last_name VARCHAR(100),
                    first_name VARCHAR(100),
                    patient_is_subscriber VARCHAR(10),
                    insured_id VARCHAR(100),
                    dob VARCHAR(20),
                    gender VARCHAR(10),
                    to_dos VARCHAR(20),
                    from_dos VARCHAR(20),
                    claim_total VARCHAR(50),
                    invoice_number VARCHAR(50),
                    customer_id VARCHAR(50),
                    last_denial_posted VARCHAR(50),
                    last_remark_posted VARCHAR(50),
                    co VARCHAR(50),
                    rg VARCHAR(50),
                    ds VARCHAR(50),
                    br VARCHAR(50),
                    invoice_balance VARCHAR(50),
                    claim_status VARCHAR(250),
                    claim_steps VARCHAR(500)
                )
            """)
            db_session.execute(create_temp_table_sql)

            # Prepare bulk insert data
            print(f"Preparing to insert {len(df)} records...")
            bulk_data = []
            for _, row in df.iterrows():
                bulk_data.append({
                    'payor_name': str(row.get('"Payor Name"', '')),
                    'ecs_id': str(row.get('ECS ID', '')),
                    'payor_address': str(row.get('Payor Address', '')),
                    'payor_city': str(row.get('Payor City', '')),
                    'payor_state': str(row.get('Payor State', '')),
                    'payor_zip': str(row.get('Payor Zip', '')),
                    'npi': str(row.get('NPI', '')),
                    'last_name': str(row.get('Last Name', '')),
                    'first_name': str(row.get('First Name', '')),
                    'patient_is_subscriber': str(row.get('Patient is same as Subscriber Y/N', '')),
                    'insured_id': str(row.get('"Insured ID"', '')),
                    'dob': str(row.get('DOB', '')),
                    'gender': str(row.get('Gender', '')),
                    'to_dos': str(row.get('To Dos', '')),
                    'from_dos': str(row.get('From Dos', '')),
                    'claim_total': str(row.get(' Claim Total ', '')),
                    'invoice_number': str(row.get('Invoice Number', '')),
                    'customer_id': str(row.get('Customer ID', '')),
                    'last_denial_posted': str(row.get('Last Denial Posted', '')),
                    'last_remark_posted': str(row.get('Last Remark Posted ', '')),
                    'co': str(row.get('CO', '')),
                    'rg': str(row.get('RG', '')),
                    'ds': str(row.get('DS', '')),
                    'br': str(row.get('BR', '')),
                    'invoice_balance': str(row.get(' Invoice Balance ', ''))
                })

            # Bulk insert in batches for optimal performance
            batch_size = 1000
            total_batches = (len(bulk_data) + batch_size - 1) // batch_size

            insert_sql = text("""
                INSERT INTO temp_eclaims_patients (
                    payor_name, ecs_id, payor_address, payor_city, payor_state,
                    payor_zip, npi, last_name, first_name, patient_is_subscriber,
                    insured_id, dob, gender, to_dos, from_dos, claim_total,
                    invoice_number, customer_id, last_denial_posted, last_remark_posted,
                    co, rg, ds, br, invoice_balance
                ) VALUES (
                    :payor_name, :ecs_id, :payor_address, :payor_city, :payor_state,
                    :payor_zip, :npi, :last_name, :first_name, :patient_is_subscriber,
                    :insured_id, :dob, :gender, :to_dos, :from_dos, :claim_total,
                    :invoice_number, :customer_id, :last_denial_posted, :last_remark_posted,
                    :co, :rg, :ds, :br, :invoice_balance
                )
            """)

            for i in range(0, len(bulk_data), batch_size):
                batch = bulk_data[i:i + batch_size]
                db_session.execute(insert_sql, batch)
                print(f"Inserted batch {(i // batch_size) + 1}/{total_batches}")

            db_session.commit()
            print(f"Successfully inserted {len(df)} records into temp_eclaims_patients table")

        except Exception as e:
            db_session.rollback()
            print(f"Error creating temp table and inserting data: {str(e)}")
            raise

        # Return the DataFrame
        return df.iloc[[11]]

    async def _read_stedi_payers(self) -> pd.DataFrame:
        """
        Private method to read the stedi_payers data from the database.
        Results are cached using LRU cache in the payer_obj.

        Returns:
            pd.DataFrame: DataFrame containing stedi payers data
        """
        # Get payers from database (cached)
        payers_data = payer_obj.get_all_payers()

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

    async def get_trading_partner_id(self, payor_name: str) -> str:
        """
        Get the trading partner service ID (Primary Payer ID) for a given payer name.
        Uses multiple matching strategies: exact match, partial match, keyword match, and regex.
        Searches only in the DisplayName column.
        Only returns payers where ClaimStatusInquiry is True.

        Args:
            payor_name: The name of the payer to search for

        Returns:
            str: The Primary Payer ID (StediId) or empty string if not found

        Raises:
            FileNotFoundError: If the stedi_payers CSV file doesn't exist
        """
        df = await self._read_stedi_payers()

        # Filter to only include payers with ClaimStatusInquiry = True
        df = df[df["ClaimStatusInquiry"].astype(str).str.lower() == "true"].copy()
        if df.empty:
            return ""

        # Clean and normalize the payor name
        payor_name_clean = str(payor_name).strip()
        payor_name_normalized = self._normalize_name(payor_name_clean)
        payor_keywords = self._extract_keywords(payor_name_clean)
        payor_upper = payor_name_clean.upper()

        # Pre-process dataframe
        df["DisplayName_normalized"] = df["DisplayName"].apply(self._normalize_name)
        df["DisplayName_upper"] = df["DisplayName"].str.strip().str.upper()

        # Strategy 1: Exact match in DisplayName (case-insensitive)
        mask = df["DisplayName_upper"] == payor_upper
        matching_rows = df[mask]
        if not matching_rows.empty:
            return str(matching_rows.iloc[0]["PrimaryPayerId"])

        # Strategy 2: Normalized exact match
        mask = df["DisplayName_normalized"] == payor_name_normalized
        matching_rows = df[mask]
        if not matching_rows.empty:
            return str(matching_rows.iloc[0]["PrimaryPayerId"])

        # Strategy 3: Partial match
        for _, row in df.iterrows():
            display_name_upper = row["DisplayName_upper"]
            if display_name_upper in payor_upper or payor_upper in display_name_upper:
                return str(row["PrimaryPayerId"])

        # Strategy 4: Keyword matching
        best_match_score = 0
        best_match_row = None

        for _, row in df.iterrows():
            display_keywords = self._extract_keywords(row["DisplayName"])
            common_keywords = payor_keywords.intersection(display_keywords)
            if common_keywords:
                score = len(common_keywords) / max(
                    len(payor_keywords), len(display_keywords)
                )
                if score > best_match_score and score >= 0.5:
                    best_match_score = score
                    best_match_row = row

        if best_match_row is not None:
            return str(best_match_row["PrimaryPayerId"])

        # Strategy 5: Regex search
        pattern = re.escape(payor_name_normalized)
        mask = df["DisplayName_normalized"].apply(
            lambda x: bool(re.search(pattern, x))
        )
        matching_rows = df[mask]
        if not matching_rows.empty:
            return str(matching_rows.iloc[0]["PrimaryPayerId"])

        # Strategy 6: Multi-keyword search (at least 2 common keywords)
        for _, row in df.iterrows():
            display_keywords = self._extract_keywords(row["DisplayName"])
            common_keywords = payor_keywords.intersection(display_keywords)
            if len(common_keywords) >= 2:
                return str(row["PrimaryPayerId"])

        # Return empty string if not found
        print(f"No match found for payor name: {payor_name_clean}")
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
        from_dos = _parse_date(row.get("From Dos", ""))
        to_dos = _parse_date(row.get("To Dos", ""))
        dob = _parse_date(row.get("DOB", ""))

        # Get NPI
        npi = str(row.get("NPI", "")).strip()

        # Get member/insured ID
        member_id = str(row.get('"Insured ID"', "")).strip()
        if not member_id:
            member_id = str(row.get("Insured ID", "")).strip()

        # Get payor name for organization
        payor_name = str(row.get('"Payor Name"', "")).strip()
        if not payor_name:
            payor_name = str(row.get("Payor Name", "")).strip()

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

    async def generate_payloads_from_csv(self) -> List[Dict[str, Any]]:
        """
        Read eclaims-patients.csv and generate JSON payloads for all rows.
        Automatically looks up trading partner service ID from stedi_payers based on payer name.

        Returns:
            List of dictionaries containing formatted claim payloads
        """
        df = await self._read_eclaims_patients()
        payloads = []

        for _, row in df.iterrows():
            # Get payor name from the row
            payor_name = str(row.get('"Payor Name"', "")).strip()
            if not payor_name:
                payor_name = str(row.get("Payor Name", "")).strip()

            # Look up trading partner service ID
            # trading_partner_service_id = await self.get_trading_partner_id(payor_name)

            trading_partner_service_id = row.get("ECS ID", "").strip() 

            payload = self._create_claim_payload_from_csv(
                row, trading_partner_service_id
            )
            payloads.append(payload)

        # print(f"Generated {len(payloads)} payloads")
        return payloads

    def _update_claim_status_in_temp_table(
        self, first_name: str, last_name: str, dob: str, claim_status: str
    ) -> None:
        """
        Update the claim_status in temp_eclaims_patients table based on first_name, last_name, and dob.

        Args:
            first_name: Patient's first name
            last_name: Patient's last name
            dob: Patient's date of birth (in YYYYMMDD format)
            claim_status: The claim status to update
        """
        try:
            # Convert DOB from YYYYMMDD to M/D/YYYY format to match CSV format
            if dob and len(dob) == 8:
                year = dob[:4]
                month = str(int(dob[4:6]))  # Remove leading zero
                day = str(int(dob[6:8]))    # Remove leading zero
                dob_formatted = f"{month}/{day}/{year}"
            else:
                dob_formatted = dob

            update_sql = text("""
                UPDATE temp_eclaims_patients
                SET claim_status = :claim_status
                WHERE UPPER(TRIM(first_name)) = UPPER(TRIM(:first_name))
                  AND UPPER(TRIM(last_name)) = UPPER(TRIM(:last_name))
                  AND dob = :dob
            """)

            result = db_session.execute(update_sql, {
                'claim_status': claim_status,
                'first_name': first_name,
                'last_name': last_name,
                'dob': dob_formatted
            })

            db_session.commit()

            if result.rowcount == 0:
                print(f"Warning: No record found to update for {first_name} {last_name} DOB: {dob_formatted}")
            else:
                print(f"Updated claim_status for {first_name} {last_name}")

        except Exception as e:
            db_session.rollback()
            print(f"Error updating claim_status for {first_name} {last_name}: {str(e)}")

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
        max_concurrent: int = None
    ) -> List[Dict[str, Any]]:
        """
        Submit claim status requests to Stedi API in parallel batches.

        Args:
            payloads: List of claim payloads to submit
            api_key: Stedi API key for authentication
            max_concurrent: Maximum number of concurrent requests (default: 20)

        Returns:
            List of dictionaries containing request and response data
        """
        if max_concurrent is None:
            max_concurrent = self.max_concurrent_requests

        headers = {
            "Authorization": f"EDarlwg.B8pZBRCJWhr9JwGNkhoxuLAj",
            "Content-Type": "application/json"
        }

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

            # Process results
            for result in completed_results:
                if isinstance(result, Exception):
                    results.append({
                        "success": False,
                        "error": str(result)
                    })
                else:
                    index, payload, response = result
                    if not response.get("success", False):
                        failed_requests.append({
                            "request_payload": payload,
                            "error": response['data'].get("message", "Unknown error"),
                            "status_code": response.get("status_code", None)
                        })
                        continue
                    parser = X12ClaimParser(response['data'].get("x12", {}))
                    parser.parse()
                    # print(parser.format_output())
                    claim_status_response = parser.format_output()

                    # Extract subscriber info from payload for database update
                    subscriber = payload.get("subscriber", {})
                    first_name = subscriber.get("firstName", "")
                    last_name = subscriber.get("lastName", "")
                    dob = subscriber.get("dateOfBirth", "")

                    # Update the temp table with claim status
                    self._update_claim_status_in_temp_table(
                        first_name=first_name,
                        last_name=last_name,
                        dob=dob,
                        claim_status=str(claim_status_response)
                    )

                    results.append({
                        "request_payload": payload,
                        "claim_status": str(claim_status_response)
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
        return results, failed_requests

    async def process_claims_from_csv(
        self,
        max_concurrent: int = None
    ) -> List[Dict[str, Any]]:
        """
        End-to-end processing: Read CSV, generate payloads, and submit to Stedi API.

        Args:
            api_key: Stedi API key for authentication
            max_concurrent: Maximum number of concurrent requests (default: 20)

        Returns:
            List of dictionaries containing request and response data
        """
        # Generate payloads from CSV
        payloads = await self.generate_payloads_from_csv()

        if not payloads:
            print("No payloads generated. Check if CSV file has valid data.")
            return []

        # Submit requests to API
        print("stating to submit batcch claim status requests..." + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        results, failed_results = await self.submit_claim_status_requests(payloads, max_concurrent)
        print("completed submitting batcch claim status requests..." + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
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


claim_service = ClaimService()
