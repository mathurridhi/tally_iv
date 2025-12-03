"""
X12 277 Claim Status Response Parser
Converts X12 277 format claims to custom formatted output
"""

from app.config.edl_config import X12Labels

class X12ClaimParser:
    def __init__(self, x12_content):
        self.x12_content = x12_content
        self.claims = []
        
    def parse(self):
        """Parse X12 277 content and extract claim information"""
        # Replace segment terminators and split
        content = self.x12_content.replace('~', '~\n')
        lines = content.split('\n')
        
        current_claim = None
        current_service = None
        current_patient = None
        hierarchical_levels = {}
        
        for line in lines:
            line = line.strip()
            if not line or line == '~':
                continue
                
            segments = line.replace('~', '').split('*')
            segment_id = segments[0]
            
            # Hierarchical Level
            if segment_id == 'HL':
                hl_id = segments[1] if len(segments) > 1 else ''
                parent_id = segments[2] if len(segments) > 2 else ''
                level_code = segments[3] if len(segments) > 3 else ''
                hierarchical_levels[hl_id] = {
                    'parent': parent_id,
                    'level': level_code,
                    'data': {}
                }
                
                # Level 22 = Patient/Claim level
                if level_code == '22':
                    if current_claim:
                        self.claims.append(current_claim)
                    current_claim = {
                        'claim_number': '',
                        'status_code': '',
                        'status_code2': '',
                        'total_charge': '0.00',
                        'paid_amount': '0.00',
                        'patient_name': '',
                        'patient_id': '',
                        'services': [],
                        'claim_adjustments': [],
                        'check_number': '',
                        'check_date': '',
                        'adjudication_date': ''
                    }
                    current_service = None

            # Name
            elif segment_id == 'NM1' and current_claim:
                entity_code = segments[1] if len(segments) > 1 else ''
                if entity_code == 'IL':  # Insured/Patient
                    last_name = segments[3] if len(segments) > 3 else ''
                    first_name = segments[4] if len(segments) > 4 else ''
                    patient_id = segments[9] if len(segments) > 9 else ''
                    current_claim['patient_name'] = f"{first_name} {last_name}".strip()
                    current_claim['patient_id'] = patient_id
            
            # Transaction Reference Number (Claim Number)
            elif segment_id == 'TRN' and current_claim:
                if len(segments) > 2:
                    current_claim['claim_number'] = segments[2]

            # Status Information - Check service level first, then claim level
            elif segment_id == 'STC' and current_service:
                # Service-level status - can have multiple status information fields
                # STC segment can have multiple status codes in segments[1], segments[10], segments[11], etc.
                status_positions = [1, 10, 11]  # Common positions for status information

                # Track which adjustments we've already added to avoid duplicates
                adjustment_keys = set()

                for pos in status_positions:
                    if len(segments) > pos and segments[pos]:
                        status_info = segments[pos].split(':')
                        status_code = status_info[0] if len(status_info) > 0 else ''
                        reason_code = status_info[1] if len(status_info) > 1 else ''
                        entity_code = status_info[2] if len(status_info) > 2 else '1P'

                        if status_code or reason_code:
                            # Create a unique key for this adjustment
                            adj_key = f"{status_code}:{reason_code}:{entity_code}"

                            # Skip if we've already added this adjustment
                            if adj_key in adjustment_keys:
                                continue

                            adjustment_keys.add(adj_key)

                            # Update service status with first occurrence
                            if not current_service['status_code']:
                                current_service['status_code'] = status_code
                            if not current_service['status_code2']:
                                current_service['status_code2'] = reason_code

                            # Store as service adjustment
                            adjustment = {
                                'status_code': status_code,
                                'reason_code': reason_code,
                                'amount': current_service['charge'],
                                'entity_code': entity_code if entity_code else '1P'
                            }
                            current_service['adjustments'].append(adjustment)

            elif segment_id == 'STC' and current_claim:
                # Claim-level status
                if len(segments) > 1:
                    status_info = segments[1].split(':')
                    current_claim['status_code'] = status_info[0] if len(status_info) > 0 else ''
                    current_claim['status_code2'] = status_info[1] if len(status_info) > 1 else ''

                # Adjudication date
                if len(segments) > 2 and segments[2]:
                    date_str = segments[2]
                    if len(date_str) == 8:
                        current_claim['adjudication_date'] = f"{date_str[4:6]}/{date_str[6:8]}/{date_str[0:4]}"

                # Amount
                if len(segments) > 4 and segments[4]:
                    current_claim['total_charge'] = segments[4]

                # Store as claim adjustment
                adjustment = {
                    'status_code': current_claim['status_code'],
                    'reason_code': current_claim['status_code2'],
                    'amount': current_claim['total_charge'],
                    'entity_code': ''
                }
                current_claim['claim_adjustments'].append(adjustment)
            
            # Reference Number
            elif segment_id == 'REF' and current_claim:
                ref_qualifier = segments[1] if len(segments) > 1 else ''
                if ref_qualifier == '1K':  # Payer Claim Control Number
                    current_claim['claim_number'] = segments[2] if len(segments) > 2 else ''
            
            # Date/Time Reference
            elif segment_id == 'DTP' and current_claim:
                date_qualifier = segments[1] if len(segments) > 1 else ''
                date_format = segments[2] if len(segments) > 2 else ''
                date_value = segments[3] if len(segments) > 3 else ''

                if date_qualifier == '472':  # Service Date
                    formatted_date = None

                    # Handle D8 format (single date: YYYYMMDD)
                    if date_format == 'D8' and len(date_value) == 8:
                        formatted_date = f"{date_value[4:6]}/{date_value[6:8]}/{date_value[0:4]}"

                    # Handle RD8 format (date range: YYYYMMDD-YYYYMMDD)
                    elif date_format == 'RD8' and '-' in date_value:
                        start_date = date_value.split('-')[0]
                        if len(start_date) == 8:
                            formatted_date = f"{start_date[4:6]}/{start_date[6:8]}/{start_date[0:4]}"

                    if formatted_date:
                        if current_service:
                            current_service['dos'] = formatted_date
                        else:
                            # Store for next service
                            if '_pending_dos' not in current_claim:
                                current_claim['_pending_dos'] = formatted_date
            
            # Service Line Information
            elif segment_id == 'SVC' and current_claim:
                current_service = {
                    'cpt_code': '',
                    'modifier': '',
                    'charge': segments[2] if len(segments) > 2 else '0.00',
                    'paid': segments[3] if len(segments) > 3 else '0.00',
                    'dos': '',
                    'adjustments': [],
                    'status_code': '',
                    'status_code2': ''
                }
                
                # Extract CPT code
                if len(segments) > 1:
                    cpt_parts = segments[1].split(':')
                    if len(cpt_parts) >= 3:
                        current_service['cpt_code'] = cpt_parts[1]
                        current_service['modifier'] = cpt_parts[2]
                    elif len(cpt_parts) == 2:
                        current_service['cpt_code'] = cpt_parts[1]
                    else:
                        current_service['cpt_code'] = segments[1]
                
                # Apply pending DOS if available
                if '_pending_dos' in current_claim:
                    current_service['dos'] = current_claim['_pending_dos']
                    del current_claim['_pending_dos']
                
                current_claim['services'].append(current_service)
        
        if current_claim:
            self.claims.append(current_claim)
            
        return self.claims
    
    def get_status_description(self, status_code):
        """Map status codes to descriptions"""
        return X12Labels.get_status_code(status_code)
    
    def get_reason_description(self, reason_code):
        """Map reason codes to descriptions"""

        return X12Labels.get_reason_code(reason_code)   
    
    def format_output(self):
        """Format claims into the desired output format"""
        output = []

        for idx, claim in enumerate(self.claims, 1):
            claim_num = f"Claim {idx}"
            total = claim['total_charge']
            status_code = claim['status_code']
            status_desc = self.get_status_description(status_code)

            # Format 1: Brief header with newline
            output.append(f"{claim_num} - ${total}: {status_code} ({status_desc})")

            # Add claim-level reason codes with newline
            if claim['status_code2']:
                reason_desc = self.get_reason_description(claim['status_code2'])
                output.append(f"{claim_num} -  {claim['status_code2']} - {reason_desc}")

            # Format 2: CPT codes summary with proper newlines
            if claim['services']:
                output.append(f"{claim_num}:")
                for svc_idx, service in enumerate(claim['services'], 1):
                    cpt_line = f"CPT {svc_idx}-{service['cpt_code']}"

                    adjustments_text = []
                    for adj in service['adjustments']:
                        reason_desc = self.get_reason_description(adj['reason_code'])
                        entity_desc = self.get_reason_description(adj.get('entity_code', '1P'))
                        adjustments_text.append(f" - {adj['reason_code']} - {reason_desc},  - {adj.get('entity_code', '1P')} - {entity_desc}")

                    output.append(cpt_line + ''.join(adjustments_text))

            # Format 3: Detailed claim information
            detail_parts = [
                f"{claim_num}: The Claim for ${total} adjudicated on {claim['adjudication_date'] or 'NA'}",
                f"Paid ${claim['paid_amount']}",
                f"EFT/Check# {claim['check_number'] or 'NA'}",
                f"EFT/Check Dt. {claim['check_date'] or 'NA'}",
                f"Claim# {claim['claim_number']}",
                f"{status_code} - {status_desc}"
            ]

            # Add claim-level reason
            if claim['status_code2']:
                reason_desc = self.get_reason_description(claim['status_code2'])
                detail_parts.append(f" {claim['status_code2']} - {reason_desc}")

            # Add service details
            for service in claim['services']:
                service_parts = [
                    f" DOS {service['dos'] or 'NA'}",
                    f"CPT {service['cpt_code']}" + (f"({service['modifier']})" if service['modifier'] else ""),
                    f"Paid ${service['paid']}"
                ]

                for adj in service['adjustments']:
                    reason_desc = self.get_reason_description(adj['reason_code'])
                    entity_desc = self.get_reason_description(adj.get('entity_code', '1P'))
                    service_parts.extend([
                        f"{adj['reason_code']} - {reason_desc}",
                        "",
                        f"{adj.get('entity_code', '1P')} - {entity_desc}",
                        "",
                        f"{service['status_code']} - {self.get_status_description(service['status_code'])}"
                    ])

                detail_parts.append("; ".join(service_parts) + ";")

            output.append("; ".join(detail_parts) + ";")
            output.append("")  # Blank line between claims

        return "\n".join(output)


# # Example usage
# if __name__ == "__main__":
#     # Your X12 277 content
#     x12_content = """
#         ISA*00*          *00*          *ZZ*STEDI          *01*117151744      *251031*0627*^*00501*884892184*0*P*:~GS*HN*STEDI*117151744*20251031*0127*884892184*X*005010X212~ST*277*884892184*005010X212~BHT*0010*08*01K8WF5CKPZKPHG29XR859PJDT*20251031*022702*DG~HL*1**20*1~NM1*PR*2*CHLIC*****PI*CIGNA~HL*2*1*21*1~NM1*41*2*CIGNA EVICORE HC COMM*****46*1578592309~HL*3*2*19*1~NM1*1P*2*CIGNA EVICORE HC COMM*****XX*1578592309~HL*4*3*22*0~NM1*IL*1*HECTOR*HECTOR****MI*U5105280302~TRN*2*01K8WF5CMTAQRVFGH66ZZSNH6V~STC*F2:542*20251031**1293.81*0*20250924~REF*1K*9432521099763~REF*EJ*TGMLKB50018813716~DTP*472*D8*20250725~SVC*HC:A4604*181.38*0****1~STC*F2:171*20250924~DTP*472*D8*20250725~SVC*HC:A7030*450*0****1~STC*F2:171*20250924~DTP*472*D8*20250725~SVC*HC:A7031*568.26*0****3~STC*F2:171*20250924~DTP*472*D8*20250725~SVC*HC:A7035*94.17*0****1~STC*F2:171*20250924~DTP*472*D8*20250725~SE*28*884892184~GE*1*884892184~IEA*1*884892184~
#     """
    
#     # Parse and format
#     parser = X12ClaimParser(x12_content)
#     parser.parse()
#     formatted_output = parser.format_output()
#     print(formatted_output)
    
#     # To use with a file:
#     # with open('your_x12_277_file.txt', 'r') as f:
#     #     x12_content = f.read()
#     # parser = X12ClaimParser(x12_content)
#     # parser.parse()
#     # print(parser.format_output())