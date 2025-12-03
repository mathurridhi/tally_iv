"""Payer lookup service for eligibility inquiries"""
import pandas as pd
import re
from typing import Optional
from database import db_session
from models.stedi_payers import StediPayers


class PayerLookupService:
    """Service to lookup trading partner service IDs from database"""

    def __init__(self):
        self.session = db_session

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
            "OF", "THE", "AND", "A", "AN", "IN", "FOR", "ON", "AT", "TO", "BY",
        }

        normalized = self._normalize_name(name)
        words = normalized.split()

        # Keep words that are at least 2 characters and not common words
        keywords = {w for w in words if len(w) >= 2 and w not in common_words}
        return keywords

    def get_payers_from_db(self, payer_id: Optional[str] = None):
        """
        Get payers from database filtered by EligibilityInquiry.

        Args:
            payer_id: Optional payer ID to filter by

        Returns:
            DataFrame of matching payers
        """
        try:
            query = self.session.query(StediPayers)

            # Filter by EligibilityInquiry = 1
            query = query.filter(StediPayers.EligibilityInquiry == 1)

            # Filter by PayerId using LIKE if payer_id is provided
            if payer_id:
                query = query.filter(StediPayers.PayerId.like(f'%{payer_id}'))

            payers = query.all()

            return pd.DataFrame([
                {
                    'PrimaryPayerId': payer.PayerId,
                    'DisplayName': payer.DisplayName,
                    'Aliases': payer.Aliases or ''
                }
                for payer in payers
            ])
        except Exception as e:
            print(f"Error retrieving payers with payer_id={payer_id}: {e}")
            return pd.DataFrame()

    def get_trading_partner_id(self, payor_name: str, payer_id: Optional[str] = None) -> str:
        """
        Get the trading partner service ID (Primary Payer ID) for a given payer name.
        Uses multiple matching strategies: exact match, partial match, and keyword match.
        Searches in both DisplayName and Aliases columns.
        Only returns payers where EligibilityInquiry is True.

        Args:
            payor_name: The name of the payer to search for
            payer_id: Optional payer ID to filter by (e.g., ECS ID)

        Returns:
            str: The Primary Payer ID (PrimaryPayerId) or empty string if not found
        """
        df = self.get_payers_from_db(payer_id)

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


# Create global instance
payer_lookup = PayerLookupService()
