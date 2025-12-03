from app.database import db_session
from app.models.StediPayersModel import StediPayers
from app.config import logger
from typing import Optional, List, Dict, Any
from functools import lru_cache


class PayerCruds:
    """
    CRUD operations for PayorBenefits model.
    """

    def __init__(self):
        self.session = db_session

    def get_all_payers(self, payer_id) -> List[Dict[str, Any]]:
        """
        Retrieve payers from the database filtered by ClaimStatusInquiry and PayerId.

        Args:
            payer_id: Payer ID to search (partial match using LIKE)

        Returns:
            List[Dict[str, Any]]: List of matching payers as dictionaries
        """
        try:
            query = self.session.query(StediPayers)

            # Filter by ClaimStatusInquiry = 1
            query = query.filter(StediPayers.ClaimStatusInquiry == 1)

            # Filter by PayerId using LIKE if payer_id is provided
            if payer_id:
                query = query.filter(StediPayers.PayerId.like(f'%{payer_id}'))

            payers = query.all()

            return [
                {
                    'PrimaryPayerId': payer.PayerId,
                    'DisplayName': payer.DisplayName,
                    'Aliases': payer.Aliases or ''
                }
                for payer in payers
            ]
        except Exception as e:
            logger.error(f"Error retrieving payers with payer_id={payer_id}: {e}")
            return []


payer_obj = PayerCruds()
