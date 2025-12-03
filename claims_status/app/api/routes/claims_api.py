from fastapi import APIRouter, HTTPException
from app.config import logger
from app.services import claim_service

router = APIRouter(prefix="/api/v1", tags=["Claims API"])


@router.get(
    "/claim-status",
    summary="Check Claim Status",
    description="Claim Status API Endpoint",
)
async def payer_info():
    data = {}
    try:
        data["results"], data["failed_requests"] = (
            await claim_service.process_claims_from_csv()
        )
        response = {
            "Successful Requestes": len(data["results"]),
            "Failed Requests": len(data["failed_requests"]),
        }
        return response
    except Exception as e:
        logger.exception("Error in /api/v1/claim-status endpoint: %s", e)
        raise HTTPException(status_code=500, detail=str(e))
