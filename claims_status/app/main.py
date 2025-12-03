# app/main.py
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from contextlib import asynccontextmanager
from app.config import logger, get_settings
from app.api.routes.claims_api import router as payers_router

# Initialize settings
settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan event handler for startup and shutdown events.
    """
    # Startup event
    logger.info("Startup event triggered")
    yield
    # Shutdown event
    logger.info("Shutdown event triggered")


# Create FastAPI app
app = FastAPI(
    title="Claim Status",
    description="Check the status of claims via API",
    version="1.0.0",
    lifespan=lifespan,
)

# Add middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=settings.ALLOWED_HOSTS
)


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"message": "Internal server error", "details": str(exc)},
    )

@app.get(
    "/health", summary="Health check", description="Check the health of the API"
)
async def health():
    return {"message": "API is healthy"}

app.include_router(payers_router)


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="127.0.0.1",
        port=8001,
    )
