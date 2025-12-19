"""
FastAPI application entry point.

This file initializes the FastAPI application and includes the health check endpoint.
Add additional route imports and include them in the app using app.include_router().
"""

from fastapi import FastAPI

# Initialize FastAPI application
app = FastAPI(
    title="Vistela Backend",
    description="FastAPI backend application",
    version="1.0.0"
)


@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    Returns a simple status to verify the API is running.
    """
    return {"status": "ok"}


# TODO: Add API route imports here
# Example:
# from app.api import router as api_router
# app.include_router(api_router, prefix="/api/v1")

# TODO: Add middleware, CORS, and other configurations here if needed
# Example:
# from fastapi.middleware.cors import CORSMiddleware
# app.add_middleware(CORSMiddleware, allow_origins=["*"])

