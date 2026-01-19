"""Main application entry point"""

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
from database import init_db
from routes import router
from log import log_info, log_error

# Load environment variables
load_dotenv()

# Create FastAPI app
app = FastAPI(
    title="Metadata Service API",
    description="A metadata service for data governance - tracks datasets and lineage",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routes
app.include_router(router, prefix="/api/v1", tags=["metadata"])


@app.on_event("startup")
async def startup_event():
    """Initialize database on startup"""
    try:
        log_info("Starting Metadata Service API...")
        init_db()
        log_info("Database initialized successfully")
        log_info("API is ready to accept requests")
    except Exception as e:
        log_error("StartupError", f"Failed to start application: {str(e)}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    log_info("Shutting down Metadata Service API...")


if __name__ == "__main__":
    import uvicorn
    
    host = os.getenv("APP_HOST", "0.0.0.0")
    port = int(os.getenv("APP_PORT", 8000))
    
    log_info(f"Starting server on {host}:{port}")
    uvicorn.run(
        "main:app",
        host=host,
        port=port,
        reload=True,
        log_level="info"
    )
    