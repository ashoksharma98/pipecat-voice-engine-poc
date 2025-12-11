import os
from fastapi import FastAPI, WebSocket, Query, WebSocketDisconnect
import logging
import uvicorn
from dotenv import load_dotenv

from agents import run_voice_agent

load_dotenv()

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Asterisk Agents API",
    description="FastAPI application for Asterisk agents",
    version="1.0.0"
)


@app.get("/")
async def root():
    """Health check endpoint"""
    return {"message": "Asterisk Agents API is running", "status": "healthy"}


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok"}


@app.websocket("/asterisk-media")
async def asterisk_media_endpoint(
    websocket: WebSocket,
    call_sid: Optional[str] = Query(None, description="Unique call identifier")
):
    """
    WebSocket endpoint for Asterisk media streaming.
    
    Asterisk connects to: ws://your-server/asterisk-media?call_sid=abc123
    
    Streams s16le mono PCM at 16kHz, 20ms packets (640 bytes)
    """
    
    # Accept the WebSocket connection
    await websocket.accept()
    
    logger.info(f"Asterisk media connection established for call_sid={call_sid}")
    
    try:
        # Run the voice agent pipeline
        await run_voice_agent(websocket, call_sid)
        
    except WebSocketDisconnect:
        logger.info(f"Asterisk disconnected for call_sid={call_sid}")
        
    except Exception as e:
        logger.error(f"Error in asterisk_media_endpoint for call_sid={call_sid}: {e}")
        
    finally:
        logger.info(f"Connection closed for call_sid={call_sid}")


if __name__ == "__main__":
    import uvicorn
    
    required_env_vars = ["CARTESIA_API_KEY", "OPENAI_API_KEY"]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        exit(1)
    
    logger.info("Starting Asterisk Pipecat Voice Agent server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)
