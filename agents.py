import asyncio
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Query
from loguru import logger
from typing import Optional

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.services.cartesia import CartesiaSTTService, CartesiaTTSService
from pipecat.services.openai import OpenAILLMService
from pipecat.processors.aggregators.openai_llm_context import (
    OpenAILLMContext,
    OpenAILLMContextFrame,
)
from pipecat.frames.frames import EndFrame

from transport import AsteriskTransport, AsteriskTransportParams


async def run_voice_agent(websocket: WebSocket, call_sid: str):
    """
    Build and run the complete Pipecat pipeline for a voice call.
    
    Pipeline: Asterisk Audio → STT → LLM → TTS → Asterisk Audio
    """
    
    # Create transport
    transport_params = AsteriskTransportParams(
        call_sid=call_sid,
        audio_in_sample_rate=16000,
        audio_out_sample_rate=16000
    )
    
    transport = AsteriskTransport(
        websocket=websocket,
        params=transport_params
    )
    
    logger.info(f"Transport created for call_sid={call_sid}")
    
    # Initialize services
    try:
        # STT: Cartesia Speech-to-Text
        stt = CartesiaSTTService(
            api_key=os.getenv("CARTESIA_API_KEY"),
            sample_rate=16000
        )
        logger.info("Cartesia STT initialized")
        
        # LLM: OpenAI for conversation
        llm = OpenAILLMService(
            api_key=os.getenv("OPENAI_API_KEY"),
            model="gpt-4o"
        )
        logger.info("OpenAI LLM initialized")
        
        # TTS: Cartesia Text-to-Speech
        tts = CartesiaTTSService(
            api_key=os.getenv("CARTESIA_API_KEY"),
            voice_id="79a125e8-cd45-4c13-8a67-188112f4dd22",  # Default voice
            sample_rate=16000,
            encoding="pcm_s16le"
        )
        logger.info("Cartesia TTS initialized")
        
        # Context for managing conversation history
        context = OpenAILLMContext(
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a helpful voice assistant. "
                        "Keep responses concise and conversational. "
                        "Speak naturally as if in a phone conversation."
                    )
                }
            ]
        )
        context_aggregator = llm.create_context_aggregator(context)
        
        logger.info("Context aggregator created")
        
        # Build pipeline
        pipeline = Pipeline([
            transport.input(),              # Receive audio from Asterisk
            stt,                           # Convert speech to text
            context_aggregator.user(),     # Add user message to context
            llm,                           # Generate response
            tts,                           # Convert text to speech
            transport.output(),            # Send audio back to Asterisk
            context_aggregator.assistant() # Add assistant message to context
        ])
        
        logger.info(f"Pipeline built for call_sid={call_sid}")
        
        # Create pipeline task
        task = PipelineTask(
            pipeline,
            params=PipelineParams(
                allow_interruptions=False,  # No barge-in for now
                enable_metrics=True,
                enable_usage_metrics=True
            )
        )
        
        logger.info(f"Pipeline task created for call_sid={call_sid}")
        
        # Queue initial context to start conversation
        await task.queue_frames([
            context_aggregator.user().get_context_frame()
        ])
        
        # Run the pipeline
        runner = PipelineRunner()
        
        logger.info(f"Starting pipeline for call_sid={call_sid}")
        
        await runner.run(task)
        
        logger.info(f"Pipeline completed for call_sid={call_sid}")
        
    except Exception as e:
        logger.error(f"Error running pipeline for call_sid={call_sid}: {e}")
        raise
    finally:
        # Cleanup
        await transport.stop()
        logger.info(f"Transport stopped for call_sid={call_sid}")
