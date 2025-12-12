import asyncio
import os
import logging
import sys

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask, PipelineParams

from pipecat.services.cartesia.stt import CartesiaSTTService
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.processors.aggregators.openai_llm_context import (
    OpenAILLMContext,
)
from pipecat.frames.frames import LLMMessagesFrame
from audiosocket_transport import AudioSocketTransport, AudioSocketTransportParams

from pipecat.audio.vad.silero import SileroVADAnalyzer
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    stream=sys.stdout
)

logger = logging.getLogger(__name__)

HOST = "0.0.0.0"
PORT = 9092


async def handle_call(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Handle an incoming AudioSocket connection from Asterisk"""
    
    addr = writer.get_extra_info("peername")
    logger.info(f"=" * 60)
    logger.info(f"NEW CALL from {addr}")
    logger.info(f"=" * 60)

    transport = None
    runner = None

    try:
        # 1. Create transport (but don't start yet)
        # IMPORTANT: Asterisk slin16 format = 16kHz, 16-bit signed PCM, mono
        SAMPLE_RATE = 16000
        transport = AudioSocketTransport(reader=reader, writer=writer, sample_rate=SAMPLE_RATE, params=AudioSocketTransportParams(vad_analyzer=SileroVADAnalyzer()))
        logger.info(f"Transport created (sample_rate={SAMPLE_RATE}Hz)")

        # 2. Create services - ALL must use same sample rate
        stt = CartesiaSTTService(
            api_key=os.getenv("CARTESIA_API_KEY"),
            sample_rate=SAMPLE_RATE
        )
        logger.info("STT service created")

        llm = OpenAILLMService(
            api_key=os.getenv("OPENAI_API_KEY"),
            model="gpt-4o"
        )
        logger.info("LLM service created")

        tts = CartesiaTTSService(
            api_key=os.getenv("CARTESIA_API_KEY"),
            sample_rate=SAMPLE_RATE,
            encoding="pcm_s16le",
            voice_id="6ccbfb76-1fc6-48f7-b71d-91ac6298247b",
        )
        logger.info(f"TTS service created (sample_rate={SAMPLE_RATE}Hz, encoding=pcm_s16le)")

        # 3. Create context
        context = OpenAILLMContext(messages=[
            {
                "role": "system",
                "content": "You are a friendly and helpful phone voice assistant. "
                          "Keep your responses brief and conversational, as if talking on the phone. "
                          "Speak naturally and avoid long explanations."
            }
        ])

        context_aggregator = llm.create_context_aggregator(context)
        logger.info("Context aggregator created")

        # 4. Build pipeline
        pipeline = Pipeline([
            transport.input(),           # Audio from Asterisk
            stt,                         # Speech-to-text
            context_aggregator.user(),   # User message aggregation
            llm,                         # LLM processing
            tts,                         # Text-to-speech
            transport.output(),          # Audio to Asterisk
            context_aggregator.assistant(), # Assistant message aggregation
        ])
        logger.info("Pipeline created")

        # 5. Create pipeline task
        task = PipelineTask(
            pipeline,
            params=PipelineParams(
                allow_interruptions=True,
                enable_metrics=False,
                enable_usage_metrics=False,
            )
        )
        logger.info("Pipeline task created")

        # 6. Start the pipeline runner FIRST (this will send StartFrame)
        runner = PipelineRunner()
        logger.info("Starting pipeline runner...")
        # initial_messages = [
        #     {
        #         "role": "system",
        #         "content": (
        #             "You are a helpful voice assistant. "
        #             "Keep answers short and conversational."
        #         ),
        #     },
        #     {
        #         "role": "user",
        #         "content": "Say hello and introduce yourself briefly.",
        #     },
        # ]
        
        # await task.queue_frames([
        #     LLMMessagesFrame(initial_messages),
        # ])
        # await asyncio.sleep(7)
        # Run pipeline in background
        runner_task = asyncio.create_task(runner.run(task))
        
        # Wait a moment for pipeline to initialize and StartFrame to propagate
        await asyncio.sleep(0.2)
        logger.info("Pipeline runner started")

        # 7. NOW start the transport (reads UUID and begins receiving audio)
        logger.info("Starting transport...")
        await transport.start()
        logger.info("Transport started - call is now active")

        # 8. Wait for pipeline to complete (call ends)
        logger.info("Waiting for call to complete...")
        await runner_task
        logger.info("Pipeline completed")

    except asyncio.CancelledError:
        logger.warning("Call handler cancelled")
        raise

    except Exception as e:
        logger.error(f"Error handling call: {e}", exc_info=True)

    finally:
        # Cleanup
        logger.info("Cleaning up call resources...")
        
        if transport:
            try:
                await transport.stop()
            except Exception as e:
                logger.error(f"Error stopping transport: {e}")
        
        # No need to stop runner - it stops automatically when pipeline ends
        
        try:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except Exception as e:
            logger.error(f"Error closing writer: {e}")
        
        logger.info(f"Call from {addr} ended")
        logger.info("=" * 60)


async def main():
    """Start the AudioSocket server"""
    
    # Verify environment variables
    required_vars = ["CARTESIA_API_KEY", "OPENAI_API_KEY"]
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        logger.error("Please set them in your .env file")
        return
    
    logger.info("=" * 60)
    logger.info("PIPECAT AUDIOSOCKET SERVER")
    logger.info("=" * 60)
    logger.info(f"Listening on {HOST}:{PORT}")
    logger.info("Waiting for calls from Asterisk...")
    logger.info("=" * 60)
    
    server = await asyncio.start_server(handle_call, HOST, PORT)
    
    try:
        async with server:
            await server.serve_forever()
    
    except KeyboardInterrupt:
        logger.info("\nShutting down server (Ctrl+C pressed)")
    
    except asyncio.CancelledError:
        logger.info("Server cancelled")
    
    finally:
        logger.info("Server stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nExiting...")