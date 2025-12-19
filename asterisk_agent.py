import asyncio
import os
import logging
import sys

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask, PipelineParams

from pipecat.services.cartesia import CartesiaSTTService, CartesiaTTSService
from pipecat.services.openai import OpenAILLMService
from pipecat.processors.aggregators.openai_llm_context import (
    OpenAILLMContext,
)
from pipecat.frames.frames import LLMMessagesFrame
from audiosocket_transport import AudioSocketTransport
from pipecat.transports.websocket.server import (
    WebsocketServerParams,
    WebsocketServerTransport,
)
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.audio.vad.silero import SileroVADAnalyzer, VADParams
from dotenv import load_dotenv

load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
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

    SAMPLE_RATE = 8000

    try:
        params = TransportParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            vad_analyzer=SileroVADAnalyzer(
                sample_rate=SAMPLE_RATE,
                params=VADParams(
                    stop_secs=0.4,
                    start_secs=0.2,
                    confidence=0.6
                ),
            ),
        )
        transport = AudioSocketTransport(
            params=params, reader=reader, writer=writer, sample_rate=SAMPLE_RATE
        )
        logger.info(f"Transport created (sample_rate={SAMPLE_RATE}Hz)")

        stt = CartesiaSTTService(
            api_key=os.getenv("CARTESIA_API_KEY"), sample_rate=SAMPLE_RATE
        )
        logger.info("STT service created")

        llm = OpenAILLMService(api_key=os.getenv("OPENAI_API_KEY"), model="gpt-4o")
        logger.info("LLM service created")

        tts = CartesiaTTSService(
            api_key=os.getenv("CARTESIA_API_KEY"),
            sample_rate=SAMPLE_RATE,
            encoding="pcm_s16le",
            voice_id="6ccbfb76-1fc6-48f7-b71d-91ac6298247b",
        )
        logger.info(
            f"TTS service created (sample_rate={SAMPLE_RATE}Hz, encoding=pcm_s16le)"
        )

        context = OpenAILLMContext(
            messages=[
                {
                    "role": "system",
                    "content": "You are a friendly and helpful phone voice assistant. "
                    "Keep your responses brief and conversational, as if talking on the phone. "
                    "Speak naturally and avoid long explanations.",
                }
            ]
        )

        context_aggregator = llm.create_context_aggregator(context)
        logger.info("Context aggregator created")

        pipeline = Pipeline(
            [
                transport.input(),
                stt,
                context_aggregator.user(),
                llm,
                tts,
                transport.output(),
                context_aggregator.assistant(),
            ]
        )
        logger.info("Pipeline created")

        task = PipelineTask(
            pipeline,
            params=PipelineParams(
                allow_interruptions=True,
                enable_metrics=False,
                enable_usage_metrics=False,
            ),
        )
        logger.info("Pipeline task created")

        runner = PipelineRunner()

        logger.info("Starting pipeline runner...")

        runner_task = asyncio.create_task(runner.run(task))

        logger.info("Pipeline runner started")

        logger.info("Starting transport...")
        await transport.start()
        logger.info("Transport started - call is now active")

        initial_messages = [
            {
                "role": "system",
                "content": (
                    "You are a helpful voice assistant. "
                    "Keep answers short and conversational."
                ),
            },
            {
                "role": "user",
                "content": "Say hello and introduce yourself briefly.",
            },
        ]

        await task.queue_frames([
            LLMMessagesFrame(initial_messages),
        ])

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
