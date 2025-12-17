import os
import asyncio
import logging
from dotenv import load_dotenv
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask, PipelineParams
from pipecat.services.cartesia.stt import CartesiaSTTService
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.frames.frames import LLMMessagesFrame, StartFrame, EndFrame
from pipecat.transports.base_transport import TransportParams
from pipecat.processors.aggregators.openai_llm_context import (
    OpenAILLMContext,
    OpenAILLMContextFrame,
)

from asterisk_stasis_transport import AudioSocketTransport

load_dotenv()

logger = logging.getLogger(__name__)


HOST = "0.0.0.0"
PORT = 9092
ASTERISK_SAMPLE_RATE = 16000


async def handle_call(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    print("NEW CALL")

    addr = writer.get_extra_info("peername")
    logger.info(f"=" * 60)
    logger.info(f"NEW CALL from {addr}")
    logger.info(f"=" * 60)

    transport = None
    runner = None

    try:
        transport = AudioSocketTransport(
            reader=reader, writer=writer, sample_rate=ASTERISK_SAMPLE_RATE, asterisk_sample_rate=ASTERISK_SAMPLE_RATE
        )
        logger.info(f"Transport created (sample_rate={ASTERISK_SAMPLE_RATE}Hz)")

        stt = CartesiaSTTService(
            api_key=os.getenv("CARTESIA_API_KEY"), sample_rate=ASTERISK_SAMPLE_RATE
        )
        logger.info("STT service created")

        llm = OpenAILLMService(api_key=os.getenv("OPENAI_API_KEY"), model="gpt-4o")
        logger.info("LLM service created")

        tts = CartesiaTTSService(
            api_key=os.getenv("CARTESIA_API_KEY"),
            sample_rate=ASTERISK_SAMPLE_RATE,
            encoding="pcm_s16le",
            voice_id="6ccbfb76-1fc6-48f7-b71d-91ac6298247b",
        )
        logger.info(
            f"TTS service created (sample_rate={ASTERISK_SAMPLE_RATE}Hz, encoding=pcm_s16le)"
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
        # await asyncio.sleep(7)
        # Run pipeline in background
        runner_task = asyncio.create_task(runner.run(task))

        await asyncio.sleep(0.2)
        logger.info("Pipeline runner started")

        logger.info("Starting transport...")
        await transport.start()
        logger.info("Transport started - call is now active")

        logger.info("Waiting for call to complete...")
        await runner_task
        logger.info("Pipeline completed")

    except asyncio.CancelledError:
        logger.warning("Call handler cancelled")
        raise

    except Exception as e:
        logger.error(f"Error handling call: {e}", exc_info=True)

    finally:
        logger.info("Cleaning up call resources...")

        if transport:
            try:
                await transport.stop()
            except Exception as e:
                logger.error(f"Error stopping transport: {e}")

        try:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except Exception as e:
            logger.error(f"Error closing writer: {e}")

        logger.info(f"Call from {addr} ended")
        logger.info("=" * 60)


async def main():
    server = await asyncio.start_server(handle_call, HOST, PORT)
    print(f"Listening on {HOST}:{PORT}")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
