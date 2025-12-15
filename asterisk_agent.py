import asyncio
import os
import logging
from dotenv import load_dotenv
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineTask, PipelineParams
from pipecat.services.cartesia.stt import CartesiaSTTService
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.frames.frames import LLMMessagesFrame, StartFrame, EndFrame
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext

from asterisk_stasis_transport import AudioSocketTransport

load_dotenv()

logger = logging.getLogger(__name__)

async def handle_call(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    """Handle an incoming AudioSocket connection from Asterisk"""
    
    addr = writer.get_extra_info("peername")
    logger.info(f"{'='*60}")
    logger.info(f"NEW CALL from {addr}")
    logger.info(f"{'='*60}")

    transport = None
    runner = None

    try:
        # IMPORTANT: Match your Asterisk dialplan!
        # slin = 8000, slin16 = 16000
        ASTERISK_RATE = 16000
        
        # 1. Create transport
        transport = AudioSocketTransport(
            reader=reader,
            writer=writer,
            asterisk_sample_rate=ASTERISK_RATE
        )
        logger.info(f"Transport created ({ASTERISK_RATE}Hz)")

        # 2. Create services (all use same rate)
        stt = CartesiaSTTService(
            api_key=os.getenv("CARTESIA_API_KEY"),
            sample_rate=ASTERISK_RATE
        )

        llm = OpenAILLMService(
            api_key=os.getenv("OPENAI_API_KEY"),
            model="gpt-4o"
        )

        tts = CartesiaTTSService(
            api_key=os.getenv("CARTESIA_API_KEY"),
            sample_rate=ASTERISK_RATE,
            encoding="pcm_s16le",
            voice_id="6ccbfb76-1fc6-48f7-b71d-91ac6298247b",
        )

        # 3. Create context
        context = OpenAILLMContext(messages=[
            {
                "role": "system",
                "content": "You are a friendly phone assistant. "
                          "Keep responses brief and conversational."
            }
        ])
        context_aggregator = llm.create_context_aggregator(context)

        # 4. Build pipeline
        pipeline = Pipeline([
            transport.input(),
            stt,
            context_aggregator.user(),
            llm,
            tts,
            transport.output(),
            context_aggregator.assistant(),
        ])

        # 5. Create task
        task = PipelineTask(
            pipeline,
            params=PipelineParams(
                allow_interruptions=True,
                enable_metrics=False,
                enable_usage_metrics=False,
            )
        )

        # 6. START RUNNER FIRST
        runner = PipelineRunner()
        runner_task = asyncio.create_task(runner.run(task))
        
        # Wait for pipeline to initialize
        await asyncio.sleep(0.2)
        logger.info("Pipeline runner started")

        # 7. Start transport (reads UUID and begins audio loop)
        await transport.input().start(StartFrame())
        await transport.output().start(StartFrame())
        logger.info("Transport started - call is active")

        # 8. Send initial greeting
        await task.queue_frames([
            LLMMessagesFrame([
                {
                    "role": "system",
                    "content": "You are a helpful assistant."
                },
                {
                    "role": "user",
                    "content": "Say hello and introduce yourself in one sentence."
                },
            ])
        ])

        # 9. Wait for call to complete
        logger.info("Waiting for call to complete...")
        await runner_task

    except asyncio.CancelledError:
        logger.warning("Call handler cancelled")
        raise

    except Exception as e:
        logger.error(f"Error handling call: {e}", exc_info=True)

    finally:
        logger.info("Cleaning up call resources...")
        
        if transport:
            try:
                await transport.input().stop(EndFrame())
                await transport.output().stop(EndFrame())
            except Exception as e:
                logger.error(f"Error stopping transport: {e}")
        
        try:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
        except Exception as e:
            logger.error(f"Error closing writer: {e}")
        
        logger.info(f"Call from {addr} ended")
        logger.info(f"{'='*60}")


# Server setup
async def main():
    server = await asyncio.start_server(
        handle_call, 
        host="0.0.0.0", 
        port=9092
    )
    
    logger.info("AudioSocket server listening on 0.0.0.0:9092")
    
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
