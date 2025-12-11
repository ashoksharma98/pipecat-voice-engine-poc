import asyncio
import os
from loguru import logger

from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask

from pipecat.transports.local.audio import (
    LocalAudioTransport,
    LocalAudioTransportParams,
)

from pipecat.services.cartesia.stt import CartesiaSTTService
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.openai.llm import OpenAILLMService

from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.frames.frames import LLMMessagesFrame


from dotenv import load_dotenv

load_dotenv()


async def run_local_voice_agent():

    transport_params = LocalAudioTransportParams(
        audio_in_enabled=True,
        audio_in_sample_rate=16000,
        audio_in_channels=1,
        audio_out_enabled=True,
        audio_out_sample_rate=22050,
        audio_out_channels=1,
    )
    transport = LocalAudioTransport(transport_params)
    print("Output transport:", transport.output())

    stt = CartesiaSTTService(
        api_key=os.getenv("CARTESIA_API_KEY"),
        sample_rate=16000,
    )
    llm = OpenAILLMService(
        api_key=os.getenv("OPENAI_API_KEY"),
        model="gpt-4o",
    )
    tts = CartesiaTTSService(
        api_key=os.getenv("CARTESIA_API_KEY"),
        voice_id="6ccbfb76-1fc6-48f7-b71d-91ac6298247b",
        sample_rate=16000,
        encoding="pcm_s16le",
    )

    context = OpenAILLMContext(
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a helpful voice assistant. "
                    "Keep answers short and conversational."
                ),
            }
        ]
    )
    context_agg = llm.create_context_aggregator(context)

    # 4) Pipeline graph
    pipeline = Pipeline(
        [
            transport.input(),
            stt,
            context_agg.user(),
            llm,
            tts,
            transport.output(),
            context_agg.assistant(),
        ]
    )

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            allow_interruptions=False,
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

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

    runner = PipelineRunner()

    logger.info("Starting local voice agent (Ctrl+C to stop)")
    await runner.run(task)
    logger.info("Local voice agent finished")


if __name__ == "__main__":
    missing = [v for v in ("CARTESIA_API_KEY", "OPENAI_API_KEY") if not os.getenv(v)]
    if missing:
        raise RuntimeError(f"Missing env vars: {missing}")

    asyncio.run(run_local_voice_agent())
