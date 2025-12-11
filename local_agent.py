"""
Local audio test for your Cartesia + OpenAI Pipecat pipeline.

Requirements:
  pip install "pipecat-ai[local]"  # enables LocalAudioTransport
  pip install cartesia openai (or whatever clients you actually use)
  export CARTESIA_API_KEY=...
  export OPENAI_API_KEY=...
"""

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
from pipecat.frames.frames import TTSSpeakFrame, TTSAudioRawFrame


from dotenv import load_dotenv

load_dotenv()


from pipecat.frames.frames import Frame, OutputAudioRawFrame
from pipecat.processors.frame_processor import FrameProcessor, FrameDirection

# class RawFileSink(FrameProcessor):
#     def __init__(self, path: str):
#         super().__init__()
#         self._path = path
#         self._fh = None

#     async def start(self):
#         await super().start()
#         self._fh = open(self._path, "wb")

#     async def stop(self):
#         if self._fh:
#             self._fh.close()
#             self._fh = None
#         await super().stop()

#     async def process_frame(self, frame: Frame, direction: FrameDirection):
#         await super().process_frame(frame, direction)

#         # Cartesia TTS produces TTSAudioRawFrame
#         if isinstance(frame, TTSAudioRawFrame) and self._fh:
#             self._fh.write(frame.audio)

#         # Always pass frame on
#         await self.push_frame(frame, direction)

# -------------------------------------------------------------------
# Build pipeline: Local mic/speaker → STT → LLM → TTS → Local speaker
# -------------------------------------------------------------------

async def run_local_voice_agent():

    transport_params = LocalAudioTransportParams(
        audio_in_sample_rate=16000,
        audio_out_sample_rate=16000,
        audio_in_channels=1,
        audio_out_channels=1,
    )
    transport = LocalAudioTransport(transport_params)
    print("Output transport:", transport.output())

    # 2) Services: Cartesia STT, OpenAI LLM, Cartesia TTS
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
        voice_id="6ccbfb76-1fc6-48f7-b71d-91ac6298247b",  # replace with your voice
        sample_rate=16000,
        encoding="pcm_s16le",
    )

    # 3) Conversation context
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
            transport.input(),             # mic → InputAudioRawFrame
            stt,                           # audio → text
            context_agg.user(),            # add user msg
            llm,                           # generate reply
            tts,                           # text → audio
            transport.output(),            # play audio on speakers
            context_agg.assistant(),       # store assistant msg
        ]
    )

    # file_sink = RawFileSink("tts_output.raw")

    # pipeline = Pipeline(
    #     [
    #         # transport.input(),
    #         # stt,
    #         # context_agg.user(),
    #         # llm,
    #         tts,
    #         file_sink,
    #     ]
    # )

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            allow_interruptions=False,
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
    )

    await task.queue_frames([
        TTSSpeakFrame("Hello, I am your local test assistant. Say something and I will reply.")
    ])

    runner = PipelineRunner()

    logger.info("Starting local voice agent (Ctrl+C to stop)")
    await runner.run(task)
    logger.info("Local voice agent finished")


if __name__ == "__main__":
    # Basic env checks
    missing = [v for v in ("CARTESIA_API_KEY", "OPENAI_API_KEY") if not os.getenv(v)]
    if missing:
        raise RuntimeError(f"Missing env vars: {missing}")

    asyncio.run(run_local_voice_agent())
