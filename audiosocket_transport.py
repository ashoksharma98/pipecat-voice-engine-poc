import asyncio
import struct
import logging
import audioop
import numpy as np
from scipy import signal
from typing import Optional

from pipecat.processors.frame_processor import FrameProcessor, FrameDirection
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.transports.base_input import BaseInputTransport
from pipecat.transports.base_output import BaseOutputTransport
from pipecat.frames.frames import (
    InputAudioRawFrame,
    OutputAudioRawFrame,
    StartFrame,
    EndFrame,
    CancelFrame,
    UserSpeakingFrame,
    UserStartedSpeakingFrame,
    UserStoppedSpeakingFrame,
)
from pipecat.audio.vad.vad_analyzer import VADState

logger = logging.getLogger("audiosocket_transport")
logger.setLevel(logging.DEBUG)

# AudioSocket protocol constants
MSG_UUID = 0x01
MSG_AUDIO = 0x10
MSG_HANGUP = 0x00
CHUNK_SIZE = 320  # 20ms at 8kHz = 160 samples * 1 byte = 160 bytes for SLIN
AUDIO_INPUT_TIMEOUT_SECS = 0.5


class AudioSocketTransportParams(TransportParams):
    def __init__(self):
        super().__init__()


class AudioSocketInput(BaseInputTransport):
    """
    Handles incoming audio from Asterisk via AudioSocket protocol.
    Inherits from BaseInputTransport to handle Start/Stop lifecycle automatically.
    """

    def __init__(
        self,
        params: TransportParams,
        reader: asyncio.StreamReader,
        asterisk_sample_rate: int = 8000,
        pipeline_sample_rate: int = 16000,
    ):
        # BaseInputTransport expects params
        super().__init__(params)
        
        self._reader = reader
        self._asterisk_sample_rate = asterisk_sample_rate
        self._pipeline_sample_rate = pipeline_sample_rate
        
        self._read_task: Optional[asyncio.Task] = None
        self._uuid_read = False  # Gate: Wait for UUID before reading audio
        self._resample_state = None  # State for audioop resampling

    async def start(self, frame: StartFrame):
        """
        Called automatically by BaseInputTransport when StartFrame is received.
        """
        await super().start(frame)
        logger.debug("AudioSocketInput: Received StartFrame (Pipeline Started)")
        self._running = True
        
        # Only start the loop if we have already shaken hands (UUID read)
        # Otherwise, start_reading() will trigger this later.
        if self._uuid_read:
            self._start_read_loop()
        # await self.set_transport_ready(frame)

    async def stop(self, frame: EndFrame):
        """
        Called automatically by BaseInputTransport when EndFrame/CancelFrame is received.
        """
        await super().stop(frame)
        logger.debug(f"AudioSocketInput: Received {frame.__class__.__name__} - Stopping")
        self._running = False
        if self._read_task and not self._read_task.done():
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass

    def start_reading(self):
        """
        Public method called by the parent Transport after the UUID header is successfully read.
        """
        logger.info("AudioSocketInput: UUID verification complete. Enabling audio read.")
        self._uuid_read = True
        
        # If the pipeline is already running (StartFrame received), kick off the loop now.
        if self._running and (self._read_task is None or self._read_task.done()):
            self._start_read_loop()

    def _start_read_loop(self):
        """Helper to safely start the task"""
        if self._read_task is None or self._read_task.done():
            self._read_task = asyncio.create_task(self._read_loop())
            logger.info("AudioSocketInput: Audio read loop task started")

    async def _read_loop(self):
        """Read audio packets from Asterisk"""
        try:
            logger.info(f"AudioSocketInput: Audio read loop active (Upsampling {self._asterisk_sample_rate} -> {self._pipeline_sample_rate})")
            vad_analyzer = self._params.vad_analyzer
            while self._running:
                try:
                    # 1. Read 3-byte header
                    header = await self._reader.readexactly(3)
                    msg_type = header[0]
                    length = struct.unpack(">H", header[1:])[0]

                    # 2. Read payload
                    payload = await self._reader.readexactly(length)

                    if msg_type == MSG_AUDIO:
                        if vad_analyzer:
                            vad_state = await vad_analyzer.analyze_audio(payload)

                            if vad_state == VADState.STARTING:
                                logger.debug("VAD: Speech Starting")
                                await self.push_frame(UserStartedSpeakingFrame())
                            
                            elif vad_state == VADState.STOPPING:
                                logger.debug("VAD: Speech Stopped")
                                await self.push_frame(UserStoppedSpeakingFrame())

                        frame = InputAudioRawFrame(
                            audio=payload, 
                            sample_rate=self._pipeline_sample_rate, 
                            num_channels=1
                        )
                        
                        await self.push_frame(frame)

                    elif msg_type == MSG_HANGUP:
                        logger.info("AudioSocketInput: Received HANGUP from Asterisk")
                        self._running = False
                        # Push EndFrame to signal pipeline shutdown
                        await self.push_frame(EndFrame())
                        break

                except asyncio.IncompleteReadError:
                    logger.warning("AudioSocketInput: Connection closed by peer")
                    self._running = False
                    await self.push_frame(EndFrame())
                    break

        except asyncio.CancelledError:
            logger.debug("AudioSocketInput: Read loop cancelled")

        except Exception as e:
            logger.error(f"AudioSocketInput: Error in read loop: {e}", exc_info=True)
            self._running = False
            await self.push_frame(EndFrame())


class AudioSocketOutput(BaseOutputTransport):
    """Handles outgoing audio to Asterisk via AudioSocket protocol"""

    def __init__(
        self,
        params: TransportParams,
        writer: asyncio.StreamWriter,
        input_sample_rate: int = 16000,
        output_sample_rate: int = 8000,
        debug: bool = False,
    ):
        super().__init__(params)
        self._writer = writer
        self._input_sample_rate = input_sample_rate   # TTS output rate (16kHz)
        self._output_sample_rate = output_sample_rate # Asterisk expects 8kHz
        self._debug = debug
        
        self._is_open = False
        self._resample_state = None  # State for audioop resampling

    async def start(self, frame: StartFrame):
        """Called automatically when the pipeline starts"""
        await super().start(frame)
        logger.debug("AudioSocketOutput: Starting")
        self._is_open = True

    async def stop(self, frame: EndFrame):
        """Called automatically when the pipeline stops"""
        await super().stop(frame)
        logger.debug("AudioSocketOutput: Stopping")
        if self._is_open:
            await self._send_hangup()
        self._is_open = False

    def _downsample_audio(self, audio_bytes: bytes) -> bytes:
        """
        Downsample audio from 16kHz (TTS) to 8kHz (Asterisk) using audioop.
        Replaces scipy/numpy to prevent event loop blocking.
        """
        if self._input_sample_rate == self._output_sample_rate:
            return audio_bytes

        # audioop.ratecv(fragment, width, nchannels, inrate, outrate, state)
        # width=2 (16-bit), nchannels=1
        new_fragment, self._resample_state = audioop.ratecv(
            audio_bytes,
            2,
            1,
            self._input_sample_rate,
            self._output_sample_rate,
            self._resample_state
        )
        return new_fragment

    async def _send_audio_chunks(self, audio_data: bytes):
        """Send audio data in 320-byte chunks via AudioSocket protocol with pacing"""
        total_chunks = (len(audio_data) + CHUNK_SIZE - 1) // CHUNK_SIZE
        chunks_sent = 0

        try:
            for i in range(0, len(audio_data), CHUNK_SIZE):
                if not self._is_open:
                    break

                chunk = audio_data[i : i + CHUNK_SIZE]

                # Pad last chunk if needed to maintain frame size
                if len(chunk) < CHUNK_SIZE:
                    chunk = chunk + (b"\x00" * (CHUNK_SIZE - len(chunk)))

                # Create AudioSocket packet: [type(1)] [length(2)] [data(n)]
                # >H = Big Endian Unsigned Short (Standard for AudioSocket)
                header = struct.pack("B", MSG_AUDIO) + struct.pack(">H", len(chunk))

                self._writer.write(header + chunk)
                await self._writer.drain()

                # Critical: Sleep for 20ms to pace the audio delivery to Asterisk
                # (320 bytes @ 8kHz 16-bit = 20ms audio)
                await asyncio.sleep(0.02)

                chunks_sent += 1

                if self._debug and chunks_sent % 10 == 0:
                    logger.debug(f"📡 Sent chunk {chunks_sent}/{total_chunks}")

        except Exception as e:
            logger.error(f"❌ AudioSocketOutput: Error sending chunk: {e}")
            self._is_open = False

    async def _send_hangup(self):
        """Sends the AudioSocket HANGUP signal"""
        try:
            hangup_msg = struct.pack("B", MSG_HANGUP) + struct.pack(">H", 0)
            self._writer.write(hangup_msg)
            await self._writer.drain()
            logger.debug("AudioSocketOutput: Sent HANGUP to Asterisk")
        except Exception as e:
            logger.warning(f"AudioSocketOutput: Could not send hangup: {e}")

    async def process_frame(self, frame, direction):
        """Process frames and send audio to Asterisk"""
        await super().process_frame(frame, direction)

        if isinstance(frame, OutputAudioRawFrame):
            if self._is_open and frame.audio and len(frame.audio) > 0:
                try:
                    # 1. Downsample (Fast C-implementation)
                    downsampled_audio = self._downsample_audio(frame.audio)

                    # 2. Send in paced chunks
                    await self._send_audio_chunks(downsampled_audio)
                
                except Exception as e:
                    logger.error(f"AudioSocketOutput: Error processing audio frame: {e}")
        
        elif isinstance(frame, (EndFrame, CancelFrame)):
            # Stop lifecycle is handled by self.stop(), but we ensure cleanup here too
            await self.stop(frame)

        await self.push_frame(frame, direction)


class AudioSocketTransport(BaseTransport):
    """
    AudioSocket transport for Asterisk external media.

    Handles the AudioSocket protocol for bidirectional audio streaming
    between Asterisk and Pipecat.

    Note: Asterisk AudioSocket uses 8kHz SLIN format, while this transport
    upsamples incoming audio to 16kHz for STT and downsamples outgoing
    audio from TTS back to 8kHz for Asterisk.
    """

    def __init__(
        self,
        params: TransportParams,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        sample_rate: int = 16000,
    ):
        super().__init__()
        self._reader = reader
        self._writer = writer
        self._sample_rate = sample_rate
        self._params = params

        self._input_processor = AudioSocketInput(
            params=self._params,
            reader=reader, 
            pipeline_sample_rate=sample_rate
        )
        self._output_processor = AudioSocketOutput(
            params=self._params, 
            writer=writer, 
            input_sample_rate=sample_rate
        )

    def input(self) -> FrameProcessor:
        """Return the input processor for the pipeline"""
        return self._input_processor

    def output(self) -> FrameProcessor:
        """Return the output processor for the pipeline"""
        return self._output_processor

    async def start(self):
        """
        Initialize the AudioSocket connection.
        Must be called AFTER the pipeline is running.

        This reads the UUID packet first, then signals the input processor
        to start reading audio packets.
        """
        try:
            logger.debug("AudioSocketTransport: Reading UUID header")

            # Read initial UUID message
            header = await self._reader.readexactly(3)
            msg_type = header[0]
            length = struct.unpack(">H", header[1:])[0]

            if msg_type == MSG_UUID:
                uuid_payload = await self._reader.readexactly(length)
                logger.info(f"AudioSocketTransport: Call UUID = {uuid_payload.hex()}")
            else:
                logger.warning(
                    f"AudioSocketTransport: Expected UUID (0x01) but got 0x{msg_type:02x}"
                )

            # Now that UUID is read, tell input processor it can start reading audio
            self._input_processor.start_reading()

            logger.info(
                "AudioSocketTransport: Initialized successfully (8kHz ↔ 16kHz conversion enabled)"
            )

        except Exception as e:
            logger.error(
                f"AudioSocketTransport: Initialization failed: {e}", exc_info=True
            )
            raise

    async def stop(self):
        """Cleanup transport resources"""
        logger.debug("AudioSocketTransport: Stopping")

        if (
            self._input_processor._read_task
            and not self._input_processor._read_task.done()
        ):
            self._input_processor._read_task.cancel()
            try:
                await self._input_processor._read_task
            except asyncio.CancelledError:
                pass

        try:
            if not self._writer.is_closing():
                self._writer.close()
                await self._writer.wait_closed()
        except Exception as e:
            logger.debug(f"AudioSocketTransport: Error closing writer: {e}")
