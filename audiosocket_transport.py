import asyncio
import struct
import logging
import numpy as np
from scipy import signal
from typing import Optional

from pipecat.processors.frame_processor import FrameProcessor, FrameDirection
from pipecat.transports.base_transport import BaseTransport
from pipecat.frames.frames import (
    InputAudioRawFrame,
    OutputAudioRawFrame,
    StartFrame,
    EndFrame,
    CancelFrame,
)

logger = logging.getLogger("audiosocket_transport")
logger.setLevel(logging.DEBUG)

# AudioSocket protocol constants
MSG_UUID = 0x01
MSG_AUDIO = 0x10
MSG_HANGUP = 0x00
CHUNK_SIZE = 160  # 20ms at 8kHz = 160 samples * 1 byte = 160 bytes for SLIN


class AudioSocketInput(FrameProcessor):
    """Handles incoming audio from Asterisk via AudioSocket protocol"""
    
    def __init__(self, reader: asyncio.StreamReader, sample_rate: int = 16000):
        super().__init__()
        self._reader = reader
        self._sample_rate = sample_rate  # Output sample rate for STT (16kHz)
        self._asterisk_sample_rate = 8000  # Asterisk sends 8kHz
        self._running = False
        self._read_task: Optional[asyncio.Task] = None
        self._uuid_read = False

    def _upsample_audio(self, audio_bytes: bytes) -> bytes:
        """Upsample audio from 8kHz (Asterisk) to 16kHz (STT)"""
        if self._asterisk_sample_rate == self._sample_rate:
            return audio_bytes
        
        # Convert bytes to numpy array (16-bit signed PCM)
        audio_data = np.frombuffer(audio_bytes, dtype=np.int16)
        
        # Calculate target number of samples
        num_samples = int(len(audio_data) * self._sample_rate / self._asterisk_sample_rate)
        
        # Resample using scipy
        resampled = signal.resample(audio_data, num_samples)
        
        # Convert back to int16 and then to bytes
        resampled_int16 = np.clip(resampled, -32768, 32767).astype(np.int16)
        return resampled_int16.tobytes()

    async def process_frame(self, frame, direction):
        """Handle control frames"""
        await super().process_frame(frame, direction)
        
        if isinstance(frame, StartFrame):
            logger.debug("AudioSocketInput received StartFrame - starting audio read loop")
            self._running = True
            if self._uuid_read:
                self._read_task = asyncio.create_task(self._read_loop())
        
        elif isinstance(frame, (EndFrame, CancelFrame)):
            logger.debug(f"AudioSocketInput received {frame.__class__.__name__} - stopping")
            self._running = False
            if self._read_task and not self._read_task.done():
                self._read_task.cancel()
        
        await self.push_frame(frame, direction)

    def start_reading(self):
        """Called by transport after UUID is read to start the audio loop"""
        if self._running and not self._read_task:
            self._uuid_read = True
            self._read_task = asyncio.create_task(self._read_loop())
            logger.info("AudioSocketInput: Read loop task started")

    async def _read_loop(self):
        """Read audio packets from Asterisk"""
        try:
            logger.info("AudioSocketInput: Audio read loop started")
            
            while self._running:
                try:
                    # Read 3-byte header
                    header = await self._reader.readexactly(3)
                    msg_type = header[0]
                    length = struct.unpack(">H", header[1:])[0]
                    
                    # Read payload
                    payload = await self._reader.readexactly(length)
                    
                    if msg_type == MSG_AUDIO:
                        # Upsample from 8kHz to 16kHz for STT
                        upsampled_audio = self._upsample_audio(payload)
                        
                        # Push audio downstream to STT
                        frame = InputAudioRawFrame(
                            audio=upsampled_audio,
                            sample_rate=self._sample_rate,
                            num_channels=1
                        )
                        await self.push_frame(frame, FrameDirection.DOWNSTREAM)
                    
                    elif msg_type == MSG_HANGUP:
                        logger.info("AudioSocketInput: Received HANGUP from Asterisk")
                        self._running = False
                        await self.push_frame(EndFrame(), FrameDirection.DOWNSTREAM)
                        break
                
                except asyncio.IncompleteReadError:
                    logger.warning("AudioSocketInput: Connection closed by peer")
                    self._running = False
                    await self.push_frame(EndFrame(), FrameDirection.DOWNSTREAM)
                    break
        
        except asyncio.CancelledError:
            logger.debug("AudioSocketInput: Read loop cancelled")
        
        except Exception as e:
            logger.error(f"AudioSocketInput: Error in read loop: {e}", exc_info=True)
            self._running = False
            await self.push_frame(EndFrame(), FrameDirection.DOWNSTREAM)


class AudioSocketOutput(FrameProcessor):
    """Handles outgoing audio to Asterisk via AudioSocket protocol"""
    
    def __init__(self, writer: asyncio.StreamWriter, input_sample_rate: int = 16000):
        super().__init__()
        self._writer = writer
        self._is_open = True
        self._input_sample_rate = input_sample_rate  # TTS output rate (16kHz)
        self._output_sample_rate = 8000  # Asterisk expects 8kHz

    def _downsample_audio(self, audio_bytes: bytes) -> bytes:
        """Downsample audio from 16kHz (TTS) to 8kHz (Asterisk)"""
        if self._input_sample_rate == self._output_sample_rate:
            return audio_bytes
        
        # Convert bytes to numpy array (16-bit signed PCM)
        audio_data = np.frombuffer(audio_bytes, dtype=np.int16)
        
        # Calculate target number of samples
        num_samples = int(len(audio_data) * self._output_sample_rate / self._input_sample_rate)
        
        # Resample using scipy
        resampled = signal.resample(audio_data, num_samples)
        
        # Convert back to int16 and then to bytes
        resampled_int16 = np.clip(resampled, -32768, 32767).astype(np.int16)
        return resampled_int16.tobytes()

    async def process_frame(self, frame, direction):
        """Process frames and send audio to Asterisk"""
        await super().process_frame(frame, direction)
        
        if isinstance(frame, OutputAudioRawFrame) and self._is_open:
            audio = frame.audio
            
            if audio and len(audio) > 0:
                logger.debug(f"AudioSocketOutput: Received {len(audio)} bytes from TTS")
                
                try:
                    # Downsample from 16kHz to 8kHz for Asterisk
                    downsampled_audio = self._downsample_audio(audio)
                    logger.debug(f"AudioSocketOutput: Downsampled to {len(downsampled_audio)} bytes")
                    
                    # Send audio in chunks
                    for i in range(0, len(downsampled_audio), CHUNK_SIZE):
                        chunk = downsampled_audio[i:i + CHUNK_SIZE]
                        
                        # Pad last chunk if needed
                        if len(chunk) < CHUNK_SIZE:
                            chunk = chunk + (b"\x00" * (CHUNK_SIZE - len(chunk)))
                        
                        # Create AudioSocket packet: [type(1)] [length(2)] [data(n)]
                        header = struct.pack("B", MSG_AUDIO) + struct.pack(">H", len(chunk))
                        
                        self._writer.write(header + chunk)
                        await self._writer.drain()
                    
                    logger.debug("AudioSocketOutput: Audio sent successfully")
                
                except Exception as e:
                    logger.error(f"AudioSocketOutput: Error writing to socket: {e}")
                    self._is_open = False
        
        elif isinstance(frame, (EndFrame, CancelFrame)):
            logger.debug(f"AudioSocketOutput: Received {frame.__class__.__name__}")
            if self._is_open:
                try:
                    hangup_msg = struct.pack("B", MSG_HANGUP) + struct.pack(">H", 0)
                    self._writer.write(hangup_msg)
                    await self._writer.drain()
                    logger.debug("AudioSocketOutput: Sent HANGUP to Asterisk")
                except Exception as e:
                    logger.debug(f"AudioSocketOutput: Could not send hangup: {e}")
                self._is_open = False
        
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
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        sample_rate: int = 16000  # Sample rate for STT/TTS (will convert to/from 8kHz)
    ):
        super().__init__()
        self._reader = reader
        self._writer = writer
        self._sample_rate = sample_rate
        
        self._input_processor = AudioSocketInput(reader, sample_rate)
        self._output_processor = AudioSocketOutput(writer, sample_rate)

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
                logger.warning(f"AudioSocketTransport: Expected UUID (0x01) but got 0x{msg_type:02x}")
            
            # Now that UUID is read, tell input processor it can start reading audio
            self._input_processor.start_reading()
            
            logger.info("AudioSocketTransport: Initialized successfully (8kHz ↔ 16kHz conversion enabled)")
        
        except Exception as e:
            logger.error(f"AudioSocketTransport: Initialization failed: {e}", exc_info=True)
            raise

    async def stop(self):
        """Cleanup transport resources"""
        logger.debug("AudioSocketTransport: Stopping")
        
        if self._input_processor._read_task and not self._input_processor._read_task.done():
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