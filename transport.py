"""
Minimal Asterisk WebSocket Transport for Pipecat
Handles s16le mono PCM at 16kHz, 20ms packets (640 bytes)
"""

import asyncio
from typing import Optional
from loguru import logger

from pipecat.frames.frames import (
    Frame,
    InputAudioRawFrame,     # This exists - it's SystemFrame + AudioRawFrame
    OutputAudioRawFrame,    # This exists - it's DataFrame + AudioRawFrame
    TTSAudioRawFrame,       # Alternative for TTS output
    CancelFrame,
    EndFrame,
    StartFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor
from pipecat.transports.base_transport import BaseTransport, TransportParams


class AsteriskTransportParams(TransportParams):
    """Configuration for Asterisk transport."""
    
    def __init__(
        self,
        call_sid: Optional[str] = None,
        audio_in_sample_rate: int = 16000,
        audio_out_sample_rate: int = 16000,
        **kwargs
    ):
        super().__init__(
            audio_in_enabled=True,
            audio_out_enabled=True,
            audio_in_sample_rate=audio_in_sample_rate,
            audio_out_sample_rate=audio_out_sample_rate,
            **kwargs
        )
        self.call_sid = call_sid


class AsteriskTransport(BaseTransport):
    """
    Transport for Asterisk media WebSockets.
    
    Expects:
    - Raw PCM s16le mono at 16kHz
    - 20ms packets (640 bytes each)
    - Full duplex audio streaming
    """
    
    def __init__(self, websocket, params: AsteriskTransportParams):
        super().__init__(params)
        self._websocket = websocket
        self._params = params
        self._receive_task: Optional[asyncio.Task] = None
        self._send_task: Optional[asyncio.Task] = None
        self._audio_out_queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        
        # Expected frame size: 16kHz * 0.02s * 2 bytes = 640 bytes
        self._expected_frame_size = int(
            params.audio_in_sample_rate * 0.02 * 2
        )
        
        logger.info(
            f"AsteriskTransport initialized for call_sid={params.call_sid}, "
            f"expected_frame_size={self._expected_frame_size} bytes"
        )

    async def start(self, frame: StartFrame):
        """Start the transport and begin audio streaming."""
        if self._running:
            return
            
        self._running = True
        
        # Start receive and send tasks
        self._receive_task = asyncio.create_task(self._receive_audio())
        self._send_task = asyncio.create_task(self._send_audio())
        
        # Push StartFrame downstream to initialize the pipeline
        await self.push_frame(frame, FrameDirection.DOWNSTREAM)
        
        logger.info(f"AsteriskTransport started for call_sid={self._params.call_sid}")

    async def stop(self):
        """Stop the transport and cleanup."""
        if not self._running:
            return
            
        self._running = False
        
        # Cancel tasks
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
                
        if self._send_task:
            self._send_task.cancel()
            try:
                await self._send_task
            except asyncio.CancelledError:
                pass
        
        # Close WebSocket
        try:
            await self._websocket.close()
        except Exception as e:
            logger.warning(f"Error closing websocket: {e}")
            
        logger.info(f"AsteriskTransport stopped for call_sid={self._params.call_sid}")

    async def _receive_audio(self):
        """
        Continuously receive raw PCM audio from Asterisk WebSocket
        and push InputAudioRawFrame into the pipeline.
        """
        try:
            while self._running:
                # Receive raw PCM bytes from Asterisk
                data = await self._websocket.receive_bytes()
                
                if not data:
                    logger.warning("Received empty audio packet from Asterisk")
                    continue
                
                # Validate frame size (should be 640 bytes for 20ms at 16kHz)
                if len(data) != self._expected_frame_size:
                    logger.debug(
                        f"Unexpected frame size: {len(data)} bytes "
                        f"(expected {self._expected_frame_size})"
                    )
                
                # Create Pipecat audio frame
                # InputAudioRawFrame is a SystemFrame, so it flows through the pipeline
                frame = InputAudioRawFrame(
                    audio=data,
                    sample_rate=self._params.audio_in_sample_rate,
                    num_channels=1
                )
                
                # Push to pipeline for STT processing
                await self.push_frame(frame, FrameDirection.DOWNSTREAM)
                
        except asyncio.CancelledError:
            logger.info("Audio receive task cancelled")
        except Exception as e:
            logger.error(f"Error receiving audio from Asterisk: {e}")
            await self._handle_error(e)

    async def _send_audio(self):
        """
        Continuously send audio from the queue back to Asterisk WebSocket.
        """
        try:
            while self._running:
                # Wait for audio from TTS/pipeline
                audio_data = await self._audio_out_queue.get()
                
                if audio_data is None:
                    break
                
                # Send raw PCM bytes to Asterisk
                await self._websocket.send_bytes(audio_data)
                
        except asyncio.CancelledError:
            logger.info("Audio send task cancelled")
        except Exception as e:
            logger.error(f"Error sending audio to Asterisk: {e}")
            await self._handle_error(e)

    async def send_audio(self, frame: OutputAudioRawFrame):
        """
        Queue audio frame to be sent to Asterisk.
        Called by the pipeline when TTS generates audio.
        """
        await self._audio_out_queue.put(frame.audio)

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """
        Process frames flowing through the transport.
        """
        await super().process_frame(frame, direction)
        
        # Handle outbound audio frames (from TTS)
        # Both OutputAudioRawFrame and TTSAudioRawFrame should be sent
        if isinstance(frame, (OutputAudioRawFrame, TTSAudioRawFrame)):
            await self.send_audio(frame)
        
        # Handle control frames
        elif isinstance(frame, StartFrame):
            await self.start(frame)
        
        elif isinstance(frame, (EndFrame, CancelFrame)):
            await self.stop()
            await self.push_frame(frame, direction)
        
        else:
            # Pass other frames through
            await self.push_frame(frame, direction)

    async def _handle_error(self, error: Exception):
        """Handle transport errors."""
        logger.error(f"AsteriskTransport error: {error}")
        self._running = False
        
        # Push error/cancel frame to stop pipeline
        await self.push_frame(CancelFrame(), FrameDirection.DOWNSTREAM)

    def input(self) -> FrameProcessor:
        """Return input processor for receiving audio from Asterisk."""
        return self

    def output(self) -> FrameProcessor:
        """Return output processor for sending audio to Asterisk."""
        return self
