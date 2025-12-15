import asyncio
import struct
import time
import logging
from typing import Optional

from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.transports.base_input import BaseInputTransport
from pipecat.transports.base_output import BaseOutputTransport
from pipecat.frames.frames import (
    StartFrame,
    EndFrame,
    CancelFrame,
    OutputAudioRawFrame,
)

from serializer import AudioSocketFrameSerializer

logger = logging.getLogger(__name__)

MSG_UUID = 0x01
MSG_AUDIO = 0x10

# ========================= PARAMS =========================

class AudioSocketTransportParams(TransportParams):
    serializer: AudioSocketFrameSerializer
    audio_in_sample_rate: int = 16000
    audio_out_sample_rate: int = 16000

# ========================= INPUT =========================

class AudioSocketInputTransport(BaseInputTransport):
    def __init__(
        self,
        transport,
        reader: asyncio.StreamReader,
        params: AudioSocketTransportParams,
        **kwargs,
    ):
        super().__init__(params, **kwargs)
        self._transport = transport
        self._reader = reader
        self._params = params
        self._task: Optional[asyncio.Task] = None

    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self._params.serializer.setup(frame)

        # Read UUID (mandatory first packet from Asterisk)
        try:
            header = await self._reader.readexactly(3)
            msg_type = header[0]
            length = struct.unpack(">H", header[1:])[0]
            
            if msg_type == MSG_UUID:
                uuid_data = await self._reader.readexactly(length)
                logger.info(f"AudioSocket UUID received: {len(uuid_data)} bytes")
            else:
                logger.warning(f"Expected UUID, got type {msg_type}")
        except Exception as e:
            logger.error(f"Failed to read UUID: {e}")
            raise

        # Start audio receive loop
        self._task = self.create_task(self._receive_audio())
        await self.set_transport_ready(frame)

    async def stop(self, frame: EndFrame):
        await super().stop(frame)
        if self._task:
            await self.cancel_task(self._task)

    async def cancel(self, frame: CancelFrame):
        await super().cancel(frame)
        if self._task:
            await self.cancel_task(self._task)

    async def _receive_audio(self):
        """Continuously read audio packets from Asterisk"""
        logger.info("AudioSocket input loop started")

        try:
            while True:
                # Read AudioSocket header: [type:1][length:2]
                header = await self._reader.readexactly(3)
                msg_type = header[0]
                length = struct.unpack(">H", header[1:])[0]
                
                # Read payload
                payload = await self._reader.readexactly(length)

                # Only process audio packets
                if msg_type != MSG_AUDIO:
                    logger.debug(f"Skipping non-audio packet: type={msg_type}")
                    continue

                # Deserialize and push to pipeline
                frame = await self._params.serializer.deserialize(payload)
                if frame:
                    await self.push_audio_frame(frame)

        except asyncio.CancelledError:
            logger.info("AudioSocket input loop cancelled")
        except Exception as exc:
            logger.error(f"AudioSocket input error: {exc}", exc_info=True)

# ========================= OUTPUT =========================

class AudioSocketOutputTransport(BaseOutputTransport):
    def __init__(
        self,
        transport,
        writer: asyncio.StreamWriter,
        params: AudioSocketTransportParams,
        **kwargs,
    ):
        super().__init__(params, **kwargs)
        self._transport = transport
        self._writer = writer
        self._params = params
        
        # Pacing: send every 20ms (standard for VoIP)
        self._send_interval = 0.02
        self._next_send_time = time.monotonic()

    async def start(self, frame: StartFrame):
        await super().start(frame)
        await self._params.serializer.setup(frame)
        await self.set_transport_ready(frame)

    async def stop(self, frame: EndFrame):
        await super().stop(frame)
        try:
            if not self._writer.is_closing():
                self._writer.close()
                await self._writer.wait_closed()
        except Exception as e:
            logger.error(f"Error closing writer: {e}")

    async def cancel(self, frame: CancelFrame):
        await super().cancel(frame)
        try:
            if not self._writer.is_closing():
                self._writer.close()
                await self._writer.wait_closed()
        except Exception as e:
            logger.error(f"Error closing writer: {e}")

    async def write_audio_frame(self, frame: OutputAudioRawFrame):
        """Send audio frame to Asterisk"""
        try:
            # Serialize to AudioSocket format
            packet = await self._params.serializer.serialize(frame)
            if not packet:
                return False

            # Send packet
            self._writer.write(packet)
            await self._writer.drain()
            
            # Pace sending to avoid overwhelming Asterisk
            await self._sleep_paced()
            
            return True

        except Exception as e:
            logger.error(f"Error writing audio frame: {e}")
            return False

    async def _sleep_paced(self):
        """Maintain consistent 20ms pacing"""
        now = time.monotonic()
        sleep_time = max(0, self._next_send_time - now)
        await asyncio.sleep(sleep_time)
        self._next_send_time = max(
            self._next_send_time + self._send_interval,
            time.monotonic(),
        )

# ========================= MAIN TRANSPORT =========================

class AudioSocketTransport(BaseTransport):
    """
    Pipecat transport for Asterisk AudioSocket protocol.
    
    Usage:
        transport = AudioSocketTransport(
            reader=reader,
            writer=writer,
            asterisk_sample_rate=16000  # 8000 for slin, 16000 for slin16
        )
    """

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        asterisk_sample_rate: int = 16000,  # Match your Asterisk config!
    ):
        super().__init__()

        # Create serializer
        serializer = AudioSocketFrameSerializer(
            AudioSocketFrameSerializer.InputParams(
                asterisk_sample_rate=asterisk_sample_rate,
                sample_rate=asterisk_sample_rate,  # Use same rate throughout
            )
        )

        # Create params
        params = AudioSocketTransportParams(
            serializer=serializer,
            audio_in_sample_rate=asterisk_sample_rate,
            audio_out_sample_rate=asterisk_sample_rate,
        )

        # Create input/output transports
        self._input = AudioSocketInputTransport(
            self, reader, params, name="audiosocket_in"
        )
        self._output = AudioSocketOutputTransport(
            self, writer, params, name="audiosocket_out"
        )

    def input(self):
        return self._input

    def output(self):
        return self._output
