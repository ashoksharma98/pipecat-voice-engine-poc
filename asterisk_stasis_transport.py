# # import socket
# # import asyncio
# # import struct
# # from typing import Optional
# from pipecat.transports.base_input import BaseInputTransport
# # from pipecat.transports.base_output import BaseOutputTransport
# # from pipecat.processors.frame_processor import FrameProcessor
# # from pipecat.frames.frames import StartFrame, EndFrame, Frame, InputAudioRawFrame, OutputAudioRawFrame, AudioRawFrame
# # from pipecat.processors.frame_processor import FrameDirection
# from pipecat.transports.base_transport import TransportParams, BaseTransport


# class AudioSocketTransportParams(TransportParams):
#     audio_in_sample_rate: int = 16000
#     audio_out_sample_rate: int = 16000


# # # class AudioSocketInputTransport(BaseInputTransport):
    
# # #     def __init__(self, params: TransportParams, **kwargs):
# # #         super().__init__(params=params)
# # #         self.params = params

# # #     async def start(self, frame: StartFrame):
# # #         print("STARTING AUDIO ====> ", frame)
# # #         await super().start(frame)

# # #     async def stop(self, frame: EndFrame):
# # #         await super().stop(frame)


# # class AudioSocketInputTransport(BaseInputTransport):
    
# #     def __init__(self, host: str, port: int, shared_connection_info: dict, params: TransportParams):
# #         super().__init__(params=params)
# #         self.host = host
# #         self.port = port
# #         self.sock = None
# #         self.connection_info = shared_connection_info

# #     async def start(self, frame: StartFrame):
# #         # 1. Setup UDP Socket
# #         # AF_INET = IPv4, SOCK_DGRAM = UDP
# #         self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
# #         self.sock.bind((self.host, self.port))
        
# #         # 2. IMPORTANT: Set non-blocking so we don't freeze the whole AI
# #         self.sock.setblocking(False)
        
# #         print(f"--- Listening for Asterisk RTP on {self.host}:{self.port} ---")
# #         # Note: We don't necessarily need to pass StartFrame to super().start() here 
# #         # unless your specific Pipecat version requires it.
        
# #     async def input(self):
# #         # 3. Signal the pipeline that we are ready
# #         yield StartFrame()
        
# #         loop = asyncio.get_event_loop()
        
# #         while True:
# #             try:
# #                 # 4. Read UDP Packet (Async)
# #                 # We use loop.sock_recvfrom to wait for data without blocking other tasks
# #                 # 4096 is a safe buffer size for RTP (usually ~172 bytes for 20ms of audio)
# #                 data, addr = await loop.sock_recvfrom(self.sock, 4096)

# #                 ## --- THE MAGIC HAPPENS HERE ---
# #                 # If we haven't learned the address yet, save it!
# #                 if self.connection_info["remote_addr"] is None:
# #                     self.connection_info["remote_addr"] = addr
# #                     print(f"--- LATCHED! Connected to Asterisk at {addr} ---")
# #                 # ------------------------------

# #                 # Strip 12-byte RTP header and yield audio
# #                 yield InputAudioRawFrame(audio=data[12:], num_channels=1, sample_rate=16000)

# #             except asyncio.CancelledError:
# #                 break
# #             except Exception as e:
# #                 print(f"Error reading socket: {e}")
# #                 break
        
# #         yield EndFrame()

# #     async def stop(self):
# #         if self.sock:
# #             self.sock.close()
# #         print("--- Socket Closed ---")


# # # class AudioSocketOutputTransport(BaseOutputTransport):
    
# # #     def __init__(self, params: TransportParams, **kwargs):
# # #         super().__init__(params=params)
# # #         self.params = params

# # #     async def start(self, frame: StartFrame):
# # #         await super().start(frame)

# # #     async def stop(self, frame: EndFrame):
# # #         await super().stop(frame)


# # class AudioSocketOutputTransport(BaseOutputTransport):
    
# #     def __init__(self, shared_connection_info: dict, params: TransportParams):
# #         super().__init__(params=params)
# #         self.connection_info = shared_connection_info
        
# #         # RTP State Variables (CRITICAL for Asterisk)
# #         self.sequence_number = 0
# #         self.timestamp = 0
# #         self.ssrc = 123456789  # Random ID for this stream
        
# #         # Socket setup
# #         self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# #     async def start(self, frame):
# #         print(f"--- Output Transport Ready. Sending to {self.connection_info.get('remote_addr')} ---")

# #     async def output(self, frame):
# #         if not isinstance(frame, AudioRawFrame):
# #             return

# #         # Check if we know where to send yet
# #         target = self.connection_info["remote_addr"]
# #         if target is None:
# #             # Asterisk hasn't spoken to us yet, so we can't speak back.
# #             # We drop this frame (or you could buffer it).
# #             return

# #         payload = frame.audio

# #         if not payload:
# #             return
        
# #         # Build RTP Header (Standard Asterisk format)
# #         header = struct.pack('!BBHII', 0x80, 96, self.sequence_number, self.timestamp, self.ssrc)
        
# #         try:
# #             self.sock.sendto(header + payload, target)
            
# #             # Update counters
# #             self.sequence_number += 1
# #             self.timestamp += len(payload) // 2
# #         except Exception as e:
# #             print(f"Error sending RTP: {e}")

# #     async def stop(self):
# #         self.sock.close()
# #         print("--- Output Socket Closed ---")


# # class AudioSocketTransport(BaseTransport):

# #     def __init__(
# #         self,
# #         params: AudioSocketTransportParams,
# #         host: str,
# #         port: int,
# #         shared_connection_info: dict,
# #         input_name: Optional[str] = None,
# #         output_name: Optional[str] = None,
# #     ):
# #         super().__init__(input_name=input_name, output_name=output_name)

# #         self._params = params
# #         self._input = AudioSocketInputTransport(
# #             host, port, shared_connection_info, self._params
# #         )
# #         self._output = AudioSocketOutputTransport(
# #             shared_connection_info, self._params
# #         )

# #     def input(self) -> AudioSocketInputTransport:
# #         """Get the input transport processor.

# #         Returns:
# #             The WebSocket input transport instance.
# #         """
# #         return self._input

# #     def output(self) -> AudioSocketOutputTransport:
# #         """Get the output transport processor.

# #         Returns:
# #             The WebSocket output transport instance.
# #         """
# #         return self._output



# #################################################################
# #################################################################

# # import asyncio
# # import socket
# # import struct
# # import logging
# # from typing import Optional

# # from pipecat.transports.base_transport import BaseTransport, TransportParams
# # from pipecat.transports.base_input import BaseInputTransport
# # from pipecat.transports.base_output import BaseOutputTransport
# # from pipecat.frames.frames import (
# #     AudioRawFrame, 
# #     InputAudioRawFrame, 
# #     OutputAudioRawFrame, 
# #     StartFrame, 
# #     EndFrame
# # )

# # # Shared dictionary to pass the active TCP socket from Input to Output
# # connection_state = {"client_socket": None}

# # # --- Helper to read exact bytes from TCP stream ---
# # async def read_exactly(loop, sock, n):
# #     data = b''
# #     while len(data) < n:
# #         try:
# #             packet = await loop.sock_recv(sock, n - len(data))
# #             if not packet:
# #                 raise ConnectionResetError("Socket closed during read")
# #             data += packet
# #         except Exception as e:
# #             raise e
# #     return data

# # # ==========================================
# # # 1. TCP Input Transport ( The Server )
# # # ==========================================
# # class TcpAudioSocketInputTransport(BaseInputTransport):
    
# #     def __init__(self, host: str, port: int, shared_state: dict, params: TransportParams):
# #         super().__init__(params=params)
# #         self.host = host
# #         self.port = port
# #         self.state = shared_state
# #         self.server_sock = None

# #     async def start(self, frame: StartFrame):
# #         # Create TCP Server Socket
# #         self.server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# #         self.server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
# #         self.server_sock.bind((self.host, self.port))
# #         self.server_sock.listen(1)
# #         self.server_sock.setblocking(False)
# #         print(f"--- 🎧 TCP Server listening on {self.host}:{self.port} ---")

# #     async def input(self):
# #         yield StartFrame()
        
# #         loop = asyncio.get_event_loop()
        
# #         # 1. Wait for Asterisk to connect
# #         print("--- Waiting for Asterisk to connect... ---")
# #         client, addr = await loop.sock_accept(self.server_sock)
# #         client.setblocking(False)
# #         print(f"--- 📞 Asterisk Connected from {addr} ---")
        
# #         # 2. Share the connection with Output Transport
# #         self.state["client_socket"] = client

# #         # 3. Read the initial UUID (AudioSocket standard: 16 bytes UUID)
# #         # Asterisk sends a UUID immediately upon connection. We just consume it.
# #         try:
# #             uuid_data = await read_exactly(loop, client, 16)
# #             print(f"--- Call UUID: {uuid_data.hex()} ---")
# #         except Exception as e:
# #             print(f"Handshake failed: {e}")
# #             return

# #         # 4. Main Audio Loop
# #         while True:
# #             try:
# #                 # STEP A: Read 3-byte Header (Type + Length)
# #                 # Byte 1: Type
# #                 # Byte 2-3: Length (Big Endian)
# #                 header = await read_exactly(loop, client, 3)
                
# #                 msg_type = header[0]
# #                 msg_len = struct.unpack('!H', header[1:3])[0]

# #                 # STEP B: Read Payload
# #                 payload = await read_exactly(loop, client, msg_len)

# #                 # STEP C: Process based on Type
# #                 if msg_type == 0x10: # 0x10 = Signed Linear 16-bit Audio
# #                     yield InputAudioRawFrame(
# #                         audio=payload, 
# #                         num_channels=1, 
# #                         sample_rate=16000
# #                     )
# #                 elif msg_type == 0x00: # 0x00 = Hangup
# #                     print("--- Asterisk Hung Up ---")
# #                     break
                    
# #             except (ConnectionResetError, BrokenPipeError):
# #                 print("--- Connection Closed ---")
# #                 break
# #             except Exception as e:
# #                 print(f"Error reading TCP: {e}")
# #                 break
        
# #         # Cleanup
# #         if self.state["client_socket"]:
# #             self.state["client_socket"].close()
# #             self.state["client_socket"] = None
            
# #         yield EndFrame()

# #     async def stop(self):
# #         if self.server_sock:
# #             self.server_sock.close()

# # # ==========================================
# # # 2. TCP Output Transport ( Writes to Socket )
# # # ==========================================
# # class TcpAudioSocketOutputTransport(BaseOutputTransport):
    
# #     def __init__(self, shared_state: dict, params: TransportParams):
# #         super().__init__(params=params)
# #         self.state = shared_state

# #     async def start(self, frame):
# #         await super().start(frame)

# #     async def output(self, frame):
# #         # Only handle audio, ignore text/LLM frames
# #         if not isinstance(frame, AudioRawFrame):
# #             return

# #         client = self.state["client_socket"]
        
# #         # Can't send if no one is connected
# #         if client is None:
# #             return

# #         payload = frame.audio
# #         if not payload:
# #             return

# #         # Construct AudioSocket Message
# #         # Type: 0x10 (Audio)
# #         # Length: Big Endian Unsigned Short (!H)
# #         header = struct.pack('!BH', 0x10, len(payload))
        
# #         try:
# #             # Send Header + Audio
# #             client.sendall(header + payload)
# #         except Exception as e:
# #             print(f"Error sending audio: {e}")

# #     async def stop(self, frame):
# #         await super().stop(frame)

# # # ==========================================
# # # 3. The Main Wrapper
# # # ==========================================
# # class AudioSocketTransport(BaseTransport):
# #     def __init__(self, host: str, port: int, params: TransportParams):
# #         super().__init__()
# #         self._params = params
# #         # This shared dictionary links the two transports
# #         self._shared_state = {"client_socket": None}
        
# #         self._input = TcpAudioSocketInputTransport(host, port, self._shared_state, params)
# #         self._output = TcpAudioSocketOutputTransport(self._shared_state, params)

# #     def input(self): return self._input
# #     def output(self): return self._output



# #######################################################
# #######################################################
# #######################################################

# import socket
# import asyncio
# from pipecat.processors.frame_processor import FrameProcessor
# from pipecat.frames.frames import StartFrame

# class AudioProbeProcessor(FrameProcessor):
#     async def process_frame(self, frame):
#         if isinstance(frame, StartFrame):
#             print(
#                 f"[AUDIO] bytes={len(frame.audio)} "
#                 f"sr={frame.sample_rate} "
#                 f"ch={frame.num_channels}"
#             )
#         return frame

# class AsteriskRTPInputTransport(BaseInputTransport):

#     def __init__(
#         self,
#         params: AudioSocketTransportParams,
#         host: str,
#         port: int,
#         sample_rate: int = 16000,
#         channels: int = 1,
#     ):
#         self.host = host
#         self.port = port
#         self.sample_rate = sample_rate
#         self.channels = channels

#         self._running = False
#         self._task = None
#         self._sock = None

#     async def start(self):
#         self._running = True

#         self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#         self._sock.bind((self.host, self.port))
#         self._sock.listen(1)
#         self._sock.setblocking(False)

#         self._task = asyncio.create_task(self.process_frame())

#     async def process_frame(self, frame):
#         if isinstance(frame, StartFrame):
#             print(
#                 f"[AUDIO] bytes={len(frame.audio)} "
#                 f"sr={frame.sample_rate} "
#                 f"ch={frame.num_channels}"
#             )
#         return self.push_frame(frame)

#     async def stop(self):
#         self._running = False
#         self._task.cancel()


# class AudioSocketTransport(BaseTransport):
#     def __init__(self, host: str, port: int, params: TransportParams):
#         super().__init__()
#         self._params = params
#         # This shared dictionary links the two transports
#         self._shared_state = {"client_socket": None}
        
#         self._input = AsteriskRTPInputTransport(self._params, host, port)
#         self._output = None

#     def input(self): return self._input
#     def output(self): return self._output






import asyncio
import struct
import logging
import numpy as np
from scipy import signal
from typing import Optional

from pipecat.processors.frame_processor import FrameProcessor, FrameDirection
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.frames.frames import (
    InputAudioRawFrame,
    OutputAudioRawFrame,
    StartFrame,
    EndFrame,
    CancelFrame,
)

logger = logging.getLogger("audiosocket_transport")
logger.setLevel(logging.DEBUG)


MSG_UUID = 0x01
MSG_AUDIO = 0x10
MSG_HANGUP = 0x00
CHUNK_SIZE = 320  # 20ms at 8kHz = 160 samples * 1 byte = 160 bytes for SLIN


class AudioSocketTransportParams(TransportParams):
    audio_in_sample_rate: int = 16000
    audio_out_sample_rate: int = 16000


class AudioSocketInput(FrameProcessor):
    def __init__(
        self,
        reader: asyncio.StreamReader,
        sample_rate: int = 16000,
        asterisk_sample_rate: int = 8000,
    ):
        super().__init__()
        self._reader = reader
        self._sample_rate = sample_rate          # Target rate (e.g., 16000 for STT)
        self._asterisk_sample_rate = asterisk_sample_rate # Source rate (8000 or 16000)
        self._running = False
        self._read_task: Optional[asyncio.Task] = None
        self._uuid_read = False

    def _resample_audio(self, audio_bytes: bytes) -> bytes:
        """Resample audio from Asterisk rate to Pipecat rate if needed"""
        if self._asterisk_sample_rate == self._sample_rate:
            return audio_bytes

        # Convert bytes to numpy array (int16)
        audio_data = np.frombuffer(audio_bytes, dtype=np.int16)
        
        # Calculate target number of samples
        num_samples = int(len(audio_data) * self._sample_rate / self._asterisk_sample_rate)
        
        # Resample
        resampled = signal.resample(audio_data, num_samples)
        return np.clip(resampled, -32768, 32767).astype(np.int16).tobytes()

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if isinstance(frame, StartFrame):
            self._running = True
            # We wait for transport.start() to verify UUID before triggering read loop
            if self._uuid_read:
                self._start_read_task()
        elif isinstance(frame, (EndFrame, CancelFrame)):
            self._running = False
            if self._read_task:
                self._read_task.cancel()

    def start_reading(self):
        """Called by transport after UUID is read"""
        self._uuid_read = True
        if self._running and not self._read_task:
            self._start_read_task()

    def _start_read_task(self):
        self._read_task = asyncio.create_task(self._read_loop())
        logger.info("AudioSocketInput: Read loop started")

    async def _read_loop(self):
        try:
            while self._running:
                try:
                    header = await self._reader.readexactly(3)
                    msg_type = header[0]
                    length = struct.unpack(">H", header[1:])[0]
                    payload = await self._reader.readexactly(length)

                    if msg_type == MSG_AUDIO:
                        # Resample if Asterisk rate != STT rate
                        processed_audio = self._resample_audio(payload)
                        
                        frame = InputAudioRawFrame(
                            audio=processed_audio, 
                            sample_rate=self._sample_rate, 
                            num_channels=1
                        )
                        await self.push_frame(frame, FrameDirection.DOWNSTREAM)

                    elif msg_type == MSG_HANGUP:
                        logger.info("Received HANGUP from Asterisk")
                        self._running = False
                        await self.push_frame(EndFrame(), FrameDirection.DOWNSTREAM)
                        break

                except asyncio.IncompleteReadError:
                    break
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in read loop: {e}")
            self._running = False
            await self.push_frame(EndFrame(), FrameDirection.DOWNSTREAM)


class AudioSocketOutput(FrameProcessor):
    def __init__(
        self,
        writer: asyncio.StreamWriter,
        input_sample_rate: int = 16000,
        asterisk_sample_rate: int = 8000,
    ):
        super().__init__()
        self._writer = writer
        self._input_sample_rate = input_sample_rate   # From TTS
        self._output_sample_rate = asterisk_sample_rate # To Asterisk
        
        # Calculate chunk size dynamically (20ms)
        # 16kHz: 16000 * 0.02 * 2 bytes = 640 bytes
        # 8kHz:  8000 * 0.02 * 2 bytes = 320 bytes
        self._chunk_size = int(self._output_sample_rate * 0.02 * 2)

    def _downsample_audio(self, audio_bytes: bytes) -> bytes:
        if self._input_sample_rate == self._output_sample_rate:
            return audio_bytes

        audio_data = np.frombuffer(audio_bytes, dtype=np.int16)
        num_samples = int(len(audio_data) * self._output_sample_rate / self._input_sample_rate)
        resampled = signal.resample(audio_data, num_samples)
        return np.clip(resampled, -32768, 32767).astype(np.int16).tobytes()

    async def _send_audio_chunks(self, audio_data: bytes):
        try:
            for i in range(0, len(audio_data), self._chunk_size):
                chunk = audio_data[i : i + self._chunk_size]
                
                # Pad if too small
                if len(chunk) < self._chunk_size:
                    chunk = chunk + (b"\x00" * (self._chunk_size - len(chunk)))

                # AudioSocket Packet: Type (0x10) | Length (2B) | Payload
                header = struct.pack("B", MSG_AUDIO) + struct.pack(">H", len(chunk))
                self._writer.write(header + chunk)
                await self._writer.drain()
                
                # Pace the audio (20ms)
                await asyncio.sleep(0.02)
        except Exception as e:
            logger.error(f"Error sending audio: {e}")

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)

        if isinstance(frame, OutputAudioRawFrame):
            audio = frame.audio
            if audio:
                # Resample to match Asterisk expectation
                out_audio = self._downsample_audio(audio)
                await self._send_audio_chunks(out_audio)

        elif isinstance(frame, EndFrame):
            try:
                msg = struct.pack("B", MSG_HANGUP) + struct.pack(">H", 0)
                self._writer.write(msg)
                await self._writer.drain()
            except Exception:
                pass


class AudioSocketTransport(BaseTransport):
    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        sample_rate: int = 16000,
        asterisk_sample_rate: int = 16000, # Set to 16000 for slin16, 8000 otherwise
    ):
        super().__init__()
        self._reader = reader
        self._writer = writer
        
        self._input_processor = AudioSocketInput(
            reader=reader, 
            sample_rate=sample_rate,
            asterisk_sample_rate=asterisk_sample_rate
        )
        self._output_processor = AudioSocketOutput(
            writer=writer, 
            input_sample_rate=sample_rate,
            asterisk_sample_rate=asterisk_sample_rate
        )

    def input(self) -> FrameProcessor:
        return self._input_processor

    def output(self) -> FrameProcessor:
        return self._output_processor

    async def start(self):
        """Perform handshake and start reading"""
        try:
            logger.info("AudioSocketTransport: Waiting for UUID...")
            
            # Added timeout to prevent hanging forever if Asterisk is silent
            try:
                header = await asyncio.wait_for(self._reader.readexactly(3), timeout=3.0)
            except asyncio.TimeoutError:
                logger.warning("AudioSocketTransport: Timed out waiting for UUID! Starting read loop anyway.")
                self._input_processor.start_reading()
                return

            msg_type = header[0]
            length = struct.unpack(">H", header[1:])[0]

            if msg_type == MSG_UUID:
                uuid_data = await self._reader.readexactly(length)
                logger.info(f"AudioSocketTransport: Connected UUID: {uuid_data.hex()}")
            else:
                # If we got audio immediately instead of UUID, handle it
                logger.warning(f"AudioSocketTransport: Unexpected initial packet 0x{msg_type:02x}")
                # We consumed the header, so the read loop might be offset if we don't handle this carefully.
                # Ideally, we should pass this buffer to the input processor, but for simplicity:
                if length > 0:
                    await self._reader.readexactly(length) # Drain unexpected payload

            self._input_processor.start_reading()

        except Exception as e:
            logger.error(f"AudioSocketTransport start failed: {e}", exc_info=True)
