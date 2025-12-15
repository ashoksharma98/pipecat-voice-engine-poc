# audiosocket_transport.py
"""
Production-grade AudioSocketTransport for Asterisk external_media (slin, 8k PCM S16LE)
Designed for pipeline sample_rate=16000 (Cartesia STT/TTS), converting to/from 8000 for Asterisk.
"""

import asyncio
import struct
import logging
import io
import wave
from typing import Optional
from math import gcd
import time

import numpy as np
from scipy.signal import resample_poly, butter, filtfilt, iirfilter, sosfiltfilt

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

# AudioSocket protocol constants
MSG_UUID = 0x01
MSG_AUDIO = 0x10
MSG_HANGUP = 0x00

DEFAULT_FRAME_MS = 20  # 20 ms frames
BYTES_PER_SAMPLE = 2  # 16-bit

# TELEPHONE DSP TUNING DEFAULTS
TEL_BAND_LOW = 120.0       # Hz (was 300) — widen to recover some lower voice body
TEL_BAND_HIGH = 4000.0     # Hz (was 3400) — widen to keep higher intelligibility
PRE_EMPHASIS_COEF = 0.92   # (was 0.97) — slightly weaker pre-emphasis
TARGET_DBFS = -18.0        # (was -20) — slightly louder target
HIGH_SHELF_FREQ = 3000.0   # center freq for gentle shelf boost
HIGH_SHELF_GAIN_DB = 3.0   # mild boost in dB to restore sibilance


def compute_chunk_bytes(sample_rate: int, ms: int = DEFAULT_FRAME_MS, bytes_per_sample: int = BYTES_PER_SAMPLE) -> int:
    samples = int(sample_rate * (ms / 1000.0))
    return samples * bytes_per_sample


def _ensure_int16_le(audio_bytes: bytes) -> np.ndarray:
    """Interpret raw bytes as little-endian signed 16-bit integers."""
    if not audio_bytes:
        return np.array([], dtype="<i2")
    return np.frombuffer(audio_bytes, dtype="<i2")


def _strip_wav_header_if_present(audio: bytes) -> bytes:
    """If audio looks like a RIFF/WAVE, attempt to extract raw pcm data after 'data' chunk."""
    if len(audio) >= 12 and audio[0:4] == b"RIFF" and b"WAVE" in audio[8:12]:
        idx = audio.find(b"data")
        if idx != -1 and idx + 8 < len(audio):
            data_start = idx + 8
            logger.debug("WAV header detected and stripped. Total bytes before strip: %d", len(audio))
            return audio[data_start:]
        logger.debug("WAV header detected but 'data' chunk not found or truncated.")
    return audio


def _float_from_int16_le(arr: np.ndarray) -> np.ndarray:
    return (arr.astype(np.float32) / 32768.0)


def _int16_le_from_float(arr: np.ndarray) -> np.ndarray:
    clipped = np.clip(arr, -1.0, 1.0)
    # Multiply by 32767 to keep symmetric range and cast to little-endian int16
    return (clipped * 32767.0).astype("<i2")


def _add_triangular_dither(float_samples: np.ndarray, scale: float = 1.0 / 32768.0) -> np.ndarray:
    # TPDF: difference of two uniform(-0.5,0.5)
    u1 = np.random.uniform(-0.5, 0.5, size=float_samples.shape)
    u2 = np.random.uniform(-0.5, 0.5, size=float_samples.shape)
    return float_samples + (u1 + u2) * scale


def _rms_normalize(samples: np.ndarray, target_dbfs: float = -20.0, eps: float = 1e-9) -> np.ndarray:
    # target_dbfs is negative (e.g., -20 dBFS)
    rms = np.sqrt(np.mean(samples ** 2) + eps)
    target_rms = 10 ** (target_dbfs / 20.0)
    if rms <= 0:
        return samples
    return samples * (target_rms / rms)


def _telephone_bandpass(samples: np.ndarray, fs: int, low: float = TEL_BAND_LOW, high: float = TEL_BAND_HIGH, order: int = 4) -> np.ndarray:
    """Zero-phase bandpass using SOS (Butterworth) with wider defaults."""
    if samples.size == 0:
        return samples
    nyq = 0.5 * fs
    low_n = max(low / nyq, 1e-6)
    high_n = min(high / nyq, 0.999999)
    if low_n >= high_n:
        return samples
    # use sos for numerical stability
    sos = iirfilter(order, [low_n, high_n], btype='band', ftype='butter', output='sos')
    try:
        return sosfiltfilt(sos, samples)
    except Exception:
        from scipy.signal import lfilter
        # fallback
        return lfilter(sos[:, :3], sos[:, 3:], samples)  # best-effort fallback (rare)


def _high_shelf(samples: np.ndarray, fs: int, freq: float = HIGH_SHELF_FREQ, gain_db: float = HIGH_SHELF_GAIN_DB) -> np.ndarray:
    """
    Simple second-order shelving filter (approximate) implemented using bilinear transform.
    Designed for small gains (+/- a few dB). If complex, skip it.
    """
    if samples.size == 0 or abs(gain_db) < 0.1:
        return samples

    # convert gain to linear
    A = 10 ** (gain_db / 40.0)  # shelving uses sqrt in some designs
    w0 = 2.0 * np.pi * freq / fs
    # choose Q for shelf slope (broad)
    Q = 0.7071
    alpha = np.sin(w0) / (2 * Q)

    # biquad high-shelf (Cookbook)
    cosw0 = np.cos(w0)
    b0 =    A*( (A+1) + (A-1)*cosw0 + 2*np.sqrt(A)*alpha )
    b1 = -2*A*( (A-1) + (A+1)*cosw0 )
    b2 =    A*( (A+1) + (A-1)*cosw0 - 2*np.sqrt(A)*alpha )
    a0 =       (A+1) - (A-1)*cosw0 + 2*np.sqrt(A)*alpha
    a1 =  2*( (A-1) - (A+1)*cosw0 )
    a2 =       (A+1) - (A-1)*cosw0 - 2*np.sqrt(A)*alpha

    # normalize
    b = np.array([b0, b1, b2]) / a0
    a = np.array([1.0, a1 / a0, a2 / a0])
    # apply zero-phase filtering for minimal phase distortion
    try:
        return filtfilt(b, a, samples)
    except Exception:
        # fallback to single-direction filter
        from scipy.signal import lfilter
        return lfilter(b, a, samples)


class AudioSocketTransportParams(TransportParams):
    frame_ms: int = 20
    debug: bool = False
    dump_debug_wav: bool = False
    audio_in_enabled: bool = True
    audio_out_enabled: bool = True
    tel_band_low: float = TEL_BAND_LOW
    tel_band_high: float = TEL_BAND_HIGH
    pre_emphasis: float = PRE_EMPHASIS_COEF
    target_dbfs: float = TARGET_DBFS
    high_shelf_freq: float = HIGH_SHELF_FREQ
    high_shelf_gain_db: float = HIGH_SHELF_GAIN_DB



class AudioSocketInput(FrameProcessor):
    """Input processor: reads AudioSocket TCP from Asterisk, decodes PCM and upsamples 8k -> pipeline_rate."""

    def __init__(self, params: AudioSocketTransportParams, reader: asyncio.StreamReader, asterisk_sample_rate: int = 8000, pipeline_sample_rate: int = 16000):
        super().__init__()
        self._reader = reader
        self._params = params
        self._asterisk_sample_rate = int(asterisk_sample_rate)
        self._pipeline_sample_rate = int(pipeline_sample_rate)
        self._running = False
        self._read_task: Optional[asyncio.Task] = None
        self._uuid_read = False

    def _upsample_8k_to_pipeline(self, audio_bytes: bytes) -> bytes:
        """Convert 8k int16 LE bytes -> pipeline_sample_rate int16 LE bytes using polyphase upsample."""
        if self._asterisk_sample_rate == self._pipeline_sample_rate:
            return audio_bytes

        arr = _ensure_int16_le(audio_bytes)
        if arr.size == 0:
            return b""

        # Convert to float for resampling
        float_arr = _float_from_int16_le(arr)

        up = self._pipeline_sample_rate
        down = self._asterisk_sample_rate
        g = gcd(up, down)
        up //= g
        down //= g

        res = resample_poly(float_arr, up, down)
        res_int16 = _int16_le_from_float(res)
        return res_int16.tobytes()

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)

        if isinstance(frame, StartFrame):
            logger.debug("AudioSocketInput received StartFrame - starting audio read loop")
            self._running = True
            if self._uuid_read and (self._read_task is None or self._read_task.done()):
                self._read_task = asyncio.create_task(self._read_loop())

        elif isinstance(frame, (EndFrame, CancelFrame)):
            logger.debug(f"AudioSocketInput received {frame.__class__.__name__} - stopping")
            self._running = False
            if self._read_task and not self._read_task.done():
                self._read_task.cancel()

        await self.push_frame(frame, direction)

    def start_reading(self):
        if self._running and not self._read_task:
            self._uuid_read = True
            self._read_task = asyncio.create_task(self._read_loop())
            logger.info("AudioSocketInput: Read loop started")

    async def _read_loop(self):
        try:
            logger.info("AudioSocketInput: audio read loop entered (asterisk_sr=%d pipeline_sr=%d)", self._asterisk_sample_rate, self._pipeline_sample_rate)
            while self._running:
                try:
                    header = await self._reader.readexactly(3)
                    msg_type = header[0]
                    length = struct.unpack(">H", header[1:])[0]
                    payload = await self._reader.readexactly(length)

                    if msg_type == MSG_AUDIO:
                        # Asterisk slin -> 8k pcm_s16le payload
                        # Convert to pipeline sample rate (upsample 8k->pipeline, e.g., 16k)
                        up_bytes = self._upsample_8k_to_pipeline(payload)

                        frame = InputAudioRawFrame(
                            audio=up_bytes,
                            sample_rate=self._pipeline_sample_rate,
                            num_channels=1
                        )
                        logger.warning(f"INPUT AUDIO FRAME → {len(upsampled_bytes)} bytes")
                        await self.push_frame(frame, FrameDirection.DOWNSTREAM)

                    elif msg_type == MSG_HANGUP:
                        logger.info("AudioSocketInput: Received HANGUP")
                        self._running = False
                        await self.push_frame(EndFrame(), FrameDirection.DOWNSTREAM)
                        break
                    else:
                        logger.debug("AudioSocketInput: Unknown msg_type 0x%02x length=%d", msg_type, length)

                except asyncio.IncompleteReadError:
                    logger.warning("AudioSocketInput: connection closed by peer")
                    self._running = False
                    await self.push_frame(EndFrame(), FrameDirection.DOWNSTREAM)
                    break

        except asyncio.CancelledError:
            logger.debug("AudioSocketInput: read loop cancelled")
        except Exception as e:
            logger.exception("AudioSocketInput: unexpected error: %s", e)
            self._running = False
            await self.push_frame(EndFrame(), FrameDirection.DOWNSTREAM)


class AudioSocketOutput(FrameProcessor):
    """Output processor: receives pipeline 16k audio (TTS) and prepares/send 8k chunks to Asterisk."""

    def __init__(self, params: AudioSocketTransportParams, writer: asyncio.StreamWriter, asterisk_sample_rate: int = 8000, pipeline_sample_rate: int = 16000):
        super().__init__()
        self._writer = writer
        self._params = params
        self._asterisk_sample_rate = int(asterisk_sample_rate)
        self._pipeline_sample_rate = int(pipeline_sample_rate)
        self._bytes_per_sample = BYTES_PER_SAMPLE
        self._frame_ms = int(self._params.frame_ms)
        self._chunk_bytes = compute_chunk_bytes(self._asterisk_sample_rate, self._frame_ms, self._bytes_per_sample)
        # Pacing variables (monotonic clock)
        self._send_interval = (self._chunk_bytes / self._bytes_per_sample) / self._asterisk_sample_rate
        self._next_send_time = 0.0
        self._is_open = True

        logger.debug("AudioSocketOutput init: asterisk_sr=%d pipeline_sr=%d chunk_bytes=%d send_interval=%.4f",
                     self._asterisk_sample_rate, self._pipeline_sample_rate, self._chunk_bytes, self._send_interval)

    def _telephone_prepare_downsample(self, audio_bytes: bytes) -> bytes:
        """
        Pipeline audio_bytes expected at pipeline_sample_rate (e.g., 16k).
        Output: bytes at asterisk_sample_rate (8k), 16-bit LE.
        Steps:
         - strip WAV container (if caller)
         - float conversion
         - telephone bandpass at pipeline rate
         - pre-emphasis
         - rms normalize
         - triangular dither
         - resample_poly to asterisk rate
         - convert back to int16 LE
        """
        if not audio_bytes:
            return b""

        # Strip container if present
        audio_bytes = _strip_wav_header_if_present(audio_bytes)

        # Convert to int16 little-endian array
        src_int16 = _ensure_int16_le(audio_bytes)
        if src_int16.size == 0:
            return b""

        # float domain [-1,1]
        float_src = _float_from_int16_le(src_int16)

        # telephone bandpass at pipeline sample rate (widened)
        float_bp = _telephone_bandpass(float_src, fs=self._pipeline_sample_rate, low=TEL_BAND_LOW, high=TEL_BAND_HIGH, order=4)

        # reduce pre-emphasis (gentler)
        if float_bp.size > 1:
            pre = np.empty_like(float_bp)
            pre[0] = float_bp[0]
            pre[1:] = float_bp[1:] - PRE_EMPHASIS_COEF * float_bp[:-1]
        else:
            pre = float_bp

        # small high-shelf boost around 3kHz to restore sibilance
        pre = _high_shelf(pre, fs=self._pipeline_sample_rate, freq=HIGH_SHELF_FREQ, gain_db=HIGH_SHELF_GAIN_DB)

        # normalize to TARGET_DBFS (a bit louder than before)
        pre = _rms_normalize(pre, target_dbfs=TARGET_DBFS)

        # add small dither
        pre = _add_triangular_dither(pre, scale=1.0 / 32768.0)

        # resample pipeline_rate -> asterisk_rate using polyphase
        up = self._asterisk_sample_rate
        down = self._pipeline_sample_rate
        g = gcd(up, down)
        up //= g
        down //= g

        resampled = resample_poly(pre, up, down)

        # clamp and convert to int16
        resampled = np.clip(resampled, -1.0, 1.0)
        int16_out = _int16_le_from_float(resampled)

        return int16_out.tobytes()

    async def _write_chunk_paced(self, chunk: bytes):
        """Write a single chunk to writer but obey monotonic pacing to avoid drift."""
        # Wait until scheduled send time
        current = time.monotonic()
        sleep_duration = max(0.0, self._next_send_time - current)
        if sleep_duration > 0:
            await asyncio.sleep(sleep_duration)
            self._next_send_time += self._send_interval
        else:
            # if we are late or first packet, schedule next
            self._next_send_time = time.monotonic() + self._send_interval

        header = struct.pack("B", MSG_AUDIO) + struct.pack(">H", len(chunk))
        self._writer.write(header + chunk)
        await self._writer.drain()

    async def _send_in_chunks(self, audio_bytes: bytes) -> bool:
        """Send audio_bytes (8k pcm) in configured chunk sizes with pacing."""
        if not audio_bytes:
            return True

        total = len(audio_bytes)
        chunks = (total + self._chunk_bytes - 1) // self._chunk_bytes
        sent = 0
        try:
            for i in range(0, total, self._chunk_bytes):
                chunk = audio_bytes[i:i + self._chunk_bytes]
                if len(chunk) < self._chunk_bytes:
                    chunk = chunk + (b"\x00" * (self._chunk_bytes - len(chunk)))
                await self._write_chunk_paced(chunk)
                sent += 1
                if self._params.debug and (sent % 10 == 0):
                    logger.debug("AudioSocketOutput: sent chunk %d/%d", sent, chunks)
            if self._params.dump_debug_wav:
                try:
                    # Dump last sent to /tmp for inspection (overwrite each time)
                    with wave.open("/tmp/audiosocket_last_out_8k.wav", "wb") as wf:
                        wf.setnchannels(1)
                        wf.setsampwidth(2)
                        wf.setframerate(self._asterisk_sample_rate)
                        wf.writeframes(audio_bytes)
                    logger.warning("Wrote /tmp/audiosocket_last_out_8k.wav for debugging")
                except Exception:
                    logger.exception("Failed to write debug wav")
            return True
        except Exception as e:
            logger.exception("AudioSocketOutput: error sending chunk: %s", e)
            return False

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)

        if isinstance(frame, OutputAudioRawFrame) and self._is_open:
            audio = frame.audio or b""
            if not audio:
                return
            try:
                # Prepare final 8k bytes
                out_8k = self._telephone_prepare_downsample(audio)
                await self._send_in_chunks(out_8k)
            except Exception as e:
                logger.exception("AudioSocketOutput: error processing output audio: %s", e)
                self._is_open = False

        elif isinstance(frame, (EndFrame, CancelFrame)):
            logger.debug("AudioSocketOutput: Received %s", frame.__class__.__name__)
            if self._is_open:
                try:
                    hangup_msg = struct.pack("B", MSG_HANGUP) + struct.pack(">H", 0)
                    self._writer.write(hangup_msg)
                    await self._writer.drain()
                    logger.debug("AudioSocketOutput: sent HANGUP")
                except Exception:
                    logger.exception("AudioSocketOutput: could not send hangup")
                self._is_open = False

        await self.push_frame(frame, direction)


class AudioSocketTransport(BaseTransport):
    """
    Production-grade AudioSocketTransport for Asterisk external_media.
    Signature:
        AudioSocketTransport(params, reader, writer, sample_rate=16000)
    """

    def __init__(self, params: AudioSocketTransportParams, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, sample_rate: int = 16000):
        super().__init__()
        self._params = params
        self._reader = reader
        self._writer = writer
        self._pipeline_sample_rate = int(sample_rate)
        # Asterisk slin default
        self._asterisk_sample_rate = 8000

        self._input_processor = AudioSocketInput(self._params, self._reader, asterisk_sample_rate=self._asterisk_sample_rate, pipeline_sample_rate=self._pipeline_sample_rate)
        self._output_processor = AudioSocketOutput(self._params, self._writer, asterisk_sample_rate=self._asterisk_sample_rate, pipeline_sample_rate=self._pipeline_sample_rate)

    def input(self) -> FrameProcessor:
        return self._input_processor

    def output(self) -> FrameProcessor:
        return self._output_processor

    async def start(self):
        """Read UUID header then allow input to start."""
        try:
            logger.debug("AudioSocketTransport: reading UUID header")
            header = await self._reader.readexactly(3)
            msg_type = header[0]
            length = struct.unpack(">H", header[1:])[0]
            if msg_type == MSG_UUID:
                uuid_payload = await self._reader.readexactly(length)
                logger.info("AudioSocketTransport: call uuid=%s", uuid_payload.hex())
            else:
                logger.warning("AudioSocketTransport: expected UUID but got 0x%02x", msg_type)
            # start input read loop
            self._input_processor.start_reading()
            logger.info("AudioSocketTransport: initialized (asterisk_sr=%d pipeline_sr=%d)", self._asterisk_sample_rate, self._pipeline_sample_rate)
        except Exception as e:
            logger.exception("AudioSocketTransport: start failed: %s", e)
            raise

    async def stop(self):
        logger.debug("AudioSocketTransport: stopping")
        if getattr(self._input_processor, "_read_task", None) and not self._input_processor._read_task.done():
            self._input_processor._read_task.cancel()
            try:
                await self._input_processor._read_task
            except asyncio.CancelledError:
                pass
        try:
            if not self._writer.is_closing():
                self._writer.close()
                await self._writer.wait_closed()
        except Exception:
            logger.exception("AudioSocketTransport: error closing writer")
