import struct
import logging
from typing import Optional

from pipecat.frames.frames import (
    InputAudioRawFrame,
    OutputAudioRawFrame,
    Frame,
)
from pipecat.serializers.base_serializer import FrameSerializer, FrameSerializerType

from scipy.signal import resample_poly
import numpy as np

logger = logging.getLogger(__name__)

MSG_AUDIO = 0x10

class AudioSocketFrameSerializer(FrameSerializer):
    """
    Serializer for Asterisk AudioSocket.
    Supports slin (8kHz) or slin16 (16kHz).
    """

    class InputParams:
        def __init__(
            self,
            asterisk_sample_rate: int = 16000,  # 8000 for slin, 16000 for slin16
            sample_rate: Optional[int] = None,  # pipeline rate (optional)
        ):
            self.asterisk_sample_rate = asterisk_sample_rate
            self.sample_rate = sample_rate

    def __init__(self, params: Optional[InputParams] = None):
        self._params = params or self.InputParams()
        self._asterisk_rate = self._params.asterisk_sample_rate
        self._pipeline_rate = 0

    @property
    def type(self) -> FrameSerializerType:
        return FrameSerializerType.BINARY

    async def setup(self, frame):
        """Called during transport start to get pipeline sample rate"""
        self._pipeline_rate = self._params.sample_rate or frame.audio_in_sample_rate
        logger.info(
            f"AudioSocket serializer: asterisk={self._asterisk_rate}Hz, "
            f"pipeline={self._pipeline_rate}Hz"
        )

    # ============== INCOMING: Asterisk → Pipeline ==============

    async def deserialize(self, payload: bytes) -> Frame | None:
        """
        Convert raw PCM from Asterisk to pipeline format.
        payload = raw PCM S16LE (NO AudioSocket header, already stripped)
        """
        if not payload:
            return None

        try:
            # Parse Asterisk audio (S16LE at asterisk_rate)
            pcm_asterisk = np.frombuffer(payload, dtype=np.int16)

            # Resample if needed
            if self._pipeline_rate != self._asterisk_rate:
                pcm_pipeline = resample_poly(
                    pcm_asterisk,
                    up=self._pipeline_rate,
                    down=self._asterisk_rate,
                ).astype(np.int16)
            else:
                pcm_pipeline = pcm_asterisk

            return InputAudioRawFrame(
                audio=pcm_pipeline.tobytes(),
                sample_rate=self._pipeline_rate,
                num_channels=1,
            )

        except Exception as exc:
            logger.error(f"Deserialize error: {exc}")
            return None

    # ============== OUTGOING: Pipeline → Asterisk ==============

    async def serialize(self, frame: Frame) -> bytes | None:
        """
        Convert pipeline audio to Asterisk AudioSocket format.
        Returns: AudioSocket packet (header + PCM payload)
        """
        if not isinstance(frame, OutputAudioRawFrame):
            return None

        try:
            # Parse pipeline audio
            pcm_pipeline = np.frombuffer(frame.audio, dtype=np.int16)

            # Resample to Asterisk rate if needed
            if frame.sample_rate != self._asterisk_rate:
                pcm_asterisk = resample_poly(
                    pcm_pipeline,
                    up=self._asterisk_rate,
                    down=frame.sample_rate,
                ).astype(np.int16)
            else:
                pcm_asterisk = pcm_pipeline

            # Build AudioSocket packet: [type:1][length:2][payload:N]
            payload = pcm_asterisk.tobytes()
            header = struct.pack("B", MSG_AUDIO) + struct.pack(">H", len(payload))
            packet = header + payload

            logger.debug(
                f"Sending audio: {len(payload)} bytes @ {self._asterisk_rate}Hz"
            )

            return packet

        except Exception as exc:
            logger.error(f"Serialize error: {exc}")
            return None
