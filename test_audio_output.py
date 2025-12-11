import pyaudio
import numpy as np
import time

def play_tone():
    p = pyaudio.PyAudio()
    
    volume = 0.5     # range [0.0, 1.0]
    fs = 16000       # sampling rate, Hz, must be integer
    duration = 3.0   # in seconds, may be float
    f = 440.0        # sine frequency, Hz, may be float

    # generate samples, note conversion to float32 array
    samples = (np.sin(2*np.pi*np.arange(fs*duration)*f/fs)).astype(np.float32)

    # for paFloat32 sample values must be in range [-1.0, 1.0]
    stream = p.open(format=pyaudio.paFloat32,
                    channels=1,
                    rate=fs,
                    output=True)

    print("Playing tone...")
    stream.write(volume*samples)
    print("Done.")

    stream.stop_stream()
    stream.close()
    p.terminate()

if __name__ == "__main__":
    play_tone()
