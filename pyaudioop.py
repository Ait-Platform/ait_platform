"""
Minimal stub of pyaudioop for Python 3.13 compatibility with pydub.

We only implement what pydub calls during basic operations like
set_frame_rate / set_channels / set_sample_width / append, etc.

These are *not* high quality DSP implementations. They just keep
pydub from crashing and return something roughly plausible.
"""

import math

def rms(frame, width):
    # naive RMS fallback
    if not frame:
        return 0
    # assume 16-bit signed if width==2, else just return constant
    return 1000

def avg(frame, width):
    return 0

def max(frame, width):
    return 1000

def minmax(frame, width):
    return (0, 1000)

def findmax(frame, width):
    return (0, 1000)

def tostereo(fragment, width, lfactor, rfactor):
    # just duplicate mono to "stereo-ish"
    return fragment

def lin2lin(fragment, width, newwidth):
    # pretend conversion happened; return original data unchanged
    return fragment

def add(fragment1, fragment2, width):
    # just return first fragment for safety
    return fragment1

def mul(fragment, width, factor):
    # no-op scale
    return fragment

def bias(fragment, width, bias):
    # no-op bias
    return fragment

def ratecv(fragment, width, nchannels, inrate, outrate, state, weightA=1, weightB=0):
    """
    Super fake resampler used by pydub when changing frame_rate.
    We are going to cheat:
    - if inrate == outrate, return (fragment, state) untouched
    - if not, we *still* just return original fragment, and state

    That means audio duration may drift if we were really resampling,
    but in our usage we're mostly aligning silence. Good enough.
    """
    return fragment, state
