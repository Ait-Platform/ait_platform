import ctypes
import os



dlls = [
    r"C:\Program Files\GTK3-Runtime Win64\bin\libcairo-2.dll",
    r"C:\Program Files\GTK3-Runtime Win64\bin\libpango-1.0-0.dll",
    r"C:\Program Files\GTK3-Runtime Win64\bin\libgdk_pixbuf-2.0-0.dll"
]

for dll in dlls:
    try:
        ctypes.CDLL(dll)
        print(f"Loaded OK: {dll}")
    except OSError as e:
        print(f"Failed to load {dll}: {e}")