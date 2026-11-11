"""Bound METSOA Chromium concurrency across threads and Gunicorn workers."""
from contextlib import contextmanager
import os
from pathlib import Path
import tempfile
import threading
from werkzeug.exceptions import ServiceUnavailable

_thread_slot = threading.Lock()


@contextmanager
def browser_slot():
    """Fail promptly rather than queue memory-heavy browsers on one instance."""
    busy = 'METSOA PDF generation is busy. Another PDF is being generated; please try again when it finishes.'
    if not _thread_slot.acquire(blocking=False):
        raise ServiceUnavailable(description=busy)
    handle = None
    locked = False
    try:
        handle = open(Path(tempfile.gettempdir()) / 'ait-metsoa-chromium.lock', 'a+b')
        if os.name == 'nt':
            import msvcrt
            if not handle.tell():
                handle.write(b'0')
                handle.flush()
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as exc:
                raise ServiceUnavailable(description=busy) from exc
        else:
            import fcntl
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise ServiceUnavailable(description=busy) from exc
        locked = True
        yield
    finally:
        if handle is not None:
            try:
                if locked and os.name == 'nt':
                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            finally:
                handle.close()  # Also releases the OS lock on Linux or process exit.
        _thread_slot.release()
