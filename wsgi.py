from app import create_app  # adjust this import if your factory lives elsewhere
import sys
import os
from pathlib import Path

# *** FORCE correct project root ***
BASE_DIR = Path(__file__).resolve().parent
ROOT = BASE_DIR  # this folder is D:/Users/yeshk/Documents/ait_platform
sys.path.insert(0, str(ROOT))

# now import the real app
from app import create_app
app = create_app()
