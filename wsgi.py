import os
import sys
from pathlib import Path

# Fix for WeasyPrint/GTK3 on Windows with Python 3.8+
if os.name == 'nt':
    gtk_path = r'C:\Program Files\GTK3-Runtime Win64\bin'
    if os.path.exists(gtk_path):
        os.add_dll_directory(gtk_path)
    os.environ['PATH'] = gtk_path + os.pathsep + os.environ.get('PATH', '')

# *** FORCE correct project root ***
BASE_DIR = Path(__file__).resolve().parent
ROOT = BASE_DIR  # this folder is D:/Users/yeshk/Documents/ait_platform
sys.path.insert(0, str(ROOT))

# now import the real app
from app import create_app
app = create_app()
