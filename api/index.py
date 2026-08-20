import sys
import os

# Make the parent directory importable so we can reuse main.py as-is
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app
