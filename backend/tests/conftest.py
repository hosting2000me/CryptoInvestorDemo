"""
Pytest configuration and shared fixtures for backend tests.
"""
import sys
from pathlib import Path

# Добавляем директорию backend в Python path
# Это позволяет импортировать модули core при запуске pytest
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))
