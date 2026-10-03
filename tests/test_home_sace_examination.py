"""Discoverable entry point for isolated HOME Phase 2B checks."""
import subprocess
import sys
import unittest
from pathlib import Path


class HomeSaceExamination(unittest.TestCase):
    def test_home_examination(self):
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run([sys.executable, '-B', str(root / 'tests/support/home_sace_examination_runner.py')],
            cwd=root, capture_output=True, text=True, timeout=180)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main(verbosity=2)
