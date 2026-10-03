"""Run the isolated HOME Phase 2A lifecycle suite."""
import subprocess
import sys
import unittest
from pathlib import Path


class HomeSaceLifecycle(unittest.TestCase):
    def test_home_lifecycle(self):
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            [sys.executable, "-B", str(root / "tests/support/home_sace_lifecycle_runner.py")],
            cwd=root, capture_output=True, text=True, timeout=120)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
