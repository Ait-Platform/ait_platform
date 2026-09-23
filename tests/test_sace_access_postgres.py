"""Run isolated PostgreSQL HTTP journeys without changing pytest's app imports."""
import subprocess
import sys
import unittest
from pathlib import Path


class SacePostgresJourneys(unittest.TestCase):
    def test_isolated_http_journeys(self):
        root = Path(__file__).resolve().parents[1]
        result = subprocess.run(
            [sys.executable, "-B", str(root / "tests/support/sace_access_postgres_runner.py")],
            cwd=root, capture_output=True, text=True, timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main(verbosity=2)
