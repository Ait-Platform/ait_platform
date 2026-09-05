import subprocess
import sys

try:
    result = subprocess.run(
        ["flask", "db", "migrate", "-m", "migration_cleanup"],
        capture_output=True,
        text=True
    )
    print("RETURN CODE:", result.returncode)
    print("STDOUT:")
    print(result.stdout)
    print("STDERR:")
    print(result.stderr)
except Exception as e:
    print("Exception:", e)
