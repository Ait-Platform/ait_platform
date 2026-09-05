import subprocess
import sys

def run(cmd):
    print("RUNNING:", cmd)
    result = subprocess.run(cmd, capture_output=True, text=True)
    print("STDOUT:", result.stdout)
    print("STDERR:", result.stderr)
    return result.returncode

run(["flask", "db", "stamp", "head"])
run(["flask", "db", "migrate", "-m", "sync_db_to_models"])
