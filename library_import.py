import subprocess
import sys
import os

# Ensure working directory is the project root
os.chdir("/Workspace/Users/akankshaslokhande14@gmail.com/Maven_Market")

# Prevent __pycache__ writes (not supported on workspace filesystem)
sys.dont_write_bytecode = True

# Install test dependencies
subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "pytest", "flake8", "pyyaml"])

# Run pytest
import pytest
exit_code = pytest.main(["tests/", "--verbose", "-p", "no:cacheprovider"])
print(f"\nPytest exit code: {exit_code}")