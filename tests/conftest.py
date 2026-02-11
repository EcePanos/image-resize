import os
import sys
from pathlib import Path


# Ensure the project root is on sys.path so that tests can import
# top-level modules like `server` and `image_processor` regardless of
# where pytest is invoked from.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Avoid hitting real MinIO/DB on import in tests.
os.environ.setdefault("SKIP_EXTERNAL_INIT", "1")

