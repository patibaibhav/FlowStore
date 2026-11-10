"""Build the deployment zip for the optional Lambda processor.

    python lambda/build.py            ->  lambda/dist/flowstore-processor.zip

The zip holds two files, copied from the backend so there is one source of truth:
  lambda_handler.py   (backend/app/pipeline/lambda_handler.py)
  events.py           (backend/app/pipeline/events.py)   <- the validation rules
Lambda settings: runtime Python 3.12, handler `lambda_handler.lambda_handler`, 256 MB, 10 s timeout.
Rebuild and re-upload whenever events.py changes (the app warns if the function's rules differ).
"""

from __future__ import annotations

import hashlib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "backend" / "app" / "pipeline"
OUT = Path(__file__).resolve().parent / "dist" / "flowstore-processor.zip"

FILES = {"lambda_handler.py": SRC / "lambda_handler.py", "events.py": SRC / "events.py"}


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(OUT, "w", zipfile.ZIP_DEFLATED) as z:
        for name, path in FILES.items():
            # normalise line endings so the code hash matches the backend regardless of OS
            data = path.read_bytes().replace(b"\r\n", b"\n")
            info = zipfile.ZipInfo(name, date_time=(2024, 1, 1, 0, 0, 0))
            info.external_attr = 0o644 << 16
            z.writestr(info, data, compress_type=zipfile.ZIP_DEFLATED)
    events_hash = hashlib.sha256(FILES["events.py"].read_bytes().replace(b"\r\n", b"\n")).hexdigest()[:16]
    print(f"wrote {OUT} ({OUT.stat().st_size:,} bytes)")
    print(f"validation code hash: {events_hash}")
    print("Lambda: runtime Python 3.12, handler lambda_handler.lambda_handler, memory 256 MB, timeout 10 s")


if __name__ == "__main__":
    main()
