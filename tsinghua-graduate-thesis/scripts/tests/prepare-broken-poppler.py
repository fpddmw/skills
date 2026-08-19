#!/usr/bin/env python3
from pathlib import Path
import re
import shutil
import subprocess


binary = Path("/usr/bin/pdftoppm")
ldd_output = subprocess.run(
    ["ldd", str(binary)],
    check=True,
    capture_output=True,
    text=True,
).stdout
match = re.search(r"^\s*(libpoppler\.so\.\S+)\s+=>\s+(\S+)", ldd_output, re.MULTILINE)
if not match:
    raise SystemExit("could not resolve pdftoppm libpoppler dependency")

soname, source_name = match.groups()
source = Path(source_name).resolve()
target_dir = Path("/opt/broken-poppler/lib")
target_dir.mkdir(parents=True, exist_ok=True)
target = target_dir / soname
shutil.copy2(source, target)

data = target.read_bytes()
compiled_data_dir = b"/usr/share/poppler"
missing_data_dir = b"/missing/poppler"
count = data.count(compiled_data_dir)
if count < 1:
    raise SystemExit("libpoppler does not contain the expected compiled data directory")
replacement = missing_data_dir.ljust(len(compiled_data_dir), b"\0")
target.write_bytes(data.replace(compiled_data_dir, replacement))
target.chmod(0o755)
print(f"patched {target} ({count} occurrence(s))")
