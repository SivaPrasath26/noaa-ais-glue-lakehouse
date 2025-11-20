"""
Build a code package zip with forward-slash paths for Glue/PySpark.
Defaults to zipping utils/ and transformations/ into code_package.zip.
"""

import argparse
import os
import zipfile
from pathlib import Path


def should_skip(path: Path) -> bool:
    name = path.name
    return name == "__pycache__" or name.endswith(".pyc") or name.endswith(".pyo")


def add_paths(zf: zipfile.ZipFile, roots: list[Path]):
    for root in roots:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if not should_skip(Path(d))]
            for fname in filenames:
                full = Path(dirpath) / fname
                if should_skip(full):
                    continue
                arc = full.as_posix()
                zf.write(full, arcname=arc)


def build_zip(output: Path, paths: list[Path]) -> None:
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as zf:
        add_paths(zf, paths)


def parse_args():
    p = argparse.ArgumentParser(description="Create code package zip with forward slashes.")
    p.add_argument(
        "-o",
        "--output",
        default="code_package.zip",
        help="Output zip filename (default: code_package.zip)",
    )
    p.add_argument(
        "paths",
        nargs="*",
        default=["utils", "transformations"],
        help="Directories to include (default: utils transformations)",
    )
    return p.parse_args()


def main():
    args = parse_args()
    paths = [Path(p) for p in args.paths]
    for p in paths:
        if not p.exists():
            raise FileNotFoundError(f"Path not found: {p}")
    build_zip(Path(args.output), paths)
    print(f"Built {args.output} with {', '.join(args.paths)}")


if __name__ == "__main__":
    main()
