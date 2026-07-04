#!/usr/bin/env python3
"""Build the single-file deployment artifact for the BARRY.IO Lakeflow connector.

This mirrors the Databricks community-connector merge step
(``python tools/scripts/merge_python_source.py --connector <source>``), which
produces "a single self-contained Python file" under ``dist/<connector>/``.

Our connector (``barry_io_lakeflow.py``) is already a single, self-contained
module — it imports only the standard library plus ``requests`` / ``pyarrow`` /
``pyspark`` (all provided by the Databricks runtime), with **no local/relative
imports**. So there is nothing to merge: this script prepends a deployment
banner and emits the source under ``dist/<name>/<module>.py`` ready to upload
to a Databricks workspace.

Usage:
    python tools/build_artifact.py                 # -> dist/barry-io-lakeflow/barry_io_lakeflow.py
    python tools/build_artifact.py --name my-name  # override the artifact name

The result is deterministic (no timestamps) so re-running yields an identical file.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Repo layout: this script lives in lakeflow-connector/tools/, the connector
# source one level up.
TOOLS_DIR = Path(__file__).resolve().parent
CONNECTOR_DIR = TOOLS_DIR.parent
DEFAULT_SOURCE = CONNECTOR_DIR / "barry_io_lakeflow.py"
DEFAULT_NAME = "barry-io-lakeflow"

def _version(source: str) -> str:
    m = re.search(r'^__version__\s*=\s*"([^"]+)"', source, re.MULTILINE)
    return m.group(1) if m else "0.0.0"


def _banner(name: str, version: str, source_name: str) -> str:
    bar = "# " + "=" * 74
    return "\n".join(
        [
            bar,
            f"# {name} — BARRY.IO Lakeflow Community Connector (deployment artifact)",
            "#",
            "# Single, self-contained Python file for upload to a Databricks workspace",
            "# as a Lakeflow community connector. Implements the LakeflowConnect",
            "# interface; pulls data on demand from the BARRY.IO Integration Runtime",
            "# serving API (Authorization: Bearer <per-group token>).",
            "#",
            f"# Version    : {version}",
            f"# Generated  : tools/build_artifact.py from {source_name} — DO NOT EDIT.",
            "#              Edit the source and re-run the build script instead.",
            bar,
            "",
            "",
        ]
    )


def build(source_path: Path, name: str, out_dir: Path) -> Path:
    if not source_path.is_file():
        raise SystemExit(f"connector source not found: {source_path}")

    source = source_path.read_text(encoding="utf-8")
    version = _version(source)

    module = name.replace("-", "_")
    artifact = _banner(name, version, source_path.name) + source

    dest_dir = out_dir / name
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{module}.py"
    dest.write_text(artifact, encoding="utf-8")
    return dest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the BARRY.IO Lakeflow deployment artifact.")
    parser.add_argument("--name", default=DEFAULT_NAME, help="artifact / connector name (default: barry-io-lakeflow)")
    parser.add_argument("--source", default=str(DEFAULT_SOURCE), help="connector source .py file")
    parser.add_argument("--out", default=str(CONNECTOR_DIR / "dist"), help="output dist/ directory")
    args = parser.parse_args(argv)

    dest = build(Path(args.source), args.name, Path(args.out))
    size = dest.stat().st_size
    print(f"Built {args.name} -> {dest} ({size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
