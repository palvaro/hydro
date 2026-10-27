#!/usr/bin/env bash
# Renders reliable-send-three-tools.md to reliable-send-three-tools.pdf.
# Needs pandoc 3.5 and typst 0.12 on PATH (the same pipeline as hydrofrac-guide.pdf).
set -euo pipefail
cd "$(dirname "$0")"
pandoc reliable-send-three-tools.md -o reliable-send-three-tools.pdf \
  -f markdown+gfm_auto_identifiers --shift-heading-level-by=-1 \
  --pdf-engine=typst \
  --metadata-file=reliable-send-three-tools-pdf.yaml \
  --lua-filter=reliable-send-three-tools-pdf.lua \
  --include-before-body=reliable-send-three-tools-style.typ
