#!/usr/bin/env bash
# Renders reliable-send-three-tools-short.md to reliable-send-three-tools-short.pdf.
# Needs pandoc 3.5 and typst 0.12 on PATH. It reuses the Lua filter and the typst style of the
# full report (reliable-send-three-tools-pdf.sh), with its own metadata file.
set -euo pipefail
cd "$(dirname "$0")"
pandoc reliable-send-three-tools-short.md -o reliable-send-three-tools-short.pdf \
  -f markdown+gfm_auto_identifiers --shift-heading-level-by=-1 \
  --pdf-engine=typst \
  --metadata-file=reliable-send-three-tools-short-pdf.yaml \
  --lua-filter=reliable-send-three-tools-pdf.lua \
  --include-before-body=reliable-send-three-tools-style.typ \
  --include-before-body=reliable-send-three-tools-short-style.typ
