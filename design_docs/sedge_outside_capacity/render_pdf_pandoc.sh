#!/usr/bin/env bash
# Render demo_relay_broadcast.md to PDF with pandoc + a LaTeX engine.
#   PANDOC     path to pandoc       (default: pandoc on PATH)
#   PDF_ENGINE tectonic or xelatex  (default: tectonic on PATH)
#   TECTONIC_CACHE_DIR  tectonic package cache (default: ${TMPDIR:-/tmp}/tectonic-cache,
#                       because ~/.cache may be read-only)
# Usage: ./render_pdf_pandoc.sh [doc.md]   (default: demo_relay_broadcast.md)
set -euo pipefail
cd "$(dirname "$0")"
PANDOC="${PANDOC:-pandoc}"
PDF_ENGINE="${PDF_ENGINE:-tectonic}"
export TECTONIC_CACHE_DIR="${TECTONIC_CACHE_DIR:-${TMPDIR:-/tmp}/tectonic-cache}"
export XDG_CACHE_HOME="${XDG_CACHE_HOME:-${TMPDIR:-/tmp}/cache}"
DOC="${1:-demo_relay_broadcast.md}"
"$PANDOC" "$DOC" -o "${DOC%.md}.pdf" \
  --from=markdown-yaml_metadata_block \
  --pdf-engine="$PDF_ENGINE" \
  --resource-path=. \
  --include-in-header=pdf_header.tex \
  --toc --highlight-style=tango \
  -V geometry:margin=2cm -V fontsize=10pt \
  -V mainfont="DejaVu Sans" -V sansfont="DejaVu Sans" -V monofont="DejaVu Sans Mono" \
  -V monofontoptions="Scale=0.9" \
  -V colorlinks=true -V linkcolor=blue -V urlcolor=blue
