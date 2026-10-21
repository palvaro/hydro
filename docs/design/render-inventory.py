#!/usr/bin/env python3
"""Render docs/design/hydrofrac-inventory.md to hydrofrac-inventory.pdf.

Setup (once):

    python3 -m venv $TMPDIR/pdfenv
    $TMPDIR/pdfenv/bin/pip install markdown 'weasyprint==52.5' pillow

Run (from anywhere; paths are resolved relative to this file):

    $TMPDIR/pdfenv/bin/python docs/design/render-inventory.py

What it does beyond a plain markdown → HTML → PDF conversion:

* inserts a blank line before a top-level list item that directly follows an
  indented continuation paragraph, so python-markdown does not swallow the new
  item into the previous paragraph as a lazy continuation;
* converts with the ``tables``, ``fenced_code``, ``toc`` and ``sane_lists``
  extensions;
* inserts U+200B (zero-width space) after ``_ . : ( , / = [`` inside ``<code>``
  within table cells so long identifiers can wrap;
* wraps ``<p><img></p>`` followed by ``<p><em>caption</em></p>`` into
  ``<figure><img><figcaption>…</figcaption></figure>``;
* rewrites every ``<img src="…/failing.png">`` to ``failing-print.png`` when a
  print re-render (larger fonts, higher dpi — see
  ``molly-viz-samples/ec/PRINT.md``) exists next to it;
* sizes every figure from the image's *natural* size in inches (pixels / dpi,
  where dpi comes from the PNG's pHYs chunk, else from a ``-Gdpi=N`` render
  comment in a sibling ``.dot``, else 160 for ``failing-print.png`` and 110
  otherwise) and never upscales:

  - natural width <= 7 in: inline in the portrait flow at natural size;
  - wider, but shrinking to the 7 in column keeps a 9 pt label >= 7 pt
    (natural width <= 9 in): inline, shrunk to the column;
  - natural width <= 9.8 in: own landscape Letter page;
  - wider still: an oversize page sized to the figure (``@page fig-N``), the
    image drawn at natural size capped at 30 in wide, so on screen the label
    text is the renderer's native size (9 pt x 1.6 for print re-renders).
"""

import html
import math
import os
import re
import sys

import markdown
from PIL import Image
from weasyprint import HTML, CSS

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "hydrofrac-inventory.md")
OUT = os.path.join(HERE, "hydrofrac-inventory.pdf")

# Portrait column and landscape frame (Letter minus the margins below).
PORTRAIT_W, PORTRAIT_H = 7.0, 8.5
LANDSCAPE_W, LANDSCAPE_H = 9.8, 6.8
# Widest an oversize page's image may be drawn.
OVERSIZE_MAX_W = 30.0
# Keep a figure inline (shrunk to the column) while a 9 pt label stays >= this.
LABEL_PT, INLINE_MIN_LABEL_PT = 9.0, 7.0
DEFAULT_DPI, PRINT_DPI = 110, 160

CSS_TEXT = """
@page { size: Letter; margin: 0.75in; @bottom-center { content: counter(page); font-size: 9pt; color: #666; } }
@page landscape { size: Letter landscape; margin: 0.6in; @bottom-center { content: counter(page); font-size: 9pt; color: #666; } }
body { font-family: "DejaVu Serif", "Nimbus Roman", serif; font-size: 10.5pt; line-height: 1.4; }
h1,h2,h3 { font-family: "DejaVu Sans", sans-serif; page-break-after: avoid; }
h1 { font-size: 20pt; } h2 { font-size: 15pt; margin-top: 1.4em; border-bottom: 1px solid #ccc; } h3 { font-size: 12pt; }
pre { white-space: pre-wrap; word-wrap: break-word; overflow-wrap: anywhere; background: #f4f4f4; padding: 6px; font-size: 8.5pt; }
code { font-family: "DejaVu Sans Mono", monospace; font-size: 88%; background: #f4f4f4; overflow-wrap: anywhere; word-break: break-all; }
table { width: 100%; table-layout: fixed; border-collapse: collapse; font-size: 8.5pt; margin: 8px 0; }
th, td { border: 1px solid #999; padding: 3px 5px; vertical-align: top; text-align: left; overflow-wrap: anywhere; word-break: break-word; white-space: normal; hyphens: auto; }
td code, th code { white-space: normal; word-break: break-all; }
tr { page-break-inside: avoid; }
blockquote { border-left: 3px solid #bbb; margin-left: 0; padding-left: 10px; color: #333; }
img { max-width: 100%; max-height: 8.5in; height: auto; display: block; margin: 8px auto; }
figure { page-break-inside: avoid; margin: 10px 0; } figcaption { font-size: 9.5pt; color: #333; margin-top: 4px; }
figure.wide { page: landscape; page-break-before: always; page-break-after: always; }
figure.oversize { margin: 0; page-break-before: always; page-break-after: always; }
figure.oversize img { margin: 0 auto 4px; max-width: none; max-height: none; }
"""


def preprocess_markdown(text):
    """Blank line before a list item that directly follows an indented paragraph."""
    out = []
    prev = ""
    for line in text.splitlines():
        if re.match(r"^[-*+] |^\d+\. ", line) and re.match(r"^ {2,}\S", prev):
            out.append("")
        out.append(line)
        prev = line
    return "\n".join(out) + "\n"


def soft_break_code_in_cells(html_text):
    """Insert U+200B after break-friendly characters inside <code> within <td>/<th>."""

    def fix_code(m):
        return m.group(1) + re.sub(r"([_.:(,/=\[])", "\\1\u200b", m.group(2)) + m.group(3)

    def fix_cell(m):
        return re.sub(r"(<code[^>]*>)(.*?)(</code>)", fix_code, m.group(0), flags=re.S)

    return re.sub(r"<t[dh]\b[^>]*>.*?</t[dh]>", fix_cell, html_text, flags=re.S)


def image_dpi(path, im):
    """pHYs chunk, else a ``-Gdpi=N`` render comment in a sibling .dot, else defaults."""
    dpi = im.info.get("dpi")
    if dpi and dpi[0] and round(dpi[0]) not in (72, 96):
        return float(dpi[0])
    base, name = os.path.split(path)
    stem = os.path.splitext(name)[0]
    dot = os.path.join(base, stem + ".dot")
    if os.path.exists(dot):
        with open(dot, encoding="utf-8", errors="replace") as f:
            m = re.search(r"-Gdpi=(\d+(?:\.\d+)?)", f.read())
        if m:
            return float(m.group(1))
    return PRINT_DPI if name == "failing-print.png" else DEFAULT_DPI


def image_inches(src):
    """(width_in, height_in) at the image's natural dpi, or None."""
    path = os.path.join(HERE, html.unescape(src))
    try:
        with Image.open(path) as im:
            w, h = im.size
            dpi = image_dpi(path, im)
    except OSError:
        return None
    return w / dpi, h / dpi


def fit(w, h, max_w, max_h):
    k = min(1.0, max_w / w, max_h / h)
    return w * k, h * k


def caption_lines(caption_html, width_in):
    """Rough line count of a 9.5 pt serif caption set across ``width_in`` inches."""
    text = html.unescape(re.sub(r"<[^>]+>", "", caption_html))
    chars_per_line = width_in * 72 / (9.5 * 0.55)
    return max(1, math.ceil(len(text) / chars_per_line))


def place_figure(w, h, caption_html=""):
    """Return (placement, draw_w, draw_h, page_w, page_h) in inches.

    placement is "inline", "landscape" or "oversize"; page_* only for oversize.
    The oversize page is the drawn image plus 0.4 in margins, the caption
    (estimated line count) and some slack.
    """
    if w <= PORTRAIT_W or LABEL_PT * PORTRAIT_W / w >= INLINE_MIN_LABEL_PT:
        dw, dh = fit(w, h, PORTRAIT_W, PORTRAIT_H)
        return "inline", dw, dh, None, None
    if w <= LANDSCAPE_W:
        dw, dh = fit(w, h, LANDSCAPE_W, LANDSCAPE_H)
        return "landscape", dw, dh, None, None
    dw, dh = fit(w, h, OVERSIZE_MAX_W, float("inf"))
    extra_lines = caption_lines(caption_html, dw) - 1
    return "oversize", dw, dh, dw + 0.8, dh + 1.4 + 0.2 * extra_lines


def prefer_print_render(src):
    """Swap failing.png for failing-print.png when a print re-render exists."""
    base, name = os.path.split(html.unescape(src))
    if name != "failing.png":
        return src
    candidate = os.path.join(base, "failing-print.png")
    if os.path.exists(os.path.join(HERE, candidate)):
        return html.escape(candidate, quote=True)
    return src


def rewrite_img(m):
    attrs = m.group(1)
    attrs = re.sub(r'src="([^"]*)"', lambda s: 'src="%s"' % prefer_print_render(s.group(1)), attrs)
    return "<img" + attrs + ">"


def wrap_figures(html_text):
    """Wrap img + em-caption pairs in <figure>; return (html, extra_css, placements)."""
    pat = re.compile(r"<p>(<img\b[^>]*>)</p>\s*<p><em>(.*?)</em></p>", re.S)
    extra_css = []
    placements = []
    counter = [0]

    def repl(m):
        img, caption = m.group(1), m.group(2)
        src = re.search(r'src="([^"]*)"', img)
        size = image_inches(src.group(1)) if src else None
        if not size:
            return "<figure>%s<figcaption>%s</figcaption></figure>" % (img, caption)
        w, h = size
        placement, dw, dh, pw, ph = place_figure(w, h, caption)
        style = ' style="width: %.2fin; height: %.2fin"' % (dw, dh)
        cls = ""
        if placement == "landscape":
            cls = ' class="wide"'
        elif placement == "oversize":
            counter[0] += 1
            name = "fig-%d" % counter[0]
            extra_css.append(
                "@page %s { size: %.2fin %.2fin; margin: 0.4in; "
                "@bottom-center { content: counter(page); font-size: 9pt; color: #666; } }\n"
                "figure.%s { page: %s; }" % (name, pw, ph, name, name)
            )
            cls = ' class="oversize %s"' % name
        img = img[:-1] + style + ">"
        placements.append((html.unescape(src.group(1)), w, h, placement, dw, dh, pw, ph))
        return "<figure%s>%s<figcaption>%s</figcaption></figure>" % (cls, img, caption)

    out = pat.sub(repl, html_text)
    return out, "\n".join(extra_css), placements


def main():
    with open(SRC, encoding="utf-8") as f:
        md_text = preprocess_markdown(f.read())

    body = markdown.markdown(
        md_text,
        extensions=["tables", "fenced_code", "toc", "sane_lists"],
        output_format="html5",
    )
    body = soft_break_code_in_cells(body)
    body = re.sub(r"<img\b([^>]*?)\s*/?>", rewrite_img, body)
    body, extra_css, placements = wrap_figures(body)

    for src, w, h, placement, dw, dh, pw, ph in placements:
        page = "" if pw is None else " on %.1fx%.1fin page" % (pw, ph)
        print("%-9s %5.1fx%4.1fin -> %5.1fx%4.1fin%s  %s" % (placement, w, h, dw, dh, page, src), file=sys.stderr)

    doc = '<!DOCTYPE html><html><head><meta charset="utf-8"></head><body>%s</body></html>' % body
    HTML(string=doc, base_url=HERE + os.sep).write_pdf(
        OUT, stylesheets=[CSS(string=CSS_TEXT), CSS(string=extra_css)]
    )
    print("wrote", OUT, file=sys.stderr)


if __name__ == "__main__":
    main()
