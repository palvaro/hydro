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
* adds class ``wide`` to figures whose image aspect ratio exceeds 1.6 so the
  stylesheet puts them on their own landscape page.
"""

import html
import os
import re
import sys

import markdown
from PIL import Image
from weasyprint import HTML, CSS

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "hydrofrac-inventory.md")
OUT = os.path.join(HERE, "hydrofrac-inventory.pdf")

WIDE_ASPECT = 1.6

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
figure.wide { page: landscape; page-break-before: always; page-break-after: always; } figure.wide img { max-height: 6.8in; }
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


def image_size(src):
    path = os.path.join(HERE, html.unescape(src))
    try:
        with Image.open(path) as im:
            return im.size
    except OSError:
        return None


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
    pat = re.compile(r"<p>(<img\b[^>]*>)</p>\s*<p><em>(.*?)</em></p>", re.S)

    def repl(m):
        img, caption = m.group(1), m.group(2)
        src = re.search(r'src="([^"]*)"', img)
        cls = ""
        if src:
            size = image_size(src.group(1))
            if size and size[1] and size[0] / size[1] > WIDE_ASPECT:
                cls = ' class="wide"'
        return "<figure%s>%s<figcaption>%s</figcaption></figure>" % (cls, img, caption)

    return pat.sub(repl, html_text)


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
    body = wrap_figures(body)

    doc = '<!DOCTYPE html><html><head><meta charset="utf-8"></head><body>%s</body></html>' % body
    HTML(string=doc, base_url=HERE + os.sep).write_pdf(OUT, stylesheets=[CSS(string=CSS_TEXT)])
    print("wrote", OUT, file=sys.stderr)


if __name__ == "__main__":
    main()
