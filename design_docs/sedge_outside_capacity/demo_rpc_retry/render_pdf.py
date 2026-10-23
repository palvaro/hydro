"""Renders a Markdown file, with its images, to PDF: Markdown (CommonMark + tables, via
markdown-it-py) → HTML → fpdf2.

usage: python3 render_pdf.py IN.md OUT.pdf FONT_DIR

FONT_DIR must hold DejaVuSans{,-Bold,-Oblique}.ttf and DejaVuSansMono.ttf (matplotlib ships them
in `mpl-data/fonts/ttf`), so that μ, ×, → and the box rules print.
"""

import html
import os
import re
import sys

from markdown_it import MarkdownIt
from fpdf import FPDF
from PIL import Image


def main():
    src, out, fonts = sys.argv[1], sys.argv[2], sys.argv[3]
    base = os.path.dirname(os.path.abspath(src))
    text = open(src, encoding="utf-8").read()
    body = MarkdownIt("commonmark").enable("table").render(text)

    # Images are placed by hand (below), so that one that does not fit starts a new page.
    body = re.sub(r'<p><img src="([^"]+)" alt="[^"]*" ?/?></p>', r"\n@@IMG \1@@\n", body)
    # Links: keep the text (fpdf2 prints hrefs fine, but relative ones are noise in a PDF).
    body = re.sub(r'<a href="#[^"]*">(.*?)</a>', r"\1", body)
    # Loose list items wrap their text in <p>, which fpdf2 renders without the bullet.
    body = re.sub(r"<li>\s*<p>", "<li>", body)
    body = re.sub(r"</p>\s*</li>", "</li>", body)
    body = re.sub(r"</p>\s*<p>", "<br><br>", body)
    # Tables: fpdf2 wants a border and a header row.
    body = body.replace("<table>", '<table border="1" width="100%">')
    # Code blocks: keep leading spaces (fpdf2 collapses them), in a smaller monospace.
    def keep_indent(m):
        code = re.sub(r"(?m)^( +)", lambda s: "\u00a0" * len(s.group(1)), m.group(0))
        return code

    body = re.sub(r"<pre><code[^>]*>.*?</code></pre>", keep_indent, body, flags=re.S)
    # Code blocks: smaller monospace.
    body = body.replace("<pre><code", '<pre><font size="6"><code').replace(
        "</code></pre>", "</code></font></pre>"
    )

    pdf = FPDF(format="A4")
    pdf.set_margins(15, 15, 15)
    pdf.set_auto_page_break(True, margin=15)
    pdf.add_font("DejaVu", "", os.path.join(fonts, "DejaVuSans.ttf"))
    pdf.add_font("DejaVu", "B", os.path.join(fonts, "DejaVuSans-Bold.ttf"))
    pdf.add_font("DejaVu", "I", os.path.join(fonts, "DejaVuSans-Oblique.ttf"))
    pdf.add_font("DejaVu", "BI", os.path.join(fonts, "DejaVuSans-BoldOblique.ttf"))
    pdf.add_font("DejaVuMono", "", os.path.join(fonts, "DejaVuSansMono.ttf"))
    pdf.add_font("DejaVuMono", "B", os.path.join(fonts, "DejaVuSansMono-Bold.ttf"))
    pdf.add_font("DejaVuMono", "I", os.path.join(fonts, "DejaVuSansMono-Oblique.ttf"))
    pdf.add_font("DejaVuMono", "BI", os.path.join(fonts, "DejaVuSansMono-BoldOblique.ttf"))
    pdf.add_page()
    pdf.set_font("DejaVu", size=9.5)
    width = pdf.w - pdf.l_margin - pdf.r_margin
    for i, chunk in enumerate(re.split(r"@@IMG ([^@]+)@@", body)):
        if i % 2 == 0:
            if chunk.strip():
                pdf.write_html(
                    chunk, font_family="DejaVu", pre_code_font="DejaVuMono", ul_bullet_char="•"
                )
            continue
        path = os.path.join(base, html.unescape(chunk))
        with Image.open(path) as im:
            w, h = im.size
        # At most the text width, and at most 70 % of a page's height.
        draw_w = min(width, (pdf.h - 30) * 0.7 * w / h)
        draw_h = draw_w * h / w
        if pdf.get_y() + draw_h > pdf.h - pdf.b_margin:
            pdf.add_page()
        pdf.image(path, x=pdf.l_margin + (width - draw_w) / 2, w=draw_w)
        pdf.ln(3)
    pdf.output(out)
    print(f"{out}: {pdf.page_no()} pages")


if __name__ == "__main__":
    main()
