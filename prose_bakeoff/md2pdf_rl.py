#!/usr/bin/env python3
"""Markdown -> PDF via python-markdown + ReportLab (platypus).

Supports headings, paragraphs, bold/italic/inline code, fenced code blocks,
bullet/numbered lists, horizontal rules, and pipe tables.

Usage: md2pdf_rl.py INPUT.md OUTPUT.pdf [TITLE]
"""
import html
import re
import sys
from xml.etree import ElementTree as ET

import markdown
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (HRFlowable, ListFlowable, ListItem, PageBreak, Paragraph,
                                Preformatted, SimpleDocTemplate, Spacer, Table, TableStyle,
                                KeepTogether)

styles = getSampleStyleSheet()
BODY = ParagraphStyle("body", parent=styles["BodyText"], fontName="Times-Roman", fontSize=10.5,
                      leading=14, spaceAfter=6, alignment=TA_LEFT)
H = {
    1: ParagraphStyle("h1", parent=styles["Heading1"], fontName="Helvetica-Bold", fontSize=17, leading=21, spaceBefore=10, spaceAfter=8),
    2: ParagraphStyle("h2", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=13.5, leading=17, spaceBefore=12, spaceAfter=5),
    3: ParagraphStyle("h3", parent=styles["Heading3"], fontName="Helvetica-Bold", fontSize=11.5, leading=15, spaceBefore=9, spaceAfter=4),
    4: ParagraphStyle("h4", parent=styles["Heading4"], fontName="Helvetica-BoldOblique", fontSize=10.5, leading=14, spaceBefore=7, spaceAfter=3),
}
CODE = ParagraphStyle("code", fontName="Courier", fontSize=7.8, leading=9.6, leftIndent=6,
                      backColor=colors.whitesmoke, borderPadding=4, spaceBefore=4, spaceAfter=8)
CELL = ParagraphStyle("cell", parent=BODY, fontSize=8.5, leading=10.5, spaceAfter=0)
CELL_HDR = ParagraphStyle("cellh", parent=CELL, fontName="Helvetica-Bold")


def inner_html(el: ET.Element) -> str:
    """Serialize children of el as ReportLab-compatible inline markup."""
    parts = [html.escape(el.text or "", quote=False)]
    for child in el:
        tag = child.tag.lower()
        txt = inner_html(child)
        if tag in ("strong", "b"):
            parts.append(f"<b>{txt}</b>")
        elif tag in ("em", "i"):
            parts.append(f"<i>{txt}</i>")
        elif tag == "code":
            parts.append(f'<font face="Courier" size="9">{txt}</font>')
        elif tag == "a":
            parts.append(f'<u>{txt}</u>')
        elif tag == "br":
            parts.append("<br/>")
        else:
            parts.append(txt)
        parts.append(html.escape(child.tail or "", quote=False))
    return "".join(parts)


def text_of(el: ET.Element) -> str:
    return "".join(el.itertext())


def build_list(el: ET.Element, ordered: bool, depth: int = 0):
    items = []
    for li in el.findall("li"):
        # paragraph text directly inside li (before any nested list)
        head = ET.Element("span")
        head.text = li.text
        nested = []
        for child in li:
            if child.tag in ("ul", "ol"):
                nested.append(child)
            else:
                head.append(child)
        flow = [Paragraph(inner_html(head).strip() or " ", BODY)]
        for n in nested:
            flow.append(build_list(n, n.tag == "ol", depth + 1))
        items.append(ListItem(flow, leftIndent=12 + depth * 12))
    return ListFlowable(items, bulletType="1" if ordered else "bullet", start=None if not ordered else 1,
                        leftIndent=14 + depth * 12, bulletFontSize=8)


def build_table(el: ET.Element, avail_width: float):
    rows = []
    header_rows = 0
    for sec in el:
        for tr in sec.findall("tr"):
            cells = []
            for td in tr:
                style = CELL_HDR if td.tag == "th" else CELL
                cells.append(Paragraph(inner_html(td).strip(), style))
            rows.append(cells)
            if sec.tag == "thead":
                header_rows += 1
    if not rows:
        return Spacer(1, 1)
    ncols = max(len(r) for r in rows)
    for r in rows:
        while len(r) < ncols:
            r.append(Paragraph("", CELL))
    col_w = avail_width / ncols
    t = Table(rows, colWidths=[col_w] * ncols, repeatRows=header_rows)
    t.setStyle(TableStyle([
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("BACKGROUND", (0, 0), (-1, header_rows - 1 if header_rows else 0), colors.Color(0.92, 0.92, 0.92)),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4), ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 2), ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]))
    return t


def md_to_flowables(md_text: str, avail_width: float):
    html_text = markdown.markdown(md_text, extensions=["fenced_code", "tables", "sane_lists"])
    root = ET.fromstring(f"<root>{html_text}</root>")
    flow = []
    for el in root:
        tag = el.tag.lower()
        if re.fullmatch(r"h[1-6]", tag):
            lvl = min(int(tag[1]), 4)
            flow.append(Paragraph(inner_html(el), H[lvl]))
        elif tag == "p":
            flow.append(Paragraph(inner_html(el), BODY))
        elif tag == "pre":
            code = text_of(el).rstrip("\n")
            flow.append(Preformatted(code, CODE))
        elif tag == "ul":
            flow.append(build_list(el, False))
            flow.append(Spacer(1, 4))
        elif tag == "ol":
            flow.append(build_list(el, True))
            flow.append(Spacer(1, 4))
        elif tag == "hr":
            flow.append(HRFlowable(width="100%", thickness=0.6, color=colors.grey, spaceBefore=6, spaceAfter=8))
        elif tag == "table":
            flow.append(build_table(el, avail_width))
            flow.append(Spacer(1, 6))
        elif tag == "blockquote":
            for p in el.findall("p"):
                flow.append(Paragraph(inner_html(p), ParagraphStyle("bq", parent=BODY, leftIndent=18, textColor=colors.Color(0.25, 0.25, 0.25))))
        else:
            txt = inner_html(el).strip()
            if txt:
                flow.append(Paragraph(txt, BODY))
    return flow


def main():
    src, dst = sys.argv[1], sys.argv[2]
    title = sys.argv[3] if len(sys.argv) > 3 else src
    md_text = open(src, encoding="utf-8").read()
    doc = SimpleDocTemplate(dst, pagesize=letter, leftMargin=0.9 * inch, rightMargin=0.9 * inch,
                            topMargin=0.85 * inch, bottomMargin=0.85 * inch, title=title, author="Infinity")
    avail = letter[0] - doc.leftMargin - doc.rightMargin

    def footer(canvas, d):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.grey)
        canvas.drawCentredString(letter[0] / 2, 0.5 * inch, f"{title}  ·  {d.page}")
        canvas.restoreState()

    doc.build(md_to_flowables(md_text, avail), onFirstPage=footer, onLaterPages=footer)
    print(dst)


if __name__ == "__main__":
    main()
