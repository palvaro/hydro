"""Render design_docs/2026-10_simulation_cost_and_hydroification.md to PDF (minimal markdown subset)."""
import re
import sys

from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
    ListFlowable,
    ListItem,
)

src, out = sys.argv[1], sys.argv[2]
lines = open(src, encoding="utf-8").read().splitlines()

ss = getSampleStyleSheet()
body = ParagraphStyle("body", parent=ss["BodyText"], fontName="Helvetica", fontSize=10, leading=13.5, spaceAfter=6)
h1 = ParagraphStyle("h1", parent=ss["Heading1"], fontName="Helvetica-Bold", fontSize=16, leading=20, spaceAfter=8)
h2 = ParagraphStyle("h2", parent=ss["Heading2"], fontName="Helvetica-Bold", fontSize=12.5, leading=16, spaceBefore=10, spaceAfter=5)
cell = ParagraphStyle("cell", parent=body, fontSize=8.5, leading=11, spaceAfter=0)
cellb = ParagraphStyle("cellb", parent=cell, fontName="Helvetica-Bold")


def inline(s):
    s = s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    s = re.sub(r"`([^`]*)`", r'<font face="Courier" size="8.5">\1</font>', s)
    s = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", s)
    s = re.sub(r"(?<![\w*])\*(?!\*)(.+?)(?<!\*)\*(?![\w*])", r"<i>\1</i>", s)
    return s


story = []
para_buf = []
table_buf = []
list_buf = []
list_kind = None


def flush_para():
    global para_buf
    if para_buf:
        story.append(Paragraph(inline(" ".join(para_buf)), body))
        para_buf = []


def flush_table():
    global table_buf
    if not table_buf:
        return
    rows = [r for r in table_buf if not re.match(r"^\s*\|?\s*:?-{2,}", r)]
    data = []
    for i, r in enumerate(rows):
        cells = [c.strip() for c in r.strip().strip("|").split("|")]
        style = cellb if i == 0 else cell
        data.append([Paragraph(inline(c), style) for c in cells])
    ncols = max(len(r) for r in data)
    avail = 7.0 * inch
    header = [c.strip() for c in rows[0].strip().strip("|").split("|")]
    if header[:1] == ["Hook"]:
        widths = [1.25 * inch, 2.0 * inch, 1.9 * inch, 1.85 * inch]
    elif ncols == 4:
        widths = [2.9 * inch, 1.1 * inch, 2.0 * inch, 1.0 * inch]
    else:
        widths = [avail / ncols] * ncols
    t = Table(data, colWidths=widths, repeatRows=1)
    t.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e8e8e8")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 4),
                ("RIGHTPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ]
        )
    )
    story.append(t)
    story.append(Spacer(1, 6))
    table_buf = []


def flush_list():
    global list_buf, list_kind
    if list_buf:
        items = [ListItem(Paragraph(inline(x), body), leftIndent=12) for x in list_buf]
        story.append(
            ListFlowable(items, bulletType="1" if list_kind == "ol" else "bullet", start="1" if list_kind == "ol" else None, leftIndent=14, bulletFontSize=9)
        )
        list_buf = []
        list_kind = None


for raw in lines:
    line = raw.rstrip()
    if line.startswith("|"):
        flush_para(); flush_list()
        table_buf.append(line)
        continue
    else:
        flush_table()
    if not line.strip():
        flush_para(); flush_list()
        continue
    if line.startswith("# "):
        flush_para(); flush_list()
        story.append(Paragraph(inline(line[2:]), h1))
        continue
    if line.startswith("## "):
        flush_para(); flush_list()
        story.append(Paragraph(inline(line[3:]), h2))
        continue
    m_ol = re.match(r"^\s*(\d+)\.\s+(.*)", line)
    m_ul = re.match(r"^\s*[-*]\s+(.*)", line)
    if m_ol or m_ul:
        flush_para()
        kind = "ol" if m_ol else "ul"
        if list_kind and list_kind != kind:
            flush_list()
        list_kind = kind
        list_buf.append((m_ol or m_ul).group(m_ol and 2 or 1))
        continue
    if list_buf and line.startswith("  "):
        list_buf[-1] += " " + line.strip()
        continue
    flush_list()
    para_buf.append(line.strip())

flush_para(); flush_table(); flush_list()

doc = SimpleDocTemplate(out, pagesize=letter, leftMargin=0.75 * inch, rightMargin=0.75 * inch, topMargin=0.75 * inch, bottomMargin=0.75 * inch,
                        title="Does Hydro-ifying a state machine make simulation cheaper?", author="Infinity")
doc.build(story)
print("wrote", out)
