#set par(justify: false)
#set text(size: 9.5pt)
#show raw.where(block: true): set text(size: 6.8pt)
#show raw.where(block: true): block.with(fill: luma(246), inset: 5pt, radius: 2pt, width: 100%)
#show raw.where(block: false): set text(size: 1.05em)
#set table(align: left, inset: 4pt, stroke: 0.4pt + luma(200))
#show table: set text(size: 8pt)
#show heading.where(level: 1): set block(above: 1.6em, below: 0.8em)
#show raw.where(block: false): r => { show "_": "_" + sym.zws; show "/": "/" + sym.zws; r }
// Additions for reliable-send-three-tools: figures and long tables may break across pages
// (multi-panel figures put one plot per row at full width), and captions are set smaller.
#show figure: set block(breakable: true)
#show figure.caption: set text(size: 8.5pt)
#set figure(gap: 0.8em)
#show figure.where(kind: image): set block(above: 1.4em, below: 1.4em)
