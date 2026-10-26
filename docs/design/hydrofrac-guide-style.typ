#set par(justify: false)
#set text(size: 9.5pt)
#show raw.where(block: true): set text(size: 6.8pt)
#show raw.where(block: true): block.with(fill: luma(246), inset: 5pt, radius: 2pt, width: 100%)
#show raw.where(block: false): set text(size: 1.05em)
#set table(align: left, inset: 4pt, stroke: 0.4pt + luma(200))
#show table: set text(size: 8pt)
#show heading.where(level: 1): set block(above: 1.6em, below: 0.8em)
#show raw.where(block: false): r => { show "_": "_" + sym.zws; show "/": "/" + sym.zws; r }
