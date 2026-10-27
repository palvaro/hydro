-- Pandoc filter for the PDF build of reliable-send-three-tools.md (typst output only).
--
-- * Points each figure at its vector (SVG) source in reliable-send-three-tools-img/.
-- * Rebuilds each multi-panel PNG (a side-by-side or 2x2 montage) as a numbered figure whose
--   panels are the individual SVG plots, one per row at full text width, labelled (a), (b), ...
--   and rewrites "left", "top right" and similar in the caption to match the labels.
-- * Turns an image that starts a paragraph into a figure followed by the rest of the paragraph.

local IMG = "reliable-send-three-tools-img/"

-- Montage PNG -> panels in reading order (left to right, then top to bottom), with labels.
local composites = {
  ["v0-f8-vs-f64.png"] = {
    { "v0-f8-resend-1s.svg", "f = 8" },
    { "v0-f64-resend-1s.svg", "f = 64, trial 1" },
  },
  ["v0-f8-period-1s-vs-20s.png"] = {
    { "v0-f8-resend-1s.svg", "resend every 1 s" },
    { "v0-f8-resend-20s.svg", "resend every 20 s" },
  },
  ["compare-f2-all-variants.png"] = {
    { "compare-f2-v0.svg", "V0" },
    { "compare-f2-v1.svg", "V1" },
    { "compare-f2-v2.svg", "V2" },
    { "compare-f2-v3.svg", "V3" },
  },
  ["compare-f1.1-budgeted.png"] = {
    { "compare-f1.1-v1.svg", "V1" },
    { "compare-f1.1-v2.svg", "V2" },
    { "compare-f1.1-v3.svg", "V3" },
  },
  ["act3-longtail-f4.png"] = {
    { "act3-longtail-f4-v0.svg", "V0", "8.3in" },
    { "act3-longtail-f4-v2.svg", "V2", "8.3in" },
    { "act3-longtail-f4-v1.svg", "V1", "8.3in" },
    { "act3-longtail-f4-v3.svg", "V3", "8.3in" },
  },
}

local positions = {
  { "top left", "a" }, { "top right", "b" }, { "bottom left", "c" }, { "bottom right", "d" },
  { "%(left", "(a" }, { "%(right", "(b" },
}

local function exists(path)
  local f = io.open(path, "r")
  if f then f:close() return true end
  return false
end

local function to_typst(blocks)
  local out = pandoc.write(pandoc.Pandoc(blocks), "typst")
  return (out:gsub("%s+$", ""))
end

local function typst_str(s)
  return '"' .. s:gsub("\\", "\\\\"):gsub('"', '\\"') .. '"'
end

local function first_image(fig)
  local found
  fig:walk({ Image = function(img) found = found or img end })
  return found
end

local function figure_block(fig)
  local img = first_image(fig)
  if not img then return nil end
  local name = img.src:match("([^/]+)$")
  local panels = composites[name]
  local caption = to_typst(fig.caption.long)
  if not panels then
    local src = img.src:gsub("%.png$", ".svg")
    if not exists(src) then src = img.src end
    -- Small figures (sequence diagrams, Cress plots) float; full-page Sedge plots stay in place,
    -- because typst 0.12 overflows a page when a tall block follows a float.
    local small = name:match("^hydrofrac") or name:match("^cress")
    return pandoc.RawBlock("typst", string.format(
      "#figure(image(%s, width: 100%%), placement: %s,\n  caption: [%s])",
      typst_str(src), small and "auto" or "none", caption))
  end
  for _, p in ipairs(positions) do caption = caption:gsub(p[1], p[2]) end
  local cells = {}
  for i, p in ipairs(panels) do
    local letter = string.char(96 + i)
    local size = p[3] and ("height: " .. p[3]) or "width: 100%"
    table.insert(cells, string.format(
      "block(breakable: false, width: 100%%)[#align(center)[#image(%s, %s)]\n#align(center, text(size: 8.5pt)[(%s) %s])]",
      typst_str(IMG .. p[1]), size, letter, p[2]))
  end
  local raw = string.format(
    "#figure(kind: image, placement: none,\n  grid(columns: 1, row-gutter: 1.2em,\n    %s),\n  caption: [%s])",
    table.concat(cells, ",\n    "), caption)
  return pandoc.RawBlock("typst", raw)
end

-- Split "![caption](img) more text" into a figure and a paragraph.
local function split_para(para)
  local c = para.content
  if #c > 1 and c[1].t == "Image" then
    local img = c[1]
    local rest = pandoc.List({ table.unpack(c, 2) })
    while #rest > 0 and (rest[1].t == "Space" or rest[1].t == "SoftBreak") do rest:remove(1) end
    local cap = img.caption
    img.caption = {}
    local fig = pandoc.Figure(pandoc.Plain({ img }), { long = { pandoc.Plain(cap) } })
    return { fig, pandoc.Para(rest) }
  end
end

return {
  { Para = split_para },
  { Figure = figure_block },
}
