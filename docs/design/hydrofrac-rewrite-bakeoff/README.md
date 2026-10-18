# Hydrofrac design-doc rewrite: blind model bake-off

Four Bedrock models were given the identical prompt (`hydrofrac_prompt.txt`, with
`hydrofrac_system.txt` as the system prompt): rewrite `docs/design/hydrofrac.md` at commit
`8d6e7e053e` for a Hydro-familiar audience, in the style of the author's own writing sample.

- `draft_A.md` / `.pdf`, `draft_B.md` / `.pdf`, `draft_C.md` / `.pdf` — the three complete
  rewrites, blinded (letters only). `key.json` maps letters to models.
- `judgement.md` — a blind ranking by a non-contestant model (`us.openai.gpt-6.1-sol`) against a
  clarity/concision/precision rubric; the key is appended at the end. `judgement.pdf` is the same
  with the key removed.
- `blind.md` — all three drafts in one shuffled file, as presented for human judging.
- `results.json` — per-model usage, latency, stop reasons, and the blind key.
- `bakeoff.py` — the harness (Bedrock Converse, concurrent, blinding, optional judge).
  `md2pdf_rl.py` renders the Markdown to PDF with ReportLab.

A fourth contestant, `us.zai.glm-5.3`, produced no usable draft: it consumed the entire output
budget (20K, then 60K tokens) on hidden reasoning and was truncated mid-document.

Run: 2026-10-18, us-west-2, no temperature set (vendor defaults), seed 42 for the shuffle.
