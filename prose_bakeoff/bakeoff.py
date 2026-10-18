#!/usr/bin/env python3
"""
Blind prose bake-off across Bedrock models.

Sends the *identical* prompt (and optional system prompt) to N models via the
Bedrock Converse API, concurrently, then writes:

  <out>/raw/<model>.md        one file per model, labelled
  <out>/results.json          everything (text, usage, latency, errors)
  <out>/blind.md              outputs shuffled and labelled A, B, C, ... with
                              no model names, for human judging
  <out>/key.json              letter -> model mapping (don't peek until done)

Optionally (--judge MODEL) asks a judge model to rank the blinded outputs
against a clarity rubric and writes <out>/judge.md.

Usage:
  python bakeoff.py --prompt-file prompt.txt [--system-file sys.txt]
                    [--models id1,id2,...] [--max-tokens 4000]
                    [--judge us.anthropic.claude-opus-5-5] [--out runs/001]
"""
import argparse
import concurrent.futures as cf
import json
import os
import random
import re
import sys
import time
from datetime import datetime, timezone

import boto3
from botocore.config import Config

DEFAULT_MODELS = [
    "us.anthropic.claude-fable-5-1",
    "us.anthropic.claude-opus-5-5",
    "us.openai.gpt-6-astra",
    "us.zai.glm-5.3",
]

JUDGE_RUBRIC = """You are judging {n} anonymous responses to the same writing task.
Judge ONLY the quality of the prose for the task's audience. Criteria, in priority order:
1. Clarity: can a reader grasp each sentence on the first pass? Is the structure easy to follow?
2. Concision: no filler, hedging, throat-clearing, or restating the question.
3. Precision: concrete words, specific claims, no vague abstractions or cliché.
4. Fitness for purpose: tone, length, and register match what the task asked for.
5. Correctness of the content, where you can assess it.

Do not reward length. Do not reward enthusiasm or flattery. Do not guess which model wrote what.

For each response give a 1-10 score per criterion and a one-sentence justification, then a final
ranking from best to worst with a short paragraph explaining the decisive differences.
Format: a markdown table of scores, then the ranking.

=== TASK GIVEN TO THE WRITERS ===
{task}

{responses}
"""


def slug(model_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", model_id)


def call_model(rt, model_id, system, prompt, max_tokens, temperature):
    kwargs = {
        "modelId": model_id,
        "messages": [{"role": "user", "content": [{"text": prompt}]}],
        "inferenceConfig": {"maxTokens": max_tokens},
    }
    if temperature is not None:
        kwargs["inferenceConfig"]["temperature"] = temperature
    if system:
        kwargs["system"] = [{"text": system}]

    t0 = time.time()
    try:
        r = rt.converse(**kwargs)
    except Exception as e:  # noqa: BLE001
        return {
            "model": model_id,
            "ok": False,
            "error": f"{type(e).__name__}: {e}",
            "latency_s": round(time.time() - t0, 2),
        }
    latency = time.time() - t0
    blocks = r["output"]["message"]["content"]
    text = "\n".join(b["text"] for b in blocks if "text" in b).strip()
    reasoning = [b for b in blocks if "reasoningContent" in b]
    return {
        "model": model_id,
        "ok": True,
        "text": text,
        "stop_reason": r.get("stopReason"),
        "usage": r.get("usage", {}),
        "had_reasoning_blocks": bool(reasoning),
        "latency_s": round(latency, 2),
        "words": len(text.split()),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--prompt-file", required=True)
    ap.add_argument("--system-file")
    ap.add_argument("--models", default=",".join(DEFAULT_MODELS),
                    help="comma-separated Bedrock model/inference-profile IDs")
    ap.add_argument("--max-tokens", type=int, default=8000,
                    help="generous by default: several of these models spend tokens on hidden reasoning first")
    ap.add_argument("--temperature", type=float, default=None,
                    help="leave unset to use each model's default (safest across vendors)")
    ap.add_argument("--region", default=os.environ.get("AWS_REGION", "us-west-2"))
    ap.add_argument("--out", default=None, help="output dir (default runs/<timestamp>)")
    ap.add_argument("--judge", default=None, help="model ID to use as a blind judge (optional)")
    ap.add_argument("--seed", type=int, default=None, help="shuffle seed for blind labels")
    args = ap.parse_args()

    prompt = open(args.prompt_file, encoding="utf-8").read().strip()
    system = open(args.system_file, encoding="utf-8").read().strip() if args.system_file else None
    models = [m.strip() for m in args.models.split(",") if m.strip()]

    out = args.out or os.path.join(os.path.dirname(os.path.abspath(__file__)), "runs",
                                   datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S"))
    os.makedirs(os.path.join(out, "raw"), exist_ok=True)

    rt = boto3.client("bedrock-runtime", region_name=args.region,
                      config=Config(read_timeout=900, connect_timeout=30, retries={"max_attempts": 3}))

    print(f"Running {len(models)} models concurrently in {args.region} -> {out}", file=sys.stderr)
    results = []
    with cf.ThreadPoolExecutor(max_workers=len(models)) as ex:
        futs = {ex.submit(call_model, rt, m, system, prompt, args.max_tokens, args.temperature): m for m in models}
        for f in cf.as_completed(futs):
            r = f.result()
            status = "ok " if r["ok"] else "ERR"
            if r["ok"]:
                extra = f"{r.get('words', 0):5d} words, stop={r.get('stop_reason')}"
                if r.get("stop_reason") == "max_tokens" and r.get("words", 0) == 0:
                    extra += "  <-- budget consumed by hidden reasoning; raise --max-tokens"
                elif r.get("stop_reason") == "max_tokens":
                    extra += "  <-- truncated; raise --max-tokens"
            else:
                extra = r["error"][:120]
            print(f"  [{status}] {r['model']:40} {r['latency_s']:6.1f}s  {extra}", file=sys.stderr)
            results.append(r)

    results.sort(key=lambda r: models.index(r["model"]))

    # Per-model labelled files
    for r in results:
        with open(os.path.join(out, "raw", slug(r["model"]) + ".md"), "w", encoding="utf-8") as fh:
            fh.write(f"# {r['model']}\n\n")
            if r["ok"]:
                fh.write(r["text"] + "\n")
            else:
                fh.write(f"**ERROR:** {r['error']}\n")

    # Blind file (exclude errors and empty outputs so the judge isn't scoring blanks)
    ok_results = [r for r in results if r["ok"] and r["words"] > 0]
    rnd = random.Random(args.seed)
    order = ok_results[:]
    rnd.shuffle(order)
    letters = [chr(ord("A") + i) for i in range(len(order))]
    key = {L: r["model"] for L, r in zip(letters, order)}

    with open(os.path.join(out, "blind.md"), "w", encoding="utf-8") as fh:
        fh.write("# Blind comparison\n\n")
        fh.write("## Task\n\n")
        if system:
            fh.write("**System prompt:**\n\n```\n" + system + "\n```\n\n")
        fh.write("**User prompt:**\n\n```\n" + prompt + "\n```\n\n")
        for L, r in zip(letters, order):
            fh.write(f"\n---\n\n## Response {L}\n\n")
            fh.write(f"*({r['words']} words)*\n\n")
            fh.write(r["text"] + "\n")

    with open(os.path.join(out, "key.json"), "w", encoding="utf-8") as fh:
        json.dump(key, fh, indent=2)

    meta = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "region": args.region,
        "prompt_file": os.path.abspath(args.prompt_file),
        "system_file": os.path.abspath(args.system_file) if args.system_file else None,
        "max_tokens": args.max_tokens,
        "temperature": args.temperature,
        "models": models,
        "blind_key": key,
        "results": results,
    }
    with open(os.path.join(out, "results.json"), "w", encoding="utf-8") as fh:
        json.dump(meta, fh, indent=2)

    # Optional blind judge
    if args.judge and ok_results:
        responses = "\n\n".join(f"=== RESPONSE {L} ===\n{r['text']}" for L, r in zip(letters, order))
        task_desc = (f"[system prompt]\n{system}\n\n[user prompt]\n{prompt}" if system else prompt)
        jprompt = JUDGE_RUBRIC.format(n=len(order), task=task_desc, responses=responses)
        print(f"Judging with {args.judge} ...", file=sys.stderr)
        jr = call_model(rt, args.judge, None, jprompt, 6000, None)
        with open(os.path.join(out, "judge.md"), "w", encoding="utf-8") as fh:
            fh.write(f"# Blind judgement by {args.judge}\n\n")
            fh.write(jr["text"] if jr["ok"] else f"**ERROR:** {jr['error']}")
            fh.write("\n\n---\n\n## Key\n\n")
            for L in letters:
                fh.write(f"- **{L}** = `{key[L]}`\n")
        meta["judge"] = {"model": args.judge, "result": jr}
        with open(os.path.join(out, "results.json"), "w", encoding="utf-8") as fh:
            json.dump(meta, fh, indent=2)
        print(("  judge ok" if jr["ok"] else f"  judge ERR {jr['error'][:120]}"), file=sys.stderr)

    print(f"\nWrote:\n  {out}/blind.md   (read this first)\n  {out}/key.json\n  {out}/results.json", file=sys.stderr)
    if args.judge:
        print(f"  {out}/judge.md", file=sys.stderr)


if __name__ == "__main__":
    main()
