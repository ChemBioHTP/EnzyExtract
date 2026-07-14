#!/usr/bin/env python3
"""Small live probe for GPT-5.6 Responses API and optional built-in tools."""
from __future__ import annotations

import argparse
import json
import os

from dotenv import load_dotenv
from openai import OpenAI


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", default="gpt-5.6-sol")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--enable-web-search", action="store_true")
    args = parser.parse_args()

    load_dotenv(args.env_file)
    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY is required; this probe must be a live API call.")

    client = OpenAI()
    request = {
        "model": args.model,
        "instructions": (
            "Answer only from the supplied validation text unless a tool is explicitly needed. "
            "For extraction scoring, external facts are not allowed."
        ),
        "input": (
            "Validation probe: say whether Responses API is reachable. "
            "Return compact JSON with keys reachable and tool_policy."
        ),
        "max_output_tokens": 128,
    }
    if args.enable_web_search:
        request["tools"] = [{"type": "web_search"}]

    response = client.responses.create(**request)
    output = {
        "model": response.model,
        "response_id": response.id,
        "output_text": response.output_text,
        "tool_probe_enabled": args.enable_web_search,
    }
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
