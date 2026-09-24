"""Send the example through the gateway with the same SDK API as discussion #18.

Install the optional client separately: python -m pip install typesafe-sdk
The gateway itself does not depend on this package.
"""

import argparse
import asyncio
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8080")
    parser.add_argument("--request", type=Path, default=Path(__file__).with_name("request.json"))
    parser.add_argument("--api-key", default="local")
    parser.add_argument("--mode", choices=("sync", "async"), default="async")
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args()
    from typesafe_sdk import AsyncTypeSafeClient, Choice, Noul, RetryPolicy, Score, TypeSafeClient

    request = json.loads(args.request.read_text(encoding="utf-8"))
    classes = {"noul": Noul, "choice": Choice, "score": Score}
    questions = {
        name: classes[spec["type"]](**{key: value for key, value in spec.items() if key != "type"})
        for name, spec in request["questions"].items()
    }
    settings = {
        "api_key": args.api_key,
        "base_url": args.base_url,
        "model": request.get("model", "kev-latest"),
        "retry": RetryPolicy(max_retries=0, timeout=args.timeout),
    }
    payload = {
        "state": request["state"],
        "questions": questions,
        "extra_body": {
            key: value for key, value in request.items() if key not in {"state", "questions", "model"}
        },
    }

    async def send_async():
        async with AsyncTypeSafeClient(**settings) as client:
            response = await client.system_one(**payload)
            return response.raw_http_response.json()

    if args.mode == "async":
        body = asyncio.run(send_async())
    else:
        with TypeSafeClient(**settings) as client:
            body = client.system_one(**payload).raw_http_response.json()
    print(json.dumps(body, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
