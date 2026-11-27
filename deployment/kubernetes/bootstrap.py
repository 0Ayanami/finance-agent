"""Create or reuse deployment secrets without passing values on the command line."""

import argparse
import hashlib
import json
import secrets
import subprocess
from pathlib import Path

from dotenv import dotenv_values


NAMESPACE = "ayanami"
PROVIDER_KEYS = {
    "MODEL_ID", "FALLBACK_MODEL_ID", "ANTHROPIC_BASE_URL", "ANTHROPIC_API_KEY",
    "OPENAI_BASE_URL", "OPENAI_API_KEY", "EMBEDDING_API_KEY", "EMBEDDING_BASE_URL",
    "EMBEDDING_MODEL", "EMBEDDING_DIMENSIONS", "DASHSCOPE_API_KEY", "RERANK_BASE_URL",
    "RERANK_API_KEY", "RERANK_ENABLED", "RERANK_MODEL", "RERANK_CANDIDATES",
    "RERANK_READ_TIMEOUT", "MILVUS_COLLECTION_PREFIX",
}


def bootstrap(context, provider_env, public_origin):
    def kubectl(*arguments, data=None):
        return subprocess.run(
            ["kubectl", "--context", context, "--request-timeout=20s", *arguments],
            input=json.dumps(data) if data is not None else None,
            text=True, check=True, capture_output=True, encoding="utf-8",
        ).stdout

    def create_secret(name, values):
        if kubectl("-n", NAMESPACE, "get", "secret", name, "--ignore-not-found", "-o", "name").strip():
            print(f"Reused Secret {name}")
            return
        kubectl("create", "-f", "-", data={
            "apiVersion": "v1", "kind": "Secret",
            "metadata": {"name": name, "namespace": NAMESPACE}, "type": "Opaque",
            "stringData": values,
        })
        print(f"Created Secret {name}")

    provider = dotenv_values(provider_env, interpolate=False)
    settings = {key: provider[key] for key in PROVIDER_KEYS if provider.get(key)}
    if not {"MODEL_ID", "ANTHROPIC_API_KEY"} <= settings.keys():
        raise SystemExit("The provider file must define MODEL_ID and ANTHROPIC_API_KEY")

    kubectl("apply", "-f", str(Path(__file__).with_name("namespace.yaml")))
    create_secret("agent-settings", {
        **settings, "HARNESS_LOGIN_USER": "owner", "HARNESS_LOGIN_PASSWORD": secrets.token_urlsafe(32),
        "HARNESS_PUBLIC_ORIGIN": public_origin, "CRON_API_TOKEN": secrets.token_urlsafe(32),
        "CRON_SCOPE": hashlib.sha256(b"ayanami/kubernetes").hexdigest()[:32],
    })
    create_secret("redis-auth", {"password": secrets.token_urlsafe(32)})
    access, secret = secrets.token_hex(10), secrets.token_hex(24)
    create_secret("milvus-storage", {
        "MINIO_ROOT_USER": access, "MINIO_ROOT_PASSWORD": secret,
        "MINIO_ACCESS_KEY_ID": access, "MINIO_SECRET_ACCESS_KEY": secret,
    })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--provider-env", type=Path, default=Path(".env"))
    parser.add_argument("--public-origin", default="")
    options = parser.parse_args()
    try:
        bootstrap(options.context, options.provider_env, options.public_origin)
    except subprocess.CalledProcessError as error:
        # kubectl can include invalid Secret fields in diagnostics.
        raise SystemExit(f"Kubernetes rejected {error.cmd[4]} (exit {error.returncode})") from None
