"""Single-workspace serving entrypoint for Kubernetes."""

import os
import fcntl
import re
import shutil
from pathlib import Path
from urllib.parse import urlsplit

import uvicorn

from agent_harness.config import HarnessConfig, make_client
from agent_harness.interfaces.web.app import create_app
from agent_harness.interfaces.web.host import frontend_directory


def build_app():
    user = os.environ["HARNESS_LOGIN_USER"]
    password = os.environ["HARNESS_LOGIN_PASSWORD"]
    if not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", user) or not re.fullmatch(r"[A-Za-z0-9_-]{24,128}", password):
        raise ValueError("Set a valid login user and a random URL-safe password of at least 24 characters")

    origin = os.environ.get("HARNESS_PUBLIC_ORIGIN", "").rstrip("/")
    hosts = ["localhost", "127.0.0.1"]
    if origin:
        address = urlsplit(origin)
        if (address.scheme != "https" or not address.hostname or "*" in origin
                or address.path or address.query or address.fragment or address.username
                or address.password):
            raise ValueError("HARNESS_PUBLIC_ORIGIN must be one HTTPS origin")
        hosts.append(address.hostname)

    config = HarnessConfig.from_env(local=False)
    if config.cron_service_url and (not config.cron_token or not config.cron_scope):
        raise ValueError("CRON_API_TOKEN and CRON_SCOPE are required for Kubernetes Cron")
    assets = Path(__file__).parent / "skills"
    if assets.is_dir() and not (config.workdir / "skills").exists():
        shutil.copytree(assets, config.workdir / "skills")
    output = frontend_directory() / "out"
    if not (output / "index.html").is_file():
        raise RuntimeError("The image is missing the frontend export")
    return create_app(config, make_client, frontend_directory=output,
                      allowed_hosts=hosts, frontend_origins=(origin,) if origin else (),
                      login=(user, password))


if __name__ == "__main__":
    with Path(".agent-process.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit("Another Agent process owns this workspace") from None
        uvicorn.run(build_app(), host="0.0.0.0", port=8000, workers=1,
                    proxy_headers=False, timeout_graceful_shutdown=30)
