"""One-time setup from the container console. No credentials go to stdout."""

import argparse
import getpass
import json
import os
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path


def write_credentials(target):
    if not sys.stdin.isatty():
        raise SystemExit("Виконайте команду в інтерактивній консолі Portainer.")
    email = input("Email Avto.pro: ").strip()
    password = getpass.getpass("Пароль Avto.pro: ")
    site_key = getpass.getpass("Ключ імпорту USAparts.top: ").strip()
    if (not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email) or not password
            or not re.fullmatch(r"[A-Za-z0-9_-]{43}", site_key)):
        raise SystemExit("Перевірте email, пароль і ключ.")
    os.umask(0o077)
    temporary = target.with_suffix(".tmp")
    temporary.write_text(json.dumps({"email": email, "password": password, "site_key": site_key}), encoding="utf-8")
    temporary.chmod(0o600)
    temporary.replace(target)


def configure():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=("preview", "sync", "paused"))
    args = parser.parse_args()
    target = Path("/data/credentials.json")
    if args.mode is None:
        write_credentials(target)
    elif args.mode != "paused" and not target.is_file():
        raise SystemExit("Спершу запустіть python configure.py для збереження даних входу.")
    mode = args.mode or "preview"
    origin = "http://127.0.0.1:8090"
    with urllib.request.urlopen(origin + "/", timeout=5) as response:
        console = response.read().decode("utf-8")
    csrf = re.search(r'name="csrf" value="([A-Za-z0-9_-]+)"', console)
    if not csrf:
        raise SystemExit("Дані збережено. Консоль сервісу ще не готова.")
    data = urllib.parse.urlencode({"csrf": csrf[1], "mode": mode}).encode("ascii")
    with urllib.request.urlopen(origin + "/mode", data=data, timeout=5):
        pass
    print("Режим оновлено. Стан перевірки: curl -fsS http://127.0.0.1:8090/status")


if __name__ == "__main__":
    configure()
