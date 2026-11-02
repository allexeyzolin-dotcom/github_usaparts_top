"""Private, persistent session keys for installations using example settings."""
import os
import secrets
import tempfile
from pathlib import Path


def session_secret(configured, backup_dir, *, production=False):
    value = str(configured or "")
    unsafe = len(value) < 20 or value in {"dev-secret", "change-me-to-a-long-random-secret"}
    if not production or not unsafe:
        return value or "dev-secret"
    path = Path(backup_dir) / ".session-secret.key"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".session-key-", delete=False) as handle:
            temporary = handle.name
            os.chmod(temporary, 0o600)
            handle.write(secrets.token_urlsafe(48).encode("ascii"))
            handle.flush()
            os.fsync(handle.fileno())
        try:
            try:
                os.link(temporary, path)
            except FileExistsError:
                pass
        finally:
            os.unlink(temporary)
    value = path.read_text(encoding="ascii").strip()
    if len(value) < 40:
        raise RuntimeError("Invalid persistent session key; restore it from the private backup")
    return value
