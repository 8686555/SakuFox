import json
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WEB_DIR = ROOT / "web"
LITERAL_I18N_KEY = re.compile(r"i18n\.t\(\s*['\"]([A-Za-z0-9_]+)['\"]")
DATA_I18N_KEY = re.compile(r"data-i18n(?:-title)?=['\"]([A-Za-z0-9_]+)['\"]")


def _load_language_pack(lang: str) -> dict[str, str]:
    return json.loads((WEB_DIR / "lang" / f"{lang}.json").read_text(encoding="utf-8"))


def _used_frontend_i18n_keys() -> dict[str, set[str]]:
    keys_by_source: dict[str, set[str]] = {}
    for path in sorted(WEB_DIR.glob("*")):
        if path.suffix not in {".html", ".js"}:
            continue
        text = path.read_text(encoding="utf-8")
        keys = {
            *(match.group(1) for match in LITERAL_I18N_KEY.finditer(text)),
            *(match.group(1) for match in DATA_I18N_KEY.finditer(text)),
        }
        if keys:
            keys_by_source[path.name] = keys
    return keys_by_source


def test_frontend_i18n_keys_exist_in_language_packs():
    used_keys = set().union(*_used_frontend_i18n_keys().values())
    assert used_keys

    for lang in ("zh", "en"):
        pack = _load_language_pack(lang)
        missing = sorted(key for key in used_keys if key not in pack)
        assert missing == []
