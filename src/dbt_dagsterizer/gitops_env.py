from __future__ import annotations

import re
from pathlib import Path

_ENV_LINE_RE = re.compile(r"^(?P<key>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>.*)$")


def _yaml_quote(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def load_dotenv(path: Path) -> dict[str, str]:
    if not path.exists():
        raise FileNotFoundError(str(path))
    data: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _ENV_LINE_RE.match(line)
        if not m:
            continue
        key = m.group("key").strip()
        value = m.group("value")
        data[key] = value.strip()
    return data


def load_pyproject_name(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    m = re.search(r'^\s*name\s*=\s*"([^"]+)"\s*$', text, re.M)
    if not m:
        raise ValueError(f"Failed to find [project].name in {path}")
    return m.group(1).strip()


def normalize_k8s_name(value: str) -> str:
    name = value.strip().lower().replace("_", "-")
    name = re.sub(r"[^a-z0-9-]+", "-", name)
    name = re.sub(r"-+", "-", name).strip("-")
    if not name:
        raise ValueError(f"Invalid Kubernetes name derived from: {value!r}")
    if not re.match(r"^[a-z0-9]", name):
        raise ValueError(f"Invalid Kubernetes name derived from: {value!r}")
    return name


def _replace_env_suffix(value: str, target: str) -> str | None:
    m = re.search(r"_(dev|snd|prd)$", value)
    if not m:
        return None
    return value[: m.start(1)] + target


def _write_configmap(*, path: Path, name: str, data: dict[str, str]) -> None:
    lines: list[str] = [
        "apiVersion: v1\n",
        "kind: ConfigMap\n",
        "metadata:\n",
        f"  name: {name}\n",
        "data:\n",
    ]
    for k in sorted(data.keys()):
        lines.append(f"  {k}: {_yaml_quote(data[k])}\n")
    path.write_text("".join(lines), encoding="utf-8")


def _write_secret(*, path: Path, name: str, keys: list[str]) -> None:
    lines: list[str] = [
        "apiVersion: v1\n",
        "kind: Secret\n",
        "metadata:\n",
        f"  name: {name}\n",
        "type: Opaque\n",
        "stringData:\n",
    ]
    if not keys:
        lines.append("  {}\n")
    else:
        for k in keys:
            lines.append(f"  {k}: \"\"\n")
    path.write_text("".join(lines), encoding="utf-8")


def ensure_gitignore_contains(*, path: Path, entry: str) -> None:
    normalized = entry.strip().lstrip("/")
    if not normalized:
        return
    normalized = normalized.rstrip("/") + "/"
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()
    else:
        lines = []
    if normalized in lines:
        return
    if lines and lines[-1].strip():
        lines.append("")
    lines.append(normalized)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def generate_gitops_env(
    *,
    project_dir: Path,
    env_file: Path,
    output_dir: Path,
    dagster_home: str,
    overwrite: bool,
    update_gitignore: bool,
) -> Path:
    project_dir = project_dir.resolve()
    env_path = env_file if env_file.is_absolute() else (project_dir / env_file)
    pyproject_path = project_dir / "pyproject.toml"
    if not pyproject_path.exists():
        raise FileNotFoundError(str(pyproject_path))

    app_name = load_pyproject_name(pyproject_path)
    k8s_name = normalize_k8s_name(app_name)
    env = load_dotenv(env_path)

    out_root = output_dir if output_dir.is_absolute() else (project_dir / output_dir)
    out_root = out_root.resolve()
    if out_root.exists() and not overwrite:
        raise FileExistsError(str(out_root))
    out_root.mkdir(parents=True, exist_ok=True)

    base_dir = out_root / "app" / "base"
    snd_dir = out_root / "app" / "overlays" / "snd"
    prd_dir = out_root / "app" / "overlays" / "prd"
    for d in (base_dir, snd_dir, prd_dir):
        d.mkdir(parents=True, exist_ok=True)

    config_name = f"{k8s_name}-config"
    secret_name = f"{k8s_name}-secret"

    secret_keys = ["STARROCKS_PASSWORD", "SSRS_DB_PASSWORD"]

    denied_prefixes = ("OTEL_",)
    denied_keys = {
        "DAGSTER_WEBSERVER_HOST",
        "DAGSTER_WEBSERVER_PORT",
        "DAGSTER_HOME",
        "LUBAN_REPO_ROOT",
        "STARROCKS_PASSWORD",
        "SSRS_DB_PASSWORD",
    }

    base_data: dict[str, str] = {"APP_ENV": "base", "DAGSTER_HOME": dagster_home}
    for k, v in env.items():
        if k in denied_keys:
            continue
        if any(k.startswith(p) for p in denied_prefixes):
            continue
        base_data[k] = v

    _write_configmap(path=base_dir / "configmap.yaml", name=config_name, data=base_data)
    _write_secret(path=base_dir / "secret.yaml", name=secret_name, keys=secret_keys)

    def _overlay_data(*, env_name: str, dbt_target: str) -> dict[str, str]:
        data: dict[str, str] = {"APP_ENV": env_name}
        if "DBT_TARGET" in env:
            data["DBT_TARGET"] = dbt_target
        for key in ["STARROCKS_ODS_DB", "STARROCKS_DWD_DB", "STARROCKS_DWS_DB", "STARROCKS_DB"]:
            value = env.get(key)
            if not value:
                continue
            replaced = _replace_env_suffix(value, env_name)
            if replaced is None:
                continue
            data[key] = replaced
        return data

    _write_configmap(
        path=snd_dir / "configmap.yaml",
        name=config_name,
        data=_overlay_data(env_name="snd", dbt_target="sandbox"),
    )
    _write_secret(path=snd_dir / "secret.yaml", name=secret_name, keys=secret_keys)

    _write_configmap(
        path=prd_dir / "configmap.yaml",
        name=config_name,
        data=_overlay_data(env_name="prd", dbt_target="production"),
    )
    _write_secret(path=prd_dir / "secret.yaml", name=secret_name, keys=secret_keys)

    if update_gitignore:
        try:
            rel = out_root.relative_to(project_dir).as_posix()
        except ValueError:
            rel = ""
        if rel:
            ensure_gitignore_contains(path=project_dir / ".gitignore", entry=rel)

    return out_root
