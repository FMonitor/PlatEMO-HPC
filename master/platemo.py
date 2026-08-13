"""PlatEMO catalog and experiment-setting adapters.

The MATLAB GUI stores experiment settings as three values in ``Setting`` and
two values in ``Environment``.  This module keeps that legacy format at the
edge of the Master and exposes a JSON-friendly model to the web application.
"""
from __future__ import annotations

import json
import re
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable

import numpy as np
from scipy.io import loadmat, savemat


PROBLEM_GUI_PARAMETERS = [
    {"name": "N", "default": "100", "source": "platemo-gui"},
    {"name": "M", "default": "", "source": "platemo-gui"},
    {"name": "D", "default": "", "source": "platemo-gui"},
    {"name": "maxFE", "default": "10000", "source": "platemo-gui"},
]


def _items(value: Any) -> list[Any]:
    """Flatten MATLAB cell/ndarray values without flattening text."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, bytes):
        return [value.decode("utf-8", errors="replace")]
    if isinstance(value, np.ndarray):
        if value.ndim == 0:
            return _items(value.item())
        if value.size == 0:
            # Empty MATLAB cells are positional parameter values, not absent
            # list entries.  Keeping them preserves later Setting{3} values.
            return [""]
        return [item for cell in value.tolist() for item in _items(cell)]
    if isinstance(value, (list, tuple)):
        result: list[Any] = []
        for item in value:
            result.extend(_items(item))
        return result
    return [value]


def _cell_members(value: Any) -> list[Any]:
    """Return immediate MATLAB cell-array members, preserving nested cells."""
    if isinstance(value, np.ndarray):
        if value.ndim == 0:
            return [value.item()]
        return list(value.flat)
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value).strip()


def parse_matlab_value(value: Any) -> Any:
    """Convert a GUI edit-field value to a JSON-safe scalar/list."""
    text = _text(value)
    if not text:
        return ""
    if len(text) >= 2 and text[0] in "'\"" and text[-1] == text[0]:
        return text[1:-1]
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    if re.fullmatch(r"[-+]?\d+", text):
        try:
            return int(text)
        except ValueError:
            pass
    if re.fullmatch(r"[-+]?(?:\d+\.?(?:\d*)?|\.\d+)(?:[eE][-+]?\d+)?", text):
        try:
            return float(text)
        except ValueError:
            pass
    vector = text.strip("[]")
    if vector != text and vector.strip():
        parts = re.split(r"[;,\s]+", vector.strip())
        try:
            return [parse_matlab_value(part) for part in parts if part]
        except Exception:
            pass
    return text


def _safe_json(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, np.generic):
        return _safe_json(value.item())
    if isinstance(value, np.ndarray):
        return [_safe_json(item) for item in value.tolist()]
    if isinstance(value, dict):
        return {str(key): _safe_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_safe_json(item) for item in value]
    return str(value)


def _parameter_schema(source: str, kind: str) -> list[dict[str, Any]]:
    """Read PlatEMO's ``% name --- default --- description`` declarations."""
    metadata_re = re.compile(r"^\s*%\s*([A-Za-z_]\w*)\s+---\s+(.+?)\s+---\s*(.*)$", re.M)
    parameters: list[dict[str, Any]] = []
    for name, default, description in metadata_re.findall(source):
        parameters.append({"name": name, "default": default.strip(), "description": description.strip(), "source": "class-comment"})
    if kind == "algorithm" and not parameters:
        parameters = _parameter_set_schema(source)
    if kind == "problem":
        existing = {item["name"].lower() for item in parameters}
        parameters = [item for item in PROBLEM_GUI_PARAMETERS if item["name"].lower() not in existing] + parameters
    return parameters


def _split_matlab_arguments(value: str) -> list[str]:
    """Split the first-level arguments of a MATLAB function call."""
    arguments: list[str] = []
    start = 0
    depth = 0
    quote = ""
    for index, char in enumerate(value):
        if quote:
            if char == quote:
                quote = ""
            continue
        if char in "'\"":
            quote = char
        elif char in "([{":
            depth += 1
        elif char in ")]}":
            depth = max(0, depth - 1)
        elif char == "," and depth == 0:
            arguments.append(value[start:index].strip())
            start = index + 1
    tail = value[start:].strip()
    if tail:
        arguments.append(tail)
    return arguments


def _parameter_set_schema(source: str) -> list[dict[str, Any]]:
    """Infer unnamed algorithm inputs from ``x = Algorithm.ParameterSet(...)``."""
    call_re = re.compile(
        r"(?P<left>\[[^\]]+\]|[A-Za-z_]\w*)\s*=\s*\w+\.ParameterSet\s*\((?P<args>.*?)\)",
        re.S,
    )
    match = call_re.search(source)
    if not match:
        return []
    args = _split_matlab_arguments(match.group("args").replace("...", " "))
    left = match.group("left").strip().strip("[]")
    names = [item.strip() for item in left.split(",") if item.strip()]
    return [
        {"name": names[index] if index < len(names) else f"参数{index + 1}", "default": default,
         "description": "从 ParameterSet 调用推断", "source": "ParameterSet"}
        for index, default in enumerate(args)
    ]


def discover_catalog(platemo_root: Path, relative: str, base_type: str) -> list[dict[str, Any]]:
    root = platemo_root / relative
    if not root.is_dir():
        return []
    class_re = re.compile(r"^\s*classdef(?:\s*\([^\n)]*\))?\s+(\w+)\s*<\s*([\w.]+)", re.I | re.M)
    result: list[dict[str, Any]] = []
    for path in root.rglob("*.m"):
        if path.stem.startswith("@") or path.stem != path.name[:-2]:
            continue
        try:
            source = path.read_text(encoding="utf-8", errors="ignore")
        except OSError as exc:
            result.append({"name": path.stem, "kind": base_type.lower(), "parameters": [], "error": str(exc)})
            continue
        match = class_re.search(source)
        if not match or match.group(1) != path.stem or match.group(2).split(".")[-1].lower() != base_type.lower():
            continue
        parameters = _parameter_schema(source, base_type.lower())
        result.append({
            "name": path.stem,
            "kind": base_type.lower(),
            "path": str(path),
            "relative_path": str(path.relative_to(platemo_root)),
            "parameters": parameters,
        })
    return sorted(result, key=lambda item: item["name"].lower())


def discover_catalogs(platemo_root: Path) -> dict[str, list[dict[str, Any]]]:
    return {
        "algorithms": discover_catalog(platemo_root, "Algorithms", "ALGORITHM"),
        "problems": discover_catalog(platemo_root, "Problems", "PROBLEM"),
    }


def _schema_for(name: str, entries: Iterable[dict[str, Any]], kind: str) -> list[dict[str, Any]]:
    found = next((entry for entry in entries if entry["name"] == name), None)
    if found:
        return list(found.get("parameters", []))
    return list(PROBLEM_GUI_PARAMETERS) if kind == "problem" else []


def parse_platemo_setting(data: dict[str, Any], catalogs: dict[str, list[dict[str, Any]]] | None = None,
                          source_name: str = "") -> dict[str, Any]:
    """Parse a native PlatEMO Setting/Environment pair into repeated instances."""
    catalogs = catalogs or {"algorithms": [], "problems": []}
    raw_setting = _cell_members(data.get("Setting"))
    if len(raw_setting) < 3:
        raise ValueError("MAT 文件缺少 Setting{1}/Setting{2}/Setting{3}")
    algorithm_names = [_text(item).removesuffix(".m") for item in _items(raw_setting[0])]
    problem_names = [_text(item).removesuffix(".m") for item in _items(raw_setting[1])]
    flat_values = _items(raw_setting[2])
    cursor = 0
    diagnostics: list[str] = []
    algorithms: list[dict[str, Any]] = []
    for index, name in enumerate(algorithm_names):
        schema = _schema_for(name, catalogs.get("algorithms", []), "algorithm")
        values = flat_values[cursor:cursor + len(schema)]
        cursor += len(schema)
        if len(values) < len(schema):
            diagnostics.append(f"算法 {name} 缺少 {len(schema) - len(values)} 个参数")
        algorithms.append({"id": f"algorithm-{index + 1}", "name": name,
                           "parameters": {item["name"]: parse_matlab_value(values[pos]) if pos < len(values) else item.get("default", "")
                                           for pos, item in enumerate(schema)}})
    problems: list[dict[str, Any]] = []
    for index, name in enumerate(problem_names):
        schema = _schema_for(name, catalogs.get("problems", []), "problem")
        values = flat_values[cursor:cursor + len(schema)]
        cursor += len(schema)
        if len(values) < len(schema):
            diagnostics.append(f"问题 {name} 第 {index + 1} 个实例缺少 {len(schema) - len(values)} 个参数")
        problems.append({"id": f"problem-{index + 1}", "name": name,
                         "parameters": {item["name"]: parse_matlab_value(values[pos]) if pos < len(values) else item.get("default", "")
                                         for pos, item in enumerate(schema)}})
    if cursor < len(flat_values):
        diagnostics.append(f"Setting{3} 中有 {len(flat_values) - cursor} 个未识别参数值")
    environment = [parse_matlab_value(item) for item in _items(data.get("Environment"))]
    runs = environment[0] if environment else 30
    retain_results = environment[1] if len(environment) > 1 else 1
    return {
        "format": "platemo-setting",
        "source": source_name,
        "algorithms": algorithms,
        "problems": problems,
        "runs": runs,
        "retain_results": retain_results,
        "diagnostics": diagnostics,
        "raw_environment": _safe_json(environment),
        "raw_flat_parameter_count": len(flat_values),
    }


def parse_platemo_setting_file(path: Path, catalogs: dict[str, list[dict[str, Any]]] | None = None) -> dict[str, Any]:
    data = loadmat(path, simplify_cells=True)
    parsed = parse_setting_data(data, catalogs, path.name)
    parsed["path"] = str(path)
    parsed["modified_at"] = path.stat().st_mtime
    return parsed


def parse_native_settings(data: dict[str, Any], source_name: str = "") -> dict[str, Any]:
    """Read the Master-owned MAT format without evaluating MATLAB content."""
    raw = data.get("PlatEMO_HPC_SettingsJSON")
    if raw is None:
        raise ValueError("MAT 文件不是 PlatEMO HPC 原生设置")
    try:
        config = json.loads(_text(raw))
    except json.JSONDecodeError as exc:
        raise ValueError("PlatEMO HPC 设置 JSON 无法解析") from exc
    execution = config.get("execution", {}) if isinstance(config, dict) else {}
    algorithms = config.get("algorithms", []) if isinstance(config, dict) else []
    problems = config.get("problems", []) if isinstance(config, dict) else []
    if not isinstance(algorithms, list) or not isinstance(problems, list):
        raise ValueError("PlatEMO HPC 设置缺少算法或问题列表")
    return {
        "format": "platemo-hpc-settings",
        "source": source_name,
        "algorithms": _safe_json(algorithms),
        "problems": _safe_json(problems),
        "runs": execution.get("runs", 30),
        "retain_results": execution.get("retain_points", 1),
        "diagnostics": [],
    }


def parse_setting_data(data: dict[str, Any], catalogs: dict[str, list[dict[str, Any]]] | None = None,
                       source_name: str = "") -> dict[str, Any]:
    if "PlatEMO_HPC_SettingsJSON" in data:
        return parse_native_settings(data, source_name)
    return parse_platemo_setting(data, catalogs, source_name)


def list_setting_files(platemo_root: Path) -> list[dict[str, Any]]:
    root = platemo_root / "Data"
    if not root.is_dir():
        return []
    result = []
    for path in sorted(root.glob("Setting*.mat"), key=lambda item: item.name.lower()):
        result.append({"name": path.stem, "filename": path.name, "path": str(path),
                       "size": path.stat().st_size, "modified_at": path.stat().st_mtime})
    return result


def discover_existing_tests(platemo_root: Path, problem_names: Iterable[str] = ()) -> list[dict[str, Any]]:
    """Group existing PlatEMO result files into selectable test candidates."""
    data_root = platemo_root / "Data"
    if not data_root.is_dir():
        return []
    known_problems = sorted(set(problem_names), key=len, reverse=True)
    groups: dict[tuple[str, str, int, int], dict[str, Any]] = {}
    suffix_re = re.compile(r"_(?P<problem>.+)_M(?P<M>\d+)_D(?P<D>\d+)_(?P<run>\d+)\.mat$", re.I)
    for algorithm_dir in data_root.iterdir():
        if not algorithm_dir.is_dir() or algorithm_dir.name.startswith("Setting"):
            continue
        for path in algorithm_dir.glob("*.mat"):
            match = suffix_re.search(path.name)
            if not match:
                continue
            problem = match.group("problem")
            if known_problems:
                problem = next((name for name in known_problems if problem == name), problem)
            key = (algorithm_dir.name, problem, int(match.group("M")), int(match.group("D")))
            group = groups.setdefault(key, {"algorithm": key[0], "problem": key[1], "M": key[2], "D": key[3], "runs": [], "files": []})
            group["runs"].append(int(match.group("run")))
            group["files"].append(str(path))
    result = list(groups.values())
    for item in result:
        item["runs"] = sorted(set(item["runs"]))
        item["run_count"] = len(item["runs"])
        item["file_count"] = len(item["files"])
        item["files"] = sorted(item["files"])[:10]
    return sorted(result, key=lambda item: (item["algorithm"].lower(), item["problem"].lower(), item["M"], item["D"]))


def native_settings_mat(config: dict[str, Any]) -> BytesIO:
    """Create a Master-owned MAT artifact; it intentionally is not PlatEMO-compatible."""
    payload = _safe_json(config)
    output = BytesIO()
    savemat(output, {
        "PlatEMO_HPC_FormatVersion": "1.0",
        "PlatEMO_HPC_SettingsJSON": json.dumps(payload, ensure_ascii=False),
    })
    output.seek(0)
    return output
