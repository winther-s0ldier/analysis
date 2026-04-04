import os
import json
import pandas as pd
import numpy as np
from pathlib import Path

SUPPORTED_EXTENSIONS = {
    ".csv", ".xlsx", ".xls",
    ".json", ".jsonl", ".parquet"
}

def normalize_file(file_path: str) -> dict:
    path = Path(file_path)
    ext  = path.suffix.lower()

    if ext not in SUPPORTED_EXTENSIONS:
        return {
            "status": "unsupported",
            "error": (
                f"Format '{ext}' is not supported. "
                f"Supported: "
                f"{', '.join(sorted(SUPPORTED_EXTENSIONS))}"
            ),
            "original_format": ext,
        }

    warnings = []

    try:
        if ext == ".csv":
            df, file_warnings = _load_csv(file_path)
        elif ext in (".xlsx", ".xls"):
            df, file_warnings = _load_excel(file_path)
        elif ext == ".json":
            df, file_warnings = _load_json(file_path)
        elif ext == ".jsonl":
            df, file_warnings = _load_jsonl(file_path)
        elif ext == ".parquet":
            df, file_warnings = _load_parquet(file_path)
        else:
            df, file_warnings = pd.DataFrame(), []

        warnings.extend(file_warnings)

        if df is None or len(df) == 0:
            return {
                "status": "error",
                "error": "File produced no rows after loading.",
                "original_format": ext,
            }

        df, clean_warnings = _clean_dataframe(df)
        warnings.extend(clean_warnings)

        csv_path = _get_output_path(file_path)
        df.to_csv(csv_path, index=False, encoding="utf-8")

        return {
            "status":            "success",
            "csv_path":          csv_path,
            "original_format":   ext,
            "original_filename": path.name,
            "row_count":         len(df),
            "col_count":         len(df.columns),
            "columns":           df.columns.tolist(),
            "warnings":          warnings,
        }

    except Exception as e:
        return {
            "status": "error",
            "error":  f"Failed to process {ext} file: {str(e)}",
            "original_format": ext,
        }

def _get_output_path(file_path: str) -> str:
    path = Path(file_path)
    return str(
        path.parent / f"{path.stem}_normalized.csv"
    )

def _load_csv(file_path: str):
    warnings = []
    df = None

    for encoding in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
        try:
            df = pd.read_csv(
                file_path,
                encoding=encoding,
                low_memory=False,
                skip_blank_lines=True,
            )
            if encoding != "utf-8":
                warnings.append(
                    f"File encoded as {encoding}, "
                    f"converted to UTF-8."
                )
            break
        except (UnicodeDecodeError, Exception):
            continue

    if df is None:
        raise ValueError("Could not decode CSV with any encoding")

    df.columns = [str(c).strip() for c in df.columns]

    df = df.dropna(how="all")

    return df, warnings

def _load_excel(file_path: str):
    warnings = []

    xl = pd.ExcelFile(file_path)
    sheet_names = xl.sheet_names

    df = None
    used_sheet = None
    for sheet in sheet_names:
        candidate = xl.parse(sheet, header=None)
        candidate = candidate.dropna(how="all")
        if len(candidate) > 1:
            df = candidate
            used_sheet = sheet
            break

    if df is None:
        raise ValueError("No non-empty sheets found in Excel file")

    if len(sheet_names) > 1:
        warnings.append(
            f"Excel has {len(sheet_names)} sheets. "
            f"Using '{used_sheet}'. Others ignored."
        )

    header_row_idx = 0
    best_string_count = 0
    for i, row in df.head(10).iterrows():
        string_count = sum(
            1 for v in row
            if isinstance(v, str) and len(v.strip()) > 0
        )
        if string_count > best_string_count:
            best_string_count = string_count
            header_row_idx    = i

    if header_row_idx > 0:
        warnings.append(
            f"Skipped {header_row_idx} junk row(s) "
            f"at top of Excel sheet."
        )

    df = xl.parse(
        used_sheet,
        header=header_row_idx,
    )
    df = df.dropna(how="all")

    df.columns = [
        str(c).strip()
        for c in df.columns
    ]

    unnamed = [
        c for c in df.columns
        if str(c).startswith("Unnamed:")
    ]
    if unnamed:
        df = df.drop(columns=unnamed)
        warnings.append(
            f"Dropped {len(unnamed)} unnamed columns "
            f"(likely from merged cells)."
        )

    df = df.dropna(how="all")

    xl.close()

    return df, warnings

def _load_json(file_path: str):
    warnings = []

    with open(file_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    if isinstance(raw, list):
        df = pd.json_normalize(raw, max_level=2)

    elif isinstance(raw, dict):
        if all(isinstance(v, list) for v in raw.values()):
            df = pd.DataFrame(raw)
        else:
            df = pd.json_normalize([raw], max_level=2)
            warnings.append(
                "JSON was a single object, not an array. "
                "Treated as one row."
            )
    else:
        raise ValueError(
            "JSON must be an array of objects or "
            "an object with array values."
        )

    df.columns = [
        c.replace(".", "_").replace(" ", "_")
        for c in df.columns
    ]

    nested_cols = [
        c for c in df.columns
        if df[c].apply(
            lambda x: isinstance(x, (dict, list))
        ).any()
    ]
    if nested_cols:
        for col in nested_cols:
            df[col] = df[col].apply(
                lambda x: json.dumps(x)
                if isinstance(x, (dict, list))
                else x
            )
        warnings.append(
            f"Deeply nested columns serialized to JSON "
            f"strings: {nested_cols[:3]}."
        )

    return df, warnings

def _load_jsonl(file_path: str):
    warnings = []
    records  = []
    errors   = 0

    with open(file_path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                errors += 1
                if errors <= 3:
                    continue

    if errors > 0:
        warnings.append(
            f"{errors} malformed lines skipped in JSONL."
        )

    if not records:
        raise ValueError("No valid JSON lines found in file")

    df = pd.json_normalize(records, max_level=2)
    df.columns = [
        c.replace(".", "_").replace(" ", "_")
        for c in df.columns
    ]

    return df, warnings

def _load_parquet(file_path: str):
    warnings = []

    try:
        df = pd.read_parquet(file_path)
    except ImportError:
        raise ValueError(
            "pyarrow or fastparquet required for Parquet. "
            "Run: pip install pyarrow"
        )

    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna().head(5)
            if sample.apply(
                lambda x: isinstance(x, (dict, list))
            ).any():
                df[col] = df[col].apply(
                    lambda x: json.dumps(x)
                    if isinstance(x, (dict, list))
                    else x
                )
                warnings.append(
                    f"Column '{col}' had nested data, "
                    f"serialized to JSON string."
                )

    return df, warnings

def _clean_dataframe(df: pd.DataFrame):
    warnings = []

    original_cols = df.columns.tolist()
    df.columns = [
        str(c)
        .strip()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "_")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("#", "num")
        .replace("%", "pct")
        .lower()
        for c in df.columns
    ]

    renamed = [
        (o, n) for o, n in
        zip(original_cols, df.columns.tolist())
        if str(o) != n
    ]
    if renamed:
        warnings.append(
            f"{len(renamed)} column(s) renamed for "
            f"compatibility (spaces/special chars removed)."
        )

    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].apply(
            lambda x: x.strip()
            if isinstance(x, str) else x
        )

    before = len(df)
    df = df.drop_duplicates()
    after  = len(df)
    if before != after:
        warnings.append(
            f"{before - after} fully duplicate rows removed."
        )

    empty_cols = [
        c for c in df.columns
        if df[c].isna().all()
    ]
    if empty_cols:
        df = df.drop(columns=empty_cols)
        warnings.append(
            f"{len(empty_cols)} fully empty column(s) dropped."
        )

    df = df.reset_index(drop=True)

    return df, warnings

def get_supported_extensions() -> list:
    return sorted(SUPPORTED_EXTENSIONS)

def is_supported(file_path: str) -> bool:
    return Path(file_path).suffix.lower() in SUPPORTED_EXTENSIONS
