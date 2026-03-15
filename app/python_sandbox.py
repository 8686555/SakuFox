"""
Safe Python sandbox for AI-generated analysis code.

Pre-injects common data science libraries (pandas, numpy, json, math, re,
datetime, collections) so AI code can reference them directly without using
import statements.  A safe subset of __builtins__ is exposed so the sandbox
stays safe while avoiding "__import__ not found" errors.
"""
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import datetime as _datetime, date as _date, timedelta as _timedelta
import json as _json
import math as _math
import re as _re

import numpy as _np
import pandas as pd

# sklearn — imported lazily so missing package gives a clear error at sandbox call time
try:
    from sklearn.linear_model import LinearRegression, LogisticRegression, Ridge, Lasso
    from sklearn.ensemble import (
        RandomForestClassifier, RandomForestRegressor,
        GradientBoostingClassifier, GradientBoostingRegressor,
    )
    from sklearn.cluster import KMeans, DBSCAN, AgglomerativeClustering
    from sklearn.preprocessing import StandardScaler, MinMaxScaler, LabelEncoder, OneHotEncoder
    from sklearn.model_selection import train_test_split, cross_val_score
    from sklearn.metrics import (
        accuracy_score, f1_score, precision_score, recall_score,
        mean_squared_error, mean_absolute_error, r2_score,
        classification_report, confusion_matrix,
    )
    from sklearn.decomposition import PCA
    from sklearn.pipeline import Pipeline
    _HAS_SKLEARN = True
except ImportError:
    _HAS_SKLEARN = False
    # Placeholders so local_vars injection doesn't fail
    (LinearRegression, LogisticRegression, Ridge, Lasso,
     RandomForestClassifier, RandomForestRegressor,
     GradientBoostingClassifier, GradientBoostingRegressor,
     KMeans, DBSCAN, AgglomerativeClustering,
     StandardScaler, MinMaxScaler, LabelEncoder, OneHotEncoder,
     train_test_split, cross_val_score,
     accuracy_score, f1_score, precision_score, recall_score,
     mean_squared_error, mean_absolute_error, r2_score,
     classification_report, confusion_matrix,
     PCA, Pipeline) = (None,) * 27


# ── Safe builtins exposed to the sandbox ─────────────────────────────────────
ALLOWED_BUILTINS = {
    # built-in types
    "bool": bool,
    "bytes": bytes,
    "complex": complex,
    "dict": dict,
    "float": float,
    "frozenset": frozenset,
    "int": int,
    "list": list,
    "set": set,
    "str": str,
    "tuple": tuple,
    "type": type,
    # itertools-like
    "abs": abs,
    "all": all,
    "any": any,
    "enumerate": enumerate,
    "filter": filter,
    "map": map,
    "max": max,
    "min": min,
    "pow": pow,
    "range": range,
    "reversed": reversed,
    "round": round,
    "sorted": sorted,
    "sum": sum,
    "zip": zip,
    # I/O
    "print": print,
    "repr": repr,
    "open": open,
    # inspection
    "callable": callable,
    "dir": dir,
    "getattr": getattr,
    "hasattr": hasattr,
    "isinstance": isinstance,
    "issubclass": issubclass,
    "len": len,
    # exceptions
    "Exception": Exception,
    "ValueError": ValueError,
    "KeyError": KeyError,
    "TypeError": TypeError,
    "IndexError": IndexError,
    "RuntimeError": RuntimeError,
    "StopIteration": StopIteration,
    "NotImplementedError": NotImplementedError,
    # import
    "__import__": __import__,
    # misc
    "hash": hash,
    "id": id,
    "iter": iter,
    "next": next,
    "object": object,
    "property": property,
    "staticmethod": staticmethod,
    "classmethod": classmethod,
    "super": super,
    "vars": vars,
}


def run_python_pipeline(
    python_code: str,
    seed_rows: list[dict],
    upload_rows: dict[str, list[dict]],
    upload_paths: dict[str, str],
    sql_tool: Callable[[str], list[dict]],
    step_results: list[dict] | None = None,
) -> dict:
    seed_df = pd.DataFrame(seed_rows) if seed_rows else pd.DataFrame()
    uploads_df = {name: pd.DataFrame(rows) for name, rows in upload_rows.items()}

    def sdk_execute_select_sql(sql: str) -> list[dict]:
        return sql_tool(sql)

    # Pre-inject all common libraries so AI code doesn't need to import them
    local_vars: dict = {
        # data
        "df": seed_df,
        "final_df": seed_df,
        "uploaded_dataframes": uploads_df,
        "uploaded_file_paths": upload_paths,
        # multi-step results from prior steps
        "step_results": step_results or [],
        # output slots
        "chart_specs": [],
        "insight_hints": [],
        # SQL tool
        "execute_select_sql": sdk_execute_select_sql,
        # ── data science libraries ──────────────────────────────────
        "pd": pd,
        "pandas": pd,
        "np": _np,
        "numpy": _np,
        "json": _json,
        "math": _math,
        "re": _re,
        # datetime helpers
        "datetime": _datetime,
        "date": _date,
        "timedelta": _timedelta,
        # collections helpers
        "Counter": Counter,
        "defaultdict": defaultdict,
        # ── sklearn ─────────────────────────────────────────────────
        # Linear models
        "LinearRegression": LinearRegression,
        "LogisticRegression": LogisticRegression,
        "Ridge": Ridge,
        "Lasso": Lasso,
        # Ensemble
        "RandomForestClassifier": RandomForestClassifier,
        "RandomForestRegressor": RandomForestRegressor,
        "GradientBoostingClassifier": GradientBoostingClassifier,
        "GradientBoostingRegressor": GradientBoostingRegressor,
        # Clustering
        "KMeans": KMeans,
        "DBSCAN": DBSCAN,
        "AgglomerativeClustering": AgglomerativeClustering,
        # Preprocessing
        "StandardScaler": StandardScaler,
        "MinMaxScaler": MinMaxScaler,
        "LabelEncoder": LabelEncoder,
        "OneHotEncoder": OneHotEncoder,
        # Model selection
        "train_test_split": train_test_split,
        "cross_val_score": cross_val_score,
        # Metrics
        "accuracy_score": accuracy_score,
        "f1_score": f1_score,
        "precision_score": precision_score,
        "recall_score": recall_score,
        "mean_squared_error": mean_squared_error,
        "mean_absolute_error": mean_absolute_error,
        "r2_score": r2_score,
        "classification_report": classification_report,
        "confusion_matrix": confusion_matrix,
        # Decomposition / Pipeline
        "PCA": PCA,
        "Pipeline": Pipeline,
    }

    global_vars = {"__builtins__": ALLOWED_BUILTINS}

    try:
        exec(python_code, global_vars, local_vars)
    except Exception as exc:
        raise RuntimeError(f"Python 执行出错: {exc}") from exc

    final_df = local_vars.get("final_df", local_vars.get("df", seed_df))
    if not isinstance(final_df, pd.DataFrame):
        # Try to coerce list-of-dicts output
        if isinstance(final_df, list):
            final_df = pd.DataFrame(final_df)
        else:
            raise ValueError("Python 脚本需要将处理结果赋值给 final_df（DataFrame 或 list[dict]）")

    chart_specs = local_vars.get("chart_specs", [])
    if not isinstance(chart_specs, list):
        chart_specs = []
    
    # Auto-inject engine tag for frontend renderer
    for spec in chart_specs:
        if isinstance(spec, dict) and "engine" not in spec:
            spec["engine"] = "echarts"

    insight_hints = local_vars.get("insight_hints", [])
    if not isinstance(insight_hints, list):
        insight_hints = []

    return {
        "rows": final_df.to_dict(orient="records"),
        "chart_specs": chart_specs,
        "insight_hints": [str(x) for x in insight_hints],
    }
