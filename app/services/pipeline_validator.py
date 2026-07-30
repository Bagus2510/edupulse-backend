"""Pipeline query validation — prevent code injection in generated DAGs."""

import ast
import re


_DANGEROUS_SQL = re.compile(
    r";\s*--|;\s*/\*|/\*.*\*/|--\s*$|xp_|EXEC\s|EXECUTE\s|\\\\copy|DO\s*\$\$|"
    r"CREATE\s+FUNCTION|CREATE\s+TRIGGER|CREATE\s+EXTENSION|LOAD\s|"
    r"SELECT\s+pg_|INTO\s+OUTFILE|INTO\s+DUMPFILE|SLEEP\s*\(|BENCHMARK\s*\(|"
    r";\s*[a-zA-Z]",
    re.IGNORECASE | re.MULTILINE,
)

_ALLOWED_SQL = {
    "select", "from", "where", "and", "or", "not", "in", "on", "as", "is",
    "null", "between", "like", "having", "group", "by", "order", "limit",
    "offset", "insert", "into", "values", "update", "set", "delete",
    "create", "table", "alter", "drop", "index", "view", "with", "left",
    "right", "inner", "outer", "join", "union", "all", "distinct", "case",
    "when", "then", "else", "end", "exists", "count", "sum", "avg", "min",
    "max", "cast", "over", "partition", "row_number", "rank", "dense_rank",
    "extract", "date_trunc", "interval", "now", "current_date",
    "current_timestamp", "asc", "desc", "true", "false", "returning",
    "recursive", "lateral", "cross", "using", "natural", "full", "ilike",
    "any", "some", "array", "string_to_array", "unnest", "jsonb", "json",
    "to_char", "to_date", "to_number", "coalesce", "nullif", "greatest",
    "least", "round", "ceil", "floor", "abs", "length", "upper", "lower",
    "trim", "replace", "substring", "concat", "split_part", "regexp_replace",
    "regexp_match", "regexp_matches", "array_agg", "string_agg", "format",
    "md5", "sha256", "encode", "decode", "age", "date_part", "date_part",
    "make_date", "make_interval", "generate_series", "generate_subscripts",
    "least", "greatest", "width_bucket", "percentile_cont", "percentile_disc",
    "mode", "xmlagg", "xmlforest", "xmlparse", "xmlserialize",
}

_DANGEROUS_PYTHON_AST = {
    "os", "sys", "subprocess", "shutil", "pathlib", "socket",
    "http", "urllib", "requests", "ctypes", "signal", "multiprocessing",
    "threading", "importlib", "code", "codeop", "compileall",
}

_DANGEROUS_PYTHON_CALLS = {"exec", "eval", "compile", "__import__"}


def validate_sql_query(query: str) -> str | None:
    """Return error message if query is unsafe, None if OK."""
    stripped = query.strip()
    if not stripped:
        return "Query kosong"

    if _DANGEROUS_SQL.search(stripped):
        return "Query mengandung pola SQL berbahaya"

    return None


def validate_python_code(code: str) -> str | None:
    """Return error message if code is unsafe, None if OK."""
    stripped = code.strip()
    if not stripped:
        return "Kode kosong"

    try:
        tree = ast.parse(stripped)
    except SyntaxError as e:
        return f"Syntax error: {e}"

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split(".")[0] in _DANGEROUS_PYTHON_AST:
                    return f"Import tidak diizinkan: {alias.name}"

        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module.split(".")[0] in _DANGEROUS_PYTHON_AST:
                return f"Import tidak diizinkan: {node.module}"

        elif isinstance(node, ast.Call):
            func = node.func
            name = ""
            if isinstance(func, ast.Name):
                name = func.id
            elif isinstance(func, ast.Attribute):
                name = func.attr
            if name in _DANGEROUS_PYTHON_CALLS:
                return f"Fungsi tidak diizinkan: {name}()"

    return None


def validate_step(step: dict) -> str | None:
    """Validate a single pipeline step. Return error or None."""
    query_type = step.get("query_type", "sql")
    query = step.get("query", "")

    if query_type == "sql":
        return validate_sql_query(query)
    return validate_python_code(query)
