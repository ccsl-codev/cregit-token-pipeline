#!/usr/bin/env python3
"""Write one project file in ctp schema 3, and the project_facts table.

    schema3.py write SRC DST --project NAME --domain-links D.parquet
               --project-links P.parquet --organizations-version V
               [--pseudonymize --salt-file S --psl PSL
                (--registry R.parquet | --registry-dir DIR) [--no-scan]]
               [--memory-limit 4GB] [--threads 2]
    schema3.py facts --sidecar S.json --out F.parquet NAME [NAME ...]
    schema3.py check FILE [FILE ...]

ctp.py run calls `write` after step 10 of cregit, so that a normal run gives the
file of the "Token on Me" release with no later pass. ctp schema 3 is
validate_schema.EXPECTED_COLUMNS_3 (61 columns):

  - the 20 project columns and the 37 token columns of the cregit generator
    (--ctp-schema 3). SRC can also be a 67-column cregit file or a 70-column
    schema 1 file: then the 10 columns of validate_schema.SCHEMA_3_DROPPED and
    the 3 firm columns are not copied, and provenance_status gets the name
    audit_frame_file;
  - the 4 organization columns, from two link tables:
      firm_based_on_namespace, project_enterprise_driven,
      project_enterprise_basis   one row of --project-links (key: `project`,
                                 the manifest name); NULL when no row;
      firm_based_on_domain       the `firm_based_on_domain` of the domain key in
                                 --domain-links (key: `domain`); else
                                 `independent` when the key is a real domain
                                 (not blank, a dot, no at sign); else `unknown`.
    The domain key is lower(trim(person_domain)). With --pseudonymize it is the
    published person_domain (pseudonymize.person_domain_expr, rule E), so the
    organization belongs to the value that the file shows;
  - the footer key-values ctp_schema_version = 3, organizations_version and
    organizations_mapping_sha256 (the sha256 of the sorted domain and project
    pairs and of the two fallback rules). With --pseudonymize also
    pseudonymization_version and pseudonymization_registry_sha256.

Without --pseudonymize the identity columns keep the real values: the file is
private. With --pseudonymize the identity columns get the keyed pseudonyms of
pseudonymize.py (the salt comes from --salt-file only and is never printed).
--registry names a registry that pseudonymize.py `registry` made before;
--registry-dir makes one registry for this project in that folder. The registry
holds real identities: keep it in private storage, never under the output.

The write keeps the row order. Checks before DST is put in place: the column
list and the types (EXPECTED_COLUMNS_3), the row count, an order-sensitive hash
of every column that the write copies, and the footer key-values. With
--pseudonymize also the pseudonym shapes and the leak scan of pseudonymize.py
(the information columns are not scanned, as in the release write).

`facts` writes project_facts (project, fact_type, fact_value): one row for each
`;`-separated item of the sidecar field `fact` of each NAME, split at the first
`:` (fact_value is NULL when the item has no `:`). A project with no fact has no
row. `check` checks the schema 3 contract and the footer key-values of files.

Exit 0 written or checked, 1 a check failed, 2 usage.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The modules of this folder, also under `python -I` (no script folder on the path).
sys.path.insert(0, str(HERE))
import validate_schema as vs  # noqa: E402
SCHEMA_VERSION = "3"
ENCODING = ("FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 19, "
            "ROW_GROUP_SIZE 1000000, PARQUET_VERSION V2")
ORG_KV_KEYS = ("organizations_version", "organizations_mapping_sha256")
PSEUDO_KV_KEYS = ("pseudonymization_version", "pseudonymization_registry_sha256")
UNMATCHED_REAL = "independent"
NO_REAL_DOMAIN = "unknown"
PROJECT_LINK_COLUMNS = tuple((c, t) for c, t in vs.ORGANIZATION_COLUMNS
                             if c != "firm_based_on_domain")
ORG_NAMES = tuple(c for c, _ in vs.ORGANIZATION_COLUMNS)
FACTS_COLUMNS = ("project", "fact_type", "fact_value")
EXIT_FAIL = 1
EXIT_USAGE = 2


class Schema3Error(Exception):
    """A check failed. The text has counts and column names only."""


# The schemas that the write gives. Schema 2 keeps the 29 metadata columns
# (the release write of TOM v1.0.0, before the column pass); schema 3 is the
# default.
OUT_SCHEMAS = ("2", "3")


def base_columns(schema: str = SCHEMA_VERSION) -> tuple[tuple[str, str], ...]:
    """The columns that the write copies from SRC: the contract of `schema`
    without the 4 organization columns."""
    if schema not in OUT_SCHEMAS:
        raise Schema3Error(f"the write gives schema 2 or 3, not {schema!r}")
    return tuple((c, t) for c, t in vs.contract(schema) if c not in ORG_NAMES)


# The columns that the cregit generator writes for schema 3: all but the 4
# organization columns.
BASE_COLUMNS = tuple((c, t) for c, t in vs.EXPECTED_COLUMNS_3 if c not in ORG_NAMES)


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_str(s: str) -> str:
    return "'" + str(s).replace("'", "''") + "'"


def real_domain_sql(expr: str) -> str:
    """SQL: true when the key is a real domain (not blank, a dot, no at sign)."""
    return f"({expr} <> '' AND contains({expr}, '.') AND NOT contains({expr}, chr(64)))"


def mapping_sha256(domain_pairs, project_pairs) -> str:
    """sha256 of what the organization columns copy: the sorted (domain, id)
    pairs, the sorted (project, firm_based_on_namespace) pairs, and the two
    fallback rules. A change of an organization name does not change it."""
    h = hashlib.sha256()
    h.update(f"rule\tunmatched-real-domain\t{UNMATCHED_REAL}\n"
             f"rule\tno-real-domain\t{NO_REAL_DOMAIN}\n".encode())
    for d, i in sorted((str(d), i or "") for d, i in domain_pairs):
        h.update(f"d\t{d}\t{i}\n".encode())
    for p, i in sorted((str(p), i or "") for p, i in project_pairs):
        h.update(f"p\t{p}\t{i}\n".encode())
    return h.hexdigest()


def source_projection(columns: list[str], schema: str = SCHEMA_VERSION) -> list[tuple[str, str]]:
    """(source column, output name) for the base columns of `schema`, in order.

    For schema 3, SRC is a cregit schema 3 file (57 columns), a 67-column
    cregit file or a 70-column schema 1 file. For schema 2 it is a 67-column
    cregit file or a 70-column schema 1 file. Any other column set is refused:
    a column that the contract does not know must not pass, and a missing one
    must not become NULL.
    """
    have = set(columns)
    if have & set(ORG_NAMES):
        raise Schema3Error("the input already has organization columns")
    base = base_columns(schema)
    rename = ({new: old for old, new in vs.SCHEMA_3_RENAMED.items()} if schema == "3" else {})
    allowed_extra = set(vs.FIRM_COLUMNS)
    if schema == "3":
        allowed_extra |= set(vs.SCHEMA_3_DROPPED) | set(rename.values())
    items, missing = [], []
    for name, _ in base:
        if name in have:
            items.append((name, name))
        elif rename.get(name) in have:
            items.append((rename[name], name))
        else:
            missing.append(name)
    unknown = sorted(have - {n for n, _ in base} - allowed_extra)
    if missing or unknown:
        raise Schema3Error(f"the input is not a cregit or ctp schema 1 file: "
                           f"missing {missing}, unknown {unknown}")
    return items


def file_columns(con, path: str) -> list[tuple[str, str]]:
    return [(r[0], r[1]) for r in con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)", [path]).fetchall()]


def file_kv(con, path: str) -> dict[str, str]:
    return dict(con.execute("SELECT decode(key), decode(value) FROM parquet_kv_metadata(?)",
                            [path]).fetchall())


def order_hash(con, path: str, exprs: list[str]) -> tuple[int, int]:
    """(rows, order-sensitive hash) of the SQL expressions over `path`."""
    return con.execute(
        f"SELECT count(*), bit_xor(hash(file_row_number, {', '.join(exprs)})) "
        "FROM read_parquet(?, file_row_number = true)", [path]).fetchone()


def project_row(con, links: str, project: str) -> dict:
    names = [c for c, _ in PROJECT_LINK_COLUMNS]
    rows = con.execute(f"SELECT {', '.join(q(c) for c in names)} FROM read_parquet(?) "
                       "WHERE project = ?", [links, project]).fetchall()
    if len(rows) > 1:
        raise Schema3Error(f"{len(rows)} rows for the project in the project links")
    return dict(zip(names, rows[0])) if rows else dict.fromkeys(names)


def load_domain_links(con, links: str) -> tuple[int, list[str]]:
    """Make the temp table d (di, domain, firm_based_on_domain) and return the
    row count and the id list, with the two fallback ids at the end."""
    con.execute("CREATE OR REPLACE TEMP TABLE d AS SELECT "
                "row_number() OVER (ORDER BY domain)::INTEGER AS di, domain, "
                "firm_based_on_domain FROM read_parquet(?)", [links])
    n, rep, nulls = con.execute(
        "SELECT count(*), count(*) - count(DISTINCT domain), "
        "count(*) - count(firm_based_on_domain) FROM d").fetchone()
    if rep or nulls:
        raise Schema3Error(f"the domain links have {rep} repeated domains and "
                           f"{nulls} rows with no id")
    ids = [r[0] for r in con.execute("SELECT firm_based_on_domain FROM d ORDER BY di").fetchall()]
    return n, ids + [UNMATCHED_REAL, NO_REAL_DOMAIN]


def links_mapping(con, domain_links: str, project_links: str) -> str:
    return mapping_sha256(
        con.execute("SELECT domain, firm_based_on_domain FROM read_parquet(?)",
                    [domain_links]).fetchall(),
        con.execute("SELECT project, firm_based_on_namespace FROM read_parquet(?)",
                    [project_links]).fetchall())


def schema3_select(con, src: str, project: str, domain_links: str, project_links: str,
                   domain_key: str, schema: str = SCHEMA_VERSION) -> tuple[str, dict]:
    """The SELECT of the 61 schema 3 columns over SRC, in the row order of SRC,
    and its parameters. domain_key: an SQL expression over the columns of SRC."""
    items = source_projection([c for c, _ in file_columns(con, src)], schema)
    p = project_row(con, project_links, project)
    n, ids = load_domain_links(con, domain_links)
    narrow = (f"SELECT x.rn, coalesce(d.di, CASE WHEN {real_domain_sql('x.k')} "
              f"THEN {n + 1} ELSE {n + 2} END) AS di FROM (SELECT file_row_number AS rn, "
              f"coalesce({domain_key}, '') AS k FROM read_parquet($src, file_row_number = true)) x "
              "LEFT JOIN d ON d.domain = x.k ORDER BY x.rn")
    pvals = {c: f"CAST($p_{c} AS {t}) AS {q(c)}" for c, t in PROJECT_LINK_COLUMNS}
    org = [pvals["firm_based_on_namespace"],
           "list_extract($ids::VARCHAR[], n.di) AS firm_based_on_domain",
           pvals["project_enterprise_driven"], pvals["project_enterprise_basis"]]
    t_items = [f"t.{q(s)} AS {q(n)}" for s, n in items]
    sel = (f"SELECT {', '.join(t_items + org)} FROM read_parquet($src) t "
           f"POSITIONAL JOIN ({narrow}) n")
    params = {"src": src, "ids": ids, **{f"p_{k}": v for k, v in p.items()}}
    return sel, params


def check_file(con, path: str, *, pseudonymized: bool | None = None,
               schema: str = SCHEMA_VERSION) -> list[str]:
    """The problems of a schema 3 (or 2) file: contract, footer key-values."""
    problems = []
    got = file_columns(con, path)
    drifts = vs.compare_schema(got, vs.contract(schema))
    if drifts:
        problems.append(f"{len(drifts)} drifts from ctp schema {schema}: "
                        + "; ".join(str(d).strip() for d in drifts[:5]))
    kv = file_kv(con, path)
    if kv.get(vs.SCHEMA_KEY) != schema:
        problems.append(f"footer {vs.SCHEMA_KEY} is {kv.get(vs.SCHEMA_KEY)!r}, not {schema!r}")
    for k in ORG_KV_KEYS:
        if not kv.get(k):
            problems.append(f"footer has no {k}")
    has_pseudo = [k for k in PSEUDO_KV_KEYS if kv.get(k)]
    if pseudonymized is True and len(has_pseudo) != len(PSEUDO_KV_KEYS):
        problems.append("footer has no pseudonymization key-values")
    if pseudonymized is False and has_pseudo:
        problems.append("footer has pseudonymization key-values, but the write did not pseudonymize")
    return problems


def already_written(path: str, pseudonymize: dict | None,
                    schema: str = SCHEMA_VERSION) -> bool:
    """True when `path` is a file of `schema` of the same kind (pseudonyms or not)."""
    import duckdb
    con = duckdb.connect()
    try:
        if vs.SCHEMA_KEY not in file_kv(con, path):
            return False
        return not check_file(con, path, pseudonymized=pseudonymize is not None,
                              schema=schema)
    finally:
        con.close()


def _connect(memory_limit: str, threads: int, spill: str):
    import duckdb
    con = duckdb.connect(config={"memory_limit": memory_limit, "threads": threads,
                                 "temp_directory": spill, "preserve_insertion_order": True})
    con.execute("SET enable_progress_bar = false")
    return con


def write_plain(con, src: str, tmp: str, sel: str, params: dict, kv: dict[str, str],
                schema: str = SCHEMA_VERSION) -> None:
    kvs = ", ".join(f"{k}: {sql_str(v)}" for k, v in kv.items())
    con.execute(f"COPY ({sel}) TO {sql_str(tmp)} ({ENCODING}, KV_METADATA {{{kvs}}})", params)
    base = [c for c, _ in base_columns(schema)]
    pairs = source_projection([c for c, _ in file_columns(con, src)], schema)
    a = order_hash(con, src, [q(s) for s, _ in pairs])
    b = order_hash(con, tmp, [q(c) for c in base])
    if a != b:
        raise Schema3Error(f"the {len(base)} copied columns differ after the write "
                           f"(rows {a[0]} -> {b[0]})")


def write(src: str, dst: str, *, project: str, domain_links: str, project_links: str,
          organizations_version: str, memory_limit: str = "4GB", threads: int = 2,
          pseudonymize: dict | None = None, out=None, schema: str = SCHEMA_VERSION) -> dict:
    """Write DST from SRC in `schema` (3, or 2: the 71 columns with the 29
    metadata columns). pseudonymize: None, or a dict with salt_file, psl,
    registry or registry_dir, and scan (bool). Returns counts only."""
    base_columns(schema)
    if not organizations_version:
        raise Schema3Error("--organizations-version is empty")
    if os.path.abspath(src) == os.path.abspath(dst) and already_written(src, pseudonymize,
                                                                        schema):
        # A resume after the write: SRC is the schema 3 file of an earlier
        # attempt, and the cregit file it came from is gone.
        result = {"project": project, "skipped": f"already ctp schema {schema}"}
        print(json.dumps(result, sort_keys=True), file=out or sys.stdout)
        return result
    dst_p = Path(dst)
    tmp = str(dst_p.with_name(f".{dst_p.name}.schema3-tmp"))
    private = str(dst_p.with_name(f".{dst_p.name}.schema3-private"))
    spill = str(dst_p.with_name(f".{dst_p.name}.schema3-spill"))
    os.makedirs(spill, exist_ok=True)
    con = _connect(memory_limit, threads, spill)
    result = {"project": project, "pseudonymized": bool(pseudonymize)}
    try:
        mapping = links_mapping(con, domain_links, project_links)
        kv = {vs.SCHEMA_KEY: schema, "organizations_version": organizations_version,
              "organizations_mapping_sha256": mapping}
        if pseudonymize is None:
            sel, params = schema3_select(con, src, project, domain_links, project_links,
                                         "lower(trim(person_domain))", schema)
            write_plain(con, src, tmp, sel, params, kv, schema)
        else:
            result.update(_write_pseudonymized(con, src, tmp, private, project, domain_links,
                                               project_links, kv, pseudonymize, dst_p, schema))
        problems = check_file(con, tmp, pseudonymized=pseudonymize is not None, schema=schema)
        if problems:
            raise Schema3Error("; ".join(problems))
        result["rows"] = con.execute("SELECT count(*) FROM read_parquet(?)", [tmp]).fetchone()[0]
        result["mapping_sha256"] = mapping
        os.replace(tmp, dst)
    finally:
        con.close()
        for f in (tmp, private):
            if os.path.exists(f):
                os.unlink(f)
        shutil.rmtree(spill, ignore_errors=True)
    print(json.dumps(result, sort_keys=True), file=out or sys.stdout)
    return result


def _write_pseudonymized(con, src: str, tmp: str, private: str, project: str,
                         domain_links: str, project_links: str, kv: dict[str, str],
                         opts: dict, dst_p: Path, schema: str = SCHEMA_VERSION) -> dict:
    """The organization columns with the rule E key, then the pseudonyms.

    First a private schema 3 file (real identities, the organization columns),
    then pseudonymize.write_public() from it. The registry of --registry-dir is
    made from the private file, so it also holds the organization values that
    the leak scan masks."""
    import anonymize_parquet as A
    import pseudonymize as P
    keys = P.derive_keys(A.load_salt(path=opts["salt_file"], env={}))
    try:
        psl = P.load_psl(opts.get("psl"))
        P.install(con, keys, psl=psl)
        key = P.person_domain_expr("person_domain", P.DEFAULT_DOMAIN_AT)
        sel, params = schema3_select(con, src, project, domain_links, project_links, key,
                                     schema)
        write_plain(con, src, private, sel, params, kv, schema)
        registry = opts.get("registry")
        if not registry:
            reg_dir = Path(opts["registry_dir"])
            registry = str(reg_dir / f"{project}.registry.parquet")
            P.check_registry_path(registry, (str(dst_p.parent),))
            reg_dir.mkdir(parents=True, exist_ok=True)
            P.build_registry([private], keys, registry, psl=opts.get("psl"),
                             memory_limit="2GB", threads=2, out=open(os.devnull, "w"))
        P.check_registry_path(registry, (str(dst_p.parent),))
        meta = P.verify_registry_file(con, registry)
        if meta.get("salt_check") != P.salt_check(con):
            raise Schema3Error("the registry was made with another salt")
        res = P.write_public(con, private, tmp, registry=registry,
                             registry_sha256=meta["registry_sha256"], kv=kv,
                             expect_schema_2=False)
        problems = list(res["problems"])
        if opts.get("scan", True):
            scan = P.leak_scan(con, tmp, registry, plan=None, content=False)
            problems += scan["failures"]
        if problems:
            raise Schema3Error("pseudonymize: " + "; ".join(map(str, problems))[:600])
        return {"identity_values": res["identity_values"],
                "registry_sha256": meta["registry_sha256"]}
    except Schema3Error as e:
        raise Schema3Error(P.redact(str(e), keys)) from None
    except Exception as e:
        raise Schema3Error(P.safe_error(e, keys)) from None


def split_fact(text: str) -> list[tuple[str, str | None]]:
    """`a:b;c` -> [(a, b), (c, None)]. Empty items are skipped."""
    out = []
    for item in (text or "").split(";"):
        if item == "":
            continue
        t, sep, v = item.partition(":")
        out.append((t, v if sep else None))
    return out


def facts_rows(sidecar: dict, names: list[str]) -> list[tuple[str, str, str | None]]:
    rows = []
    for name in names:
        if name not in sidecar:
            raise Schema3Error(f"{name} is not in the sidecar")
        for t, v in split_fact(sidecar[name].get("fact", "")):
            rows.append((name, t, v))
    return rows


def write_facts(sidecar_path: str, out_path: str, names: list[str]) -> int:
    """Write project_facts for `names`. Returns the row count."""
    import duckdb
    sidecar = json.loads(Path(sidecar_path).read_text())
    rows = facts_rows(sidecar, sorted(set(names)))
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(f".{out.name}.tmp")
    con = duckdb.connect()
    try:
        con.execute("CREATE TEMP TABLE f (project VARCHAR, fact_type VARCHAR, fact_value VARCHAR)")
        if rows:
            con.executemany("INSERT INTO f VALUES (?, ?, ?)", rows)
        con.execute(f"COPY (SELECT * FROM f ORDER BY project, fact_type, fact_value NULLS FIRST) "
                    f"TO {sql_str(str(tmp))} (FORMAT PARQUET, COMPRESSION ZSTD)")
    finally:
        con.close()
    os.replace(tmp, out)
    return len(rows)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    w = sub.add_parser("write", help="write one schema 3 file")
    w.add_argument("src")
    w.add_argument("dst")
    w.add_argument("--project", required=True, help="the manifest name (project links key)")
    w.add_argument("--domain-links", required=True, metavar="PARQUET")
    w.add_argument("--project-links", required=True, metavar="PARQUET")
    w.add_argument("--organizations-version", required=True, metavar="V")
    w.add_argument("--memory-limit", default="4GB")
    w.add_argument("--threads", type=int, default=2)
    w.add_argument("--pseudonymize", action="store_true")
    w.add_argument("--salt-file", default="")
    w.add_argument("--psl", default=None, help="the Public Suffix List file")
    w.add_argument("--registry", default="", help="an existing registry file")
    w.add_argument("--registry-dir", default="", help="make a registry per project here")
    w.add_argument("--no-scan", action="store_true", help="skip the leak scan")
    w.add_argument("--schema", default=SCHEMA_VERSION, choices=OUT_SCHEMAS,
                   help="3 (the default), or 2: the 71 columns with the 29 metadata columns")
    f = sub.add_parser("facts", help="write project_facts from the sidecar")
    f.add_argument("--sidecar", required=True)
    f.add_argument("--out", required=True)
    f.add_argument("names", nargs="+")
    c = sub.add_parser("check", help="check schema 3 files")
    c.add_argument("files", nargs="+")
    c.add_argument("--schema", default=SCHEMA_VERSION, choices=OUT_SCHEMAS)
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = parse_args(argv)
    try:
        if a.cmd == "facts":
            n = write_facts(a.sidecar, a.out, a.names)
            print(json.dumps({"facts_rows": n, "projects": len(set(a.names))}))
            return 0
        if a.cmd == "check":
            import duckdb
            con = duckdb.connect()
            bad = 0
            for path in a.files:
                problems = check_file(con, path, schema=a.schema)
                print(("OK   " if not problems else "FAIL ") + path
                      + ("" if not problems else "  " + "; ".join(problems)))
                bad += bool(problems)
            return EXIT_FAIL if bad else 0
        pseudo = None
        if a.pseudonymize:
            if not a.salt_file or bool(a.registry) == bool(a.registry_dir):
                print("--pseudonymize needs --salt-file and one of --registry, --registry-dir",
                      file=sys.stderr)
                return EXIT_USAGE
            pseudo = {"salt_file": a.salt_file, "psl": a.psl, "registry": a.registry,
                      "registry_dir": a.registry_dir, "scan": not a.no_scan}
        elif a.salt_file or a.registry or a.registry_dir:
            print("--salt-file, --registry and --registry-dir need --pseudonymize",
                  file=sys.stderr)
            return EXIT_USAGE
        write(a.src, a.dst, project=a.project, domain_links=a.domain_links,
              project_links=a.project_links, organizations_version=a.organizations_version,
              memory_limit=a.memory_limit, threads=a.threads, pseudonymize=pseudo,
              schema=a.schema)
        return 0
    except Schema3Error as e:
        print(f"schema3: {e}", file=sys.stderr)
        return EXIT_FAIL
    except Exception as e:
        # A DuckDB or OS error before the pseudonym step (a bad link file, a
        # missing input). The pseudonym step turns its own errors into
        # Schema3Error with a safe text, so no salt or identity reaches here.
        print(f"schema3: {type(e).__name__}: {str(e)[:300]}", file=sys.stderr)
        return EXIT_FAIL


if __name__ == "__main__":
    sys.exit(main())
