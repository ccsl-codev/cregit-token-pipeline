"""The rule layer of the license pass: one license value per project from a ScanCode scan.

The input is the ScanCode JSON of a folder of key files of one project (the files staged by
license_scan.py at the pinned commit), with optional results of a header pass, of the SPDX template
diff and of two second-opinion scanners. The output is one decision: an SPDX expression, the
evidence, a confidence class and the agreement with the second opinions.

The rules (each one fixes a failure of the earlier licensee/askalono rescan):

- The main license comes from the project's own root license files. Bundled files
  (`LICENSE.<dependency>`, `third_party/`, `vendor/`, `licenses/<dependency>`) do not enter it.
- A root notice (a manifest, a README, a notice in a root license file) can choose among the root
  license files: when all its licenses are found as texts in the root files, its expression is the
  value (this gives "MIT OR Apache-2.0" for dual licenses and drops bundled texts).
- `-only` or `-or-later` is claimed only from a notice, a tag or a header, never from a license text
  alone. Without a notice the id has no suffix and `version_choice` says "unknown".
- A text id is accepted at once only for an exact ScanCode match (`1-hash`, `2-aho` or `1-spdx-id`,
  score and coverage 100). A near match asks for an SPDX template diff of the id and of its close
  relatives (ECL-2.0 for Apache-2.0, BSD-3-Clause-Clear for BSD-3-Clause, ...).
- "X AND exception" becomes "X WITH exception".
- REUSE `LICENSES/<id>` files count only where ScanCode finds the license text of that id.
- askalono's AGPL-1.0 is dropped unless ScanCode agrees.

The module is stdlib Python only, so that it runs with any Python 3.9+ (the EC2 machine uses the
Python of the scanner Nix environment).
"""

from __future__ import annotations

import posixpath
import re
from dataclasses import dataclass, field

# --- SPDX expressions ------------------------------------------------------------------------------

_TOKEN = re.compile(r"\s*(\(|\)|[A-Za-z0-9.+:\-_]+)")
_OPS = {"AND", "OR", "WITH"}


class ExpressionError(ValueError):
    pass


def tokenize(expr: str) -> list[str]:
    out, pos = [], 0
    expr = expr.strip()
    while pos < len(expr):
        m = _TOKEN.match(expr, pos)
        if not m:
            raise ExpressionError(f"bad character at {pos} in {expr!r}")
        tok = m.group(1)
        out.append(tok.upper() if tok.upper() in _OPS else tok)
        pos = m.end()
    return out


def parse(expr: str):
    """Parse an SPDX expression into a tree: str (a license id), ("WITH", id, exc), ("AND"|"OR", [kids])."""
    toks = tokenize(expr)
    if not toks:
        raise ExpressionError("empty expression")
    pos = 0

    def peek():
        return toks[pos] if pos < len(toks) else None

    def take():
        nonlocal pos
        tok = toks[pos]
        pos += 1
        return tok

    def primary():
        tok = peek()
        if tok is None:
            raise ExpressionError(f"unexpected end in {expr!r}")
        if tok == "(":
            take()
            node = or_expr()
            if peek() != ")":
                raise ExpressionError(f"missing ) in {expr!r}")
            take()
            return node
        if tok in _OPS or tok == ")":
            raise ExpressionError(f"unexpected {tok} in {expr!r}")
        lic = take()
        if peek() == "WITH":
            take()
            exc = peek()
            if exc is None or exc in _OPS or exc in "()":
                raise ExpressionError(f"WITH without an exception in {expr!r}")
            take()
            return ("WITH", lic, exc)
        return lic

    def chain(op, sub):
        kids = [sub()]
        while peek() == op:
            take()
            kids.append(sub())
        return kids[0] if len(kids) == 1 else (op, kids)

    def and_expr():
        return chain("AND", primary)

    def or_expr():
        return chain("OR", and_expr)

    node = or_expr()
    if pos != len(toks):
        raise ExpressionError(f"trailing tokens in {expr!r}")
    return node


def render(node, top: bool = True) -> str:
    if isinstance(node, str):
        return node
    if node[0] == "WITH":
        return f"{node[1]} WITH {node[2]}"
    op, kids = node
    parts = []
    for k in kids:
        s = render(k, top=False)
        if isinstance(k, tuple) and k[0] in ("AND", "OR") and k[0] != op:
            s = f"({s})"
        parts.append(s)
    return f" {op} ".join(parts)


def leaves(node) -> list:
    """The license terms of a tree: ids and ("WITH", id, exc) tuples, in order."""
    if isinstance(node, str) or node[0] == "WITH":
        return [node]
    out = []
    for k in node[1]:
        out.extend(leaves(k))
    return out


def license_ids(expr: str) -> list[str]:
    """All license ids of an expression (no exceptions), in order, without duplicates."""
    out = []
    for leaf in leaves(parse(expr)):
        lic = leaf if isinstance(leaf, str) else leaf[1]
        if lic not in out:
            out.append(lic)
    return out


def _flatten(op, kids):
    out = []
    for k in kids:
        if isinstance(k, tuple) and k[0] == op:
            out.extend(_flatten(op, k[1]))
        else:
            out.append(k)
    return out


def join_and(terms: list) -> object:
    """An AND of terms (trees), flat, without duplicates, sorted by text; None for no terms."""
    flat = _flatten("AND", terms)
    uniq = {render(t): t for t in flat}
    keys = sorted(uniq, key=lambda s: (s.lower(), s))
    if not keys:
        return None
    if len(keys) == 1:
        return uniq[keys[0]]
    return ("AND", [uniq[k] for k in keys])


# --- License ids -----------------------------------------------------------------------------------

GNU_FAMILIES = ("AGPL", "LGPL", "GPL", "GFDL")
_GNU_RE = re.compile(r"^(AGPL|LGPL|GPL|GFDL)-(\d\.\d)(.*)$")
_DEPRECATED_PLUS = {"GPL-1.0+", "GPL-2.0+", "GPL-3.0+", "LGPL-2.0+", "LGPL-2.1+", "LGPL-3.0+", "AGPL-3.0+",
                    "GFDL-1.1+", "GFDL-1.2+", "GFDL-1.3+"}


def gnu_base(lic: str) -> str | None:
    """'GPL-2.0' for GPL-2.0, GPL-2.0-only, GPL-2.0-or-later, GPL-2.0+; GFDL variants to the GFDL base."""
    m = _GNU_RE.match(lic)
    if not m:
        return None
    rest = m.group(3)
    if rest in ("", "-only", "-or-later", "+") or (m.group(1) == "GFDL" and re.fullmatch(
            r"(-invariants|-no-invariants)?(-only|-or-later)?", rest)):
        return f"{m.group(1)}-{m.group(2)}"
    return None


def gnu_choice(lic: str) -> str | None:
    """'or-later', 'only' or None (no suffix) for a GNU id; None for other ids."""
    if gnu_base(lic) is None:
        return None
    if lic.endswith("-or-later") or lic.endswith("+"):
        return "or-later"
    if lic.endswith("-only"):
        return "only"
    return None


def base_id(lic: str) -> str:
    """The id without -only, -or-later and +, for GNU licenses; other ids as they are."""
    b = gnu_base(lic)
    return b if b else lic


def base_set(expr: str | None) -> frozenset:
    """The base license ids of an expression (exceptions dropped), for comparisons."""
    if not expr or expr in ("NOASSERTION", "NONE"):
        return frozenset()
    try:
        ids = license_ids(expr)
    except ExpressionError:
        ids = [x for x in re.split(r"[;,\s]+|\bAND\b|\bOR\b", expr) if x]
    return frozenset(base_id(i) for i in ids if i not in ("NOASSERTION", "NONE", "Other"))


def split_value(value: str | None) -> frozenset:
    """Base ids of a value that is either an SPDX expression or a ';'-joined list (the TOM 1.0.0 style)."""
    if not value or value in ("NOASSERTION", "NONE", "(missing)"):
        return frozenset()
    out = set()
    for part in value.split(";"):
        part = part.strip()
        if part:
            out |= base_set(part)
    return frozenset(out)


# The near relatives of a license text: one changed clause gives another license. On a near match of
# one of them, the SPDX template diff checks all of them.
RELATIVES = [
    {"Apache-2.0", "ECL-2.0"},
    {"BSD-3-Clause", "BSD-3-Clause-Clear", "BSD-4-Clause", "BSD-3-Clause-No-Nuclear-License",
     "BSD-3-Clause-No-Nuclear-Warranty", "BSD-3-Clause-Modification", "BSD-3-Clause-Attribution",
     "BSD-2-Clause", "BSD-4-Clause-UC"},
    {"BSD-2-Clause", "BSD-2-Clause-Patent", "BSD-2-Clause-Views", "BSD-1-Clause"},
    {"EPL-1.0", "CPL-1.0", "IPL-1.0", "EPL-2.0"},
    {"EUPL-1.0", "EUPL-1.1", "EUPL-1.2"},
    {"MPL-1.0", "MPL-1.1", "MPL-2.0", "MPL-2.0-no-copyleft-exception"},
    {"MIT", "MIT-0", "X11", "MIT-CMU", "MIT-advertising", "MIT-enna", "MIT-feh", "MIT-Modern-Variant"},
    {"GPL-2.0-only", "AGPL-1.0-only", "GPL-3.0-only", "LGPL-2.1-only"},
    {"Zlib", "zlib-acknowledgement", "Libpng"},
    {"ISC", "ISC-Veillard", "MIT"},
    {"CC-BY-4.0", "CC-BY-SA-4.0", "CC-BY-NC-4.0", "CC-BY-NC-SA-4.0", "CC-BY-ND-4.0"},
]


def relatives(lic: str) -> list[str]:
    out = {lic}
    for group in RELATIVES:
        if lic in group:
            out |= group
    return sorted(out, key=lambda x: (x != lic, x))


# --- Key-file roles --------------------------------------------------------------------------------

DOC_EXT = {"", ".txt", ".md", ".markdown", ".rst", ".html", ".htm", ".adoc", ".asciidoc", ".org", ".text",
           ".textile", ".pod", ".rtf", ".pdf"}
CODE_EXT = {".c", ".h", ".cc", ".cpp", ".cxx", ".hpp", ".hh", ".hxx", ".py", ".pyi", ".js", ".mjs", ".cjs",
            ".ts", ".tsx", ".jsx", ".java", ".kt", ".kts", ".scala", ".go", ".rs", ".rb", ".php", ".cs", ".fs",
            ".m", ".mm", ".swift", ".pl", ".pm", ".sh", ".bash", ".ps1", ".lua", ".dart", ".ex", ".exs", ".erl",
            ".hs", ".ml", ".mli", ".clj", ".el", ".vim", ".r", ".jl", ".groovy", ".gradle", ".cmake", ".mk",
            ".json", ".yml", ".yaml", ".toml", ".xml", ".ini", ".cfg", ".in", ".am", ".ac", ".svg", ".png",
            ".jpg", ".gif", ".ico", ".css", ".scss", ".less", ".vue", ".svelte", ".sql", ".proto", ".tpl",
            ".j2", ".jinja", ".erb", ".haml", ".tmpl", ".template", ".bat", ".cmd", ".nix", ".zig", ".v",
            ".sv", ".vhd", ".asm", ".s", ".d", ".nim", ".cr", ".f90", ".f", ".for", ".pas", ".ada", ".adb",
            ".ads", ".lisp", ".scm", ".rkt", ".tcl", ".awk", ".sed", ".po", ".pot", ".ts.map", ".map", ".lock",
            ".csproj", ".sln", ".vcxproj", ".pro", ".pri", ".qml", ".ui", ".glsl", ".hlsl", ".metal", ".cu"}
LEGAL_PREFIXES = ("license", "licence", "copying", "copyright", "notice", "unlicense", "eula")
_EXTRA_LEGAL = re.compile(r"^(mit[-_.]?licen[cs]e|a?l?gpl[-_.v0-9a-z]*|apache[-_.v0-9a-z]*|bsd[-_.v0-9a-z]*|"
                          r"mpl[-_.v0-9a-z]*|eula[-_.a-z0-9]*|artistic[-_.v0-9a-z]*|zlib[-_.]licen[cs]e)$")
README_RE = re.compile(r"^readme($|[._-])", re.I)
SPDX_DOC = re.compile(r"\.spdx(\.(json|rdf|xml|ya?ml|tv))?$")  # SPDX documents: their DataLicense is CC0-1.0
BUNDLED_DIRS = {"third_party", "third-party", "thirdparty", "3rdparty", "3rd_party", "3rd-party", "vendor",
                "vendors", "vendored", "external", "externals", "extern", "deps", "node_modules", "bower_components",
                "submodules", "subprojects", "bundled", "dependencies", "libs-external", "ext"}
# Words that, after LICENSE./COPYING., name a license (the project's) and not a dependency.
LICENSE_WORDS = re.compile(
    r"^(mit|x11|isc|zlib|bsd|bsd[0-9]|bsd-?[234]-?clause|apache|apache-?2(\.0)?|apachev2|asl|asl2|al2|"
    r"a?l?gpl|a?l?gpl-?v?[123](\.[01])?(\+|-or-later|-only)?|gnu|fdl|gfdl|lesser|lib|library|mpl|mpl-?[12](\.[01])?|"
    r"epl|epl-?[12](\.0)?|cddl|cc0|cc-?by.*|unlicense|wtfpl|bsl|boost|bsl-?1(\.0)?|artistic|perl|python|psf|"
    r"ofl|cecill.*|eupl.*|ms-?pl|ms-?rl|ncsa|upl|postgresql|curl|openssl|ssleay|libpng|zpl|public-?domain|pd|"
    r"spdx|commercial|proprietary|docs?|documentation|code|source|exceptions?|.*-exception|llvm.*|"
    r"dual|multi|txt|md|rst|html)$", re.I)
DEP_WORDS = re.compile(r"(third|3rd|vendor|deps|dependenc|bundled|external|libraries)", re.I)
MANIFEST_NAMES = {"package.json", "bower.json", "composer.json", "cargo.toml", "pom.xml", "setup.py", "setup.cfg",
                  "pyproject.toml", "pkg-info", "description", "meta.json", "meta.yml", "dist.ini", "vcpkg.json",
                  "conanfile.py", "chart.yaml", "pubspec.yaml", "haxelib.json", "elm.json", "meson.build",
                  "build.gradle", "build.gradle.kts", "gemfile", "go.mod", "package.xml", "metadata.json",
                  "mix.exs", "project.clj", "deps.edn", "rebar.config", "build.sbt", "flake.nix",
                  "default.nix", "cmakelists.txt", "configure.ac", "makefile.pl", "build.pl", "info.plist"}
# Build scripts: ScanCode may read a package from them, but their license matches are file headers and
# strings, not a declaration of the project.
BUILD_SCRIPTS = {"meson.build", "cmakelists.txt", "configure.ac", "build.gradle", "build.gradle.kts", "setup.py",
                 "conanfile.py", "makefile.pl", "build.pl", "default.nix", "flake.nix", "build.sbt", "mix.exs",
                 "project.clj", "rebar.config", "deps.edn", "gemfile", "go.mod", "info.plist"}
MANIFEST_EXT = {".gemspec", ".podspec", ".cabal", ".nuspec", ".opam", ".nimble", ".csproj", ".rockspec",
                ".spec"}


def _split_ext(name: str) -> tuple[str, str]:
    root, ext = posixpath.splitext(name)
    return root, ext.lower()


def is_legal_name(name: str, depth: int) -> bool:
    """A license, copying, copyright or notice file name (not a code file)."""
    low = name.lower()
    root, ext = _split_ext(low)
    if ext in CODE_EXT or SPDX_DOC.search(low):
        return False
    if low.startswith(LEGAL_PREFIXES):
        return True
    return depth == 0 and ext in DOC_EXT and bool(_EXTRA_LEGAL.match(root))


def is_manifest_name(name: str) -> bool:
    low = name.lower()
    return low in MANIFEST_NAMES or _split_ext(low)[1] in MANIFEST_EXT


def legal_suffix(name: str) -> str:
    """What follows the legal word: 'catch2' for LICENSE.catch2, '' for LICENSE, LICENSE.txt, COPYING."""
    low = name.lower()
    for p in sorted(LEGAL_PREFIXES, key=len, reverse=True):
        if low.startswith(p):
            rest = name[len(p):]
            break
    else:
        return name
    rest = re.sub(r"^(s$|s(?=[._ -]))", "", rest, flags=re.I)  # LICENSES.txt, COPYRIGHTS
    root, ext = _split_ext("x" + rest)
    if ext in DOC_EXT and ext:
        rest = root[1:]
    return rest.strip("._- ")


def path_role(path: str) -> str:
    """The role of a staged key file in the rule layer.

    primary   a root LICENSE/COPYING/COPYRIGHT/UNLICENSE file with no suffix or a format suffix
    alt       a root legal file whose suffix names a license (LICENSE-MIT, COPYING.LESSER, LICENSE.GPL)
    named     a root file named after a license, with no legal word (GPL.txt, MIT-LICENSE, lgpl-2.1.txt)
    secondary a root legal file whose suffix names something else (LICENSE.catch2, LICENSE-3RD-PARTY)
    notice    a root NOTICE file
    reuse     a file in the root LICENSES/ (or LICENCES/) folder
    bundled   a legal file in a bundled folder (third_party/, vendor/, ...) or in a lower-case licenses/
    sub       a legal file in another subfolder (depth 1 or 2)
    readme    a root README
    manifest  a package manifest (root) or a spec file
    debian    debian/copyright
    reuse-meta REUSE.toml or .reuse/dep5
    header    a file of the header pass
    other     anything else
    """
    parts = path.split("/")
    name = parts[-1]
    depth = len(parts) - 1
    low_parts = [p.lower() for p in parts[:-1]]
    if parts[0] == ".license-scan-headers":
        return "header"
    if path in ("REUSE.toml", ".reuse/dep5"):
        return "reuse-meta"
    if path.lower() == "debian/copyright":
        return "debian"
    if depth == 1 and parts[0] in ("LICENSES", "LICENCES"):
        return "reuse-meta" if README_RE.match(name) else "reuse"
    if any(p in BUNDLED_DIRS for p in low_parts):
        return "bundled"
    if depth >= 1 and low_parts[0] in ("licenses", "licences", "license", "licence", "legal", "notices"):
        return "bundled"
    if depth == 0 and README_RE.match(name):
        return "readme"
    if is_manifest_name(name) and (depth == 0 or name.lower().endswith(".spec")):
        return "manifest"
    if not is_legal_name(name, depth):
        return "other"
    if depth > 0:
        return "sub"
    low = name.lower()
    if low.startswith("notice"):
        return "notice"
    suf = legal_suffix(name)
    if not suf:
        return "primary"
    if not low.startswith(LEGAL_PREFIXES):  # GPL.txt, MIT-LICENSE, APACHE-2.0.txt: the name is a license
        return "named"
    if DEP_WORDS.search(suf):
        return "secondary"
    if LICENSE_WORDS.match(suf) or LICENSE_WORDS.match(suf.replace("_", "-")):
        return "alt"
    return "secondary"


# --- ScanCode evidence -----------------------------------------------------------------------------

EXACT_MATCHERS = {"1-hash", "1-spdx-id", "2-aho"}
# ScanCode keys that do not name a license of the project.
IGNORED_KEYS = {"unknown-license-reference", "unknown", "free-unknown", "unknown-spdx", "warranty-disclaimer",
                "generic-cla", "public-domain-disclaimer", "license-intro", "scancode-unknown"}


@dataclass
class Match:
    path: str
    role: str
    spdx: str                 # the SPDX expression of the match (usually one id)
    key: str                  # the ScanCode expression
    matcher: str
    score: float
    coverage: float
    rule: str
    kind: str                 # text, notice, reference, tag, intro, clue, unknown
    start_line: int = 0
    end_line: int = 0
    text: str = ""
    rule_length: int = 0

    @property
    def exact(self) -> bool:
        return self.matcher in EXACT_MATCHERS and self.score >= 100 and self.coverage >= 100

    def brief(self) -> dict:
        return {"spdx": self.spdx, "matcher": self.matcher, "score": self.score, "coverage": self.coverage,
                "rule": self.rule, "kind": self.kind, "lines": [self.start_line, self.end_line]}


@dataclass
class ScanIndex:
    matches: list = field(default_factory=list)          # [Match]
    files: dict = field(default_factory=dict)            # path -> file dict (ScanCode)
    exceptions: set = field(default_factory=set)         # SPDX ids of exceptions
    ignored_spdx: set = field(default_factory=set)       # SPDX ids of the IGNORED_KEYS and unknown/generic keys
    packages: list = field(default_factory=list)         # [(path, declared spdx, raw statement)]
    summary: dict = field(default_factory=dict)


def _rule_kind(ref: dict | None) -> str:
    if not ref:
        return "unknown"
    for k in ("text", "notice", "reference", "tag", "intro", "clue"):
        if ref.get(f"is_license_{k}"):
            return k
    return "unknown"


def index_scan(scan: dict, role_of=path_role) -> ScanIndex:
    """Flatten a ScanCode JSON (run with -l -p --classify --license-references) into matches."""
    rules = {r.get("identifier"): r for r in scan.get("license_rule_references", [])}
    idx = ScanIndex(summary=scan.get("summary") or {})
    for lic in scan.get("license_references", []):
        spdx = lic.get("spdx_license_key") or ""
        if lic.get("is_exception"):
            idx.exceptions.add(spdx)
        if lic.get("key") in IGNORED_KEYS or lic.get("is_unknown"):
            idx.ignored_spdx.add(spdx)
    for f in scan.get("files", []):
        if f.get("type") != "file":
            continue
        path = f["path"]
        idx.files[path] = f
        role = role_of(path)
        for det in f.get("license_detections") or []:
            for m in det.get("matches") or []:
                ref = rules.get(m.get("rule_identifier"))
                kind = "tag" if m.get("matcher") == "1-spdx-id" else _rule_kind(ref)
                idx.matches.append(Match(
                    path=path, role=role, spdx=m.get("license_expression_spdx") or "",
                    key=m.get("license_expression") or "", matcher=m.get("matcher") or "",
                    score=float(m.get("score") or 0), coverage=float(m.get("match_coverage") or 0),
                    rule=m.get("rule_identifier") or "", kind=kind,
                    start_line=int(m.get("start_line") or 0), end_line=int(m.get("end_line") or 0),
                    text=m.get("matched_text") or "", rule_length=int((ref or {}).get("length") or 0)))
        for pd in f.get("package_data") or []:
            dec = pd.get("declared_license_expression_spdx") or ""
            raw = pd.get("extracted_license_statement")
            if dec or raw:
                idx.packages.append((path, dec, raw if isinstance(raw, str) else repr(raw)))
    return idx


def _usable_ids(spdx: str, idx: ScanIndex) -> list[str]:
    try:
        ids = license_ids(spdx)
    except ExpressionError:
        return []
    return [i for i in ids if i not in idx.ignored_spdx and not i.startswith("LicenseRef-scancode-unknown")]


def _is_exception(lic: str, idx: ScanIndex) -> bool:
    return lic in idx.exceptions or lic.endswith("-exception") or "exception" in lic.lower()


def file_terms(matches: list, idx: ScanIndex) -> list:
    """The license terms of the matches of one file: ids and ("WITH", id, exc), exceptions paired.

    When the file has a license text, only its text matches (and the exceptions) give terms: the
    notices and references in it (a "GPL" mention, the license of a part) do not add licenses.
    A plain id goes when the same file gives it WITH an exception; an unversioned GPL mention
    (GPL-1.0-or-later) goes when the file names another GPL version."""
    terms, pending_exc = [], []

    def non_exc(m):
        return [i for i in _usable_ids(m.spdx, idx) if not _is_exception(i, idx)]

    texts = [m for m in matches if _text_like(m) and non_exc(m)]
    if texts:
        matches = texts + [m for m in matches if m not in texts and _usable_ids(m.spdx, idx)
                           and not non_exc(m)]
    for m in sorted(matches, key=lambda m: (m.start_line, m.end_line)):
        try:
            node = parse(m.spdx)
        except ExpressionError:
            continue
        for leaf in leaves(node):
            if isinstance(leaf, tuple):
                if leaf[1] not in idx.ignored_spdx and leaf not in terms:
                    terms.append(leaf)
                continue
            if leaf in idx.ignored_spdx or leaf.startswith("LicenseRef-scancode-unknown"):
                continue
            if _is_exception(leaf, idx):
                if leaf not in pending_exc:
                    pending_exc.append(leaf)
            elif leaf not in terms:
                terms.append(leaf)
    with_bases = {base_id(t[1]) for t in terms if isinstance(t, tuple) and t[0] == "WITH"}
    terms = [t for t in terms if not (isinstance(t, str) and base_id(t) in with_bases)]
    gpl_versions = {base_id(term_license(t)) for t in terms if base_id(term_license(t)).startswith("GPL-")}
    if "GPL-1.0" in gpl_versions and len(gpl_versions) > 1:
        terms = [t for t in terms if not (isinstance(t, str) and base_id(t) == "GPL-1.0")]
    plain = [t for t in terms if isinstance(t, str)]
    for exc in pending_exc:
        if any(isinstance(t, tuple) and t[0] == "WITH" for t in terms):
            continue  # the file gives its WITH expression already
        target = None
        bases = {base_id(t) for t in plain}
        if len(bases) == 1:
            target = plain[0]
        elif plain:
            target = plain[-1]
        if target is None:
            terms.append(("UNPAIRED", exc))
            continue
        terms = [("WITH", t, exc) if t == target else t for t in terms]
        plain = [t for t in terms if isinstance(t, str)]
    return terms


def term_license(term) -> str:
    return term if isinstance(term, str) else term[1]


def strip_choice(lic: str) -> str:
    """The id with its GNU -only/-or-later choice removed (GPL-2.0-only -> GPL-2.0)."""
    b = gnu_base(lic)
    return b if b else lic


def _term_base(term):
    if isinstance(term, str):
        return strip_choice(term)
    if term[0] == "WITH":
        return ("WITH", strip_choice(term[1]), term[2])
    return term


def _base_key(term) -> str:
    t = _term_base(term)
    return t if isinstance(t, str) else (f"{t[1]} WITH {t[2]}" if t[0] == "WITH" else f"? {t[1]}")


# --- The decision ----------------------------------------------------------------------------------

DECIDING_ROLES = ("primary", "alt", "named", "reuse", "secondary", "sub")
CHOICE_LEVELS = ("license-file", "readme", "manifest", "spec", "header")


@dataclass
class Decision:
    license: str = "NOASSERTION"
    version_choice: str = "n/a"             # or-later, only, unknown, mixed (GNU licenses); n/a for others
    version_choice_detail: dict = field(default_factory=dict)
    confidence: str = "none"                # high, medium, low, none
    evidence_kind: str = "none"
    evidence: list = field(default_factory=list)
    other_licenses: list = field(default_factory=list)
    bundled_licenses: list = field(default_factory=list)
    flags: list = field(default_factory=list)
    template_requests: list = field(default_factory=list)   # [(path, [ids])]
    needs_header_pass: bool = False
    second_opinions: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {k: getattr(self, k) for k in (
            "license", "version_choice", "version_choice_detail", "confidence", "evidence_kind", "evidence",
            "other_licenses", "bundled_licenses", "flags", "needs_header_pass", "second_opinions")} | {
            "template_requests": [[p, ids] for p, ids in self.template_requests]}


def _group(matches):
    by = {}
    for m in matches:
        by.setdefault(m.path, []).append(m)
    return by


WEAK_SCORE = 60.0
LOW_TEXT_PCT = 50.0   # ScanCode percentage_of_license_text under this: much text that no rule matches


def _weak(m: Match) -> bool:
    """A low-score fragment (a few words of a rule): not evidence of a license."""
    return m.score < WEAK_SCORE or (m.matcher == "3-seq" and m.coverage < WEAK_SCORE)


LONG_RULE = 300      # tokens: a "notice" rule this long holds a license text (epl-2.0_10.RULE has 2,119)


def _text_like(m: Match) -> bool:
    return m.kind in ("text", "unknown") or m.matcher == "1-hash" or (
        m.rule_length >= LONG_RULE and m.coverage >= 80)


def _template_outcome(path: str, lic: str, templates: dict) -> tuple[str | None, str]:
    """(accepted id or None, how) from the template results of one file. templates: path -> {id: result}."""
    res = templates.get(path)
    if not res:
        return None, "no-template-result"
    own = res.get(lic) or res.get(base_id(lic)) or {}
    if own.get("match") or own.get("within"):
        return lic, "template-match" if own.get("match") else "template-within"
    for rel in relatives(lic):
        r = res.get(rel) or {}
        if rel != lic and (r.get("match") or r.get("within")):
            return rel, "template-relative"
    return None, "template-no-match"


def _reuse_confirmed(path: str, matches: list, idx: ScanIndex) -> str | None:
    """For LICENSES/<id>.txt: the id when ScanCode finds that license text in the file."""
    stem = posixpath.splitext(path.split("/", 1)[1])[0]
    for m in matches:
        if not _text_like(m):
            continue
        for lic in _usable_ids(m.spdx, idx):
            if base_id(lic) == base_id(stem) or lic == stem or (
                    stem.startswith("LicenseRef-") and lic.startswith("LicenseRef-")):
                return stem if not stem.startswith("LicenseRef-") else lic
    return None


def _choice_claims(m: Match, base: str) -> str | None:
    """'or-later'/'only' when a match claims the version choice of a GNU base license."""
    for lic in _usable_ids(m.spdx, ScanIndex()):
        if gnu_base(lic) != base:
            continue
        ch = gnu_choice(lic)
        if ch is None:
            continue
        if ch == "or-later":
            return ch
        if m.kind != "text" and m.matcher != "1-hash":
            if m.kind == "tag" and m.text and "only" not in m.text.lower():
                continue  # a bare 'GPL-2.0' tag in a header: no explicit choice
            return ch
    return None


def _manifest_choice(raw: str, base: str) -> str | None:
    s = (raw or "").lower()
    fam, ver = base.split("-")
    major = ver.split(".")[0]
    if not re.search(rf"\b{fam.lower()}", s):
        return None
    if re.search(rf"{fam.lower()}[- v]*{major}(\.\d)?(\+|-or-later|\s*or\s*later|\s*or\s*(any\s*)?newer)", s):
        return "or-later"
    if re.search(rf"{fam.lower()}[- v]*{major}(\.\d)?-only", s):
        return "only"
    return None


def decide(scan: dict, templates: dict | None = None, headers: dict | None = None,
           second: dict | None = None) -> Decision:
    """One decision for a project from its key-file scan.

    templates: path -> {spdx id: {"match": bool, "within": bool}} (SpdxTemplateDiff results)
    headers: the ScanCode JSON of the header pass (or None)
    second: {"licensee": [ids], "askalono": {path: [(id, score)]}}
    """
    templates = templates or {}
    idx = index_scan(scan)
    d = Decision()
    by_file = _group([m for m in idx.matches if m.role != "header" and not _weak(m)])

    # 1. per-file terms of the deciding roles, with the exactness of the evidence
    file_info = {}
    for path, ms in by_file.items():
        role = ms[0].role
        terms = file_terms(ms, idx)
        if not terms:
            continue
        lic_matches = [m for m in ms if _usable_ids(m.spdx, idx)]
        file_info[path] = {"role": role, "terms": terms, "matches": lic_matches}

    # template diff of near text matches in the deciding files
    accepted = {}   # (path, license) -> how: exact, template-*, notice, near
    rank = {"exact": 0, "template-match": 1, "template-within": 1, "template-relative": 2, "title-relative": 3,
            "notice": 3, "near": 4}
    for path, info in file_info.items():
        if info["role"] not in DECIDING_ROLES:
            continue
        for m in info["matches"]:
            for lic in _usable_ids(m.spdx, idx):
                if m.exact:
                    got, how = lic, "exact"
                elif not _text_like(m):
                    got, how = lic, "notice"
                else:
                    got, how = _template_outcome(path, lic, templates)
                    if got is None:
                        if how == "no-template-result" and (path, relatives(lic)) not in d.template_requests:
                            d.template_requests.append((path, relatives(lic)))
                        got, how = lic, "near"
                        title = _title_relative(info["matches"], m, lic, idx)
                        if title:
                            got, how = title, "title-relative"
                            info["terms"] = [_replace_lic(t, lic, got) for t in info["terms"]]
                            d.flags.append(f"title-relative:{path}:{lic}->{got}")
                    elif got != lic:
                        info["terms"] = [_replace_lic(t, lic, got) for t in info["terms"]]
                        d.flags.append(f"template-relative:{path}:{lic}->{got}")
                key = (path, got)
                if key not in accepted or rank[how] < rank[accepted[key]]:
                    accepted[key] = how

    def tier(role):
        return {p: i for p, i in file_info.items() if i["role"] == role}

    def has_text(info):
        return any(_text_like(m) for m in info["matches"])

    reuse_ok = {}
    for path, info in tier("reuse").items():
        lic = _reuse_confirmed(path, info["matches"], idx)
        if lic:
            reuse_ok[path] = lic
        else:
            d.flags.append(f"reuse-unconfirmed:{path}")

    # 2. the deciding tier: root license texts (primary, with the alt files), else the REUSE folder,
    #    else alt and named files, else root license files with notices only, else secondary files
    primary, alt, secondary, sub = tier("primary"), tier("alt"), tier("secondary"), tier("sub")
    named = tier("named")
    plural = {p for p in primary if re.match(r"(?i)^(licen[cs]es|copyrights)\b", p)}
    if any(p not in plural and has_text(i) for p, i in primary.items()):
        for p in plural:
            file_info[p]["role"] = "secondary"
            secondary[p] = primary.pop(p)
    primary_text = {p: i for p, i in primary.items() if has_text(i)}
    # an aggregate root license file (3 or more license texts: the project's, then bundled ones):
    # the first text is the project's license
    for p, i in primary_text.items():
        plain = [t for t in i["terms"] if not (isinstance(t, tuple) and t[0] == "UNPAIRED")]
        if len({strip_choice(term_license(t)) for t in plain}) >= 3:
            first = _first_text_term(i, idx)
            if first is not None:
                i["aggregate_rest"] = [t for t in plain if _base_key(t) != _base_key(first)]
                i["terms"] = [first]
                d.flags.append(f"aggregate-license-file:{p}")
    primary_notice = {p: i for p, i in primary.items() if p not in primary_text}
    if primary_text:
        deciding, kind = dict(primary_text), "text"
        deciding.update(alt)
    elif primary_notice and not (alt or named):
        deciding, kind = dict(primary_notice), "license-file-notice"
    elif reuse_ok and not primary_notice:
        deciding, kind = {p: file_info[p] for p in reuse_ok}, "reuse"
    elif alt or named:
        deciding, kind = dict(alt), "text"
        deciding.update(named)
    elif primary_notice:
        deciding, kind = dict(primary_notice), "license-file-notice"
    elif secondary:
        deciding, kind = dict(secondary), "text"
    else:
        deciding, kind = {}, "none"
    if kind == "text" and not any(has_text(i) for i in deciding.values()):
        kind = "license-file-notice"

    text_terms = {}
    for path, info in deciding.items():
        if kind == "reuse":
            if _is_exception(reuse_ok[path], idx):
                d.flags.append(f"reuse-exception-not-paired:{reuse_ok[path]}")
                continue
            text_terms.setdefault(_base_key(reuse_ok[path]), (reuse_ok[path], path))
            continue
        for t in info["terms"]:
            if isinstance(t, tuple) and t[0] == "UNPAIRED":
                d.flags.append(f"unpaired-exception:{t[1]}")
                continue
            text_terms.setdefault(_base_key(t), (t, path))
    all_root_texts = {}
    root_files = list(primary_text.items()) + list(alt.items()) + list(named.items()) + list(secondary.items())
    for path, info in root_files + [(p, file_info[p]) for p in reuse_ok]:
        if path in reuse_ok:
            if not _is_exception(reuse_ok[path], idx):
                all_root_texts.setdefault(_base_key(reuse_ok[path]), (reuse_ok[path], path))
            continue
        for t in info["terms"]:
            if not (isinstance(t, tuple) and t[0] == "UNPAIRED"):
                all_root_texts.setdefault(_base_key(t), (t, path))
    if kind == "license-file-notice":
        all_root_texts = dict(text_terms)

    # 3. root notices that may choose among the root texts: manifests, READMEs, root license files
    #    with notices only. Per source: each OR expression alone, then all its ids together.
    sources = []  # (level, path, [candidate trees])
    for path, dec, raw in idx.packages:
        if path_role(path) == "manifest" and "/" not in path and dec:
            try:
                sources.append(("manifest", path, [parse(dec)]))
            except ExpressionError:
                pass
    for path, info in sorted(file_info.items()):
        role = info["role"]
        if role == "manifest" and ("/" in path or path.lower() in BUILD_SCRIPTS):
            continue
        if role not in ("manifest", "readme", "notice") and not (role == "primary" and path in primary_notice):
            continue
        cands, ids = [], []
        # an SPDX-License-Identifier line in a README or NOTICE is the license of that file (REUSE headers)
        good = [m for m in info["matches"] if not _text_like(m) and m.kind in ("notice", "reference", "tag")
                and m.score >= 90 and m.coverage >= 90 and not (m.matcher == "1-spdx-id" and role != "manifest")]
        for m in sorted(info["matches"], key=lambda m: (m.start_line, m.end_line)):
            if m not in good:
                continue
            try:
                node = parse(m.spdx)
            except ExpressionError:
                continue
            if isinstance(node, tuple) and node[0] == "OR":
                cands.append(node)
            for i in license_ids(m.spdx):
                if i not in idx.ignored_spdx and not _is_exception(i, idx) and i not in ids:
                    ids.append(i)
        if ids:
            cands.append(join_and(ids))
        if cands:
            level = {"manifest": "manifest", "readme": "readme", "notice": "notice-file"}.get(role, "license-file-notice")
            sources.append((level, path, cands))
    # an OR declaration inside a root license text file ("SPDX-License-Identifier: GPL-2.0-only OR
    # BSD-2-Clause" above the GPL text): it may name a license whose text is not in the root
    for path, info in sorted(primary_text.items()):
        first = _first_text_term(info, idx)
        text_lines = [m.start_line for m in info["matches"] if _text_like(m)]
        first_line = min(text_lines) if text_lines else 0
        ors = []
        for m in info["matches"]:
            if _text_like(m) or m.score < 90 or m.coverage < 90 or m.start_line > first_line:
                continue
            try:
                node = parse(m.spdx)
            except ExpressionError:
                continue
            if isinstance(node, tuple) and node[0] == "OR" and first is not None and \
                    strip_choice(term_license(first)) in {strip_choice(i) for i in license_ids(m.spdx)}:
                ors.append(node)
        if ors:
            sources.append(("license-file-or", path, ors[:1]))
    order = {"manifest": 0, "license-file-or": 1, "readme": 2, "notice-file": 3, "license-file-notice": 4}
    sources.sort(key=lambda x: order[x[0]])
    chosen = None
    root_keys = {strip_choice(term_license(t)) for t, _ in all_root_texts.values()}
    # the license of a plain LICENSE/COPYING text stays: a notice may drop other root texts only
    primary_keys = set()
    for i in primary_text.values():
        first = _first_text_term(i, idx)
        if first is not None:
            primary_keys.add(strip_choice(term_license(first)))
    for level, path, cands in sources:
        for node in cands:
            ids = [i for i in license_ids(render(node)) if i not in idx.ignored_spdx]
            keys = {strip_choice(i) for i in ids if not _is_exception(i, idx)}
            is_or = isinstance(node, tuple) and node[0] == "OR"
            if level == "license-file-or" and primary_keys <= keys:
                chosen = (level, path, node)
                break
            if keys and keys <= root_keys and primary_keys <= keys and (len(keys) < len(root_keys) or is_or):
                chosen = (level, path, node)
                break
        if chosen:
            break
    notices = [(level, path, render(c)) for level, path, cands in sources for c in cands[-1:]]

    if chosen:
        level, path, node = chosen
        final = _attach_exceptions(node, all_root_texts)
        d.evidence_kind = "text+notice"
        d.evidence.append({"path": path, "role": level, "expression": render(node), "use": "chooses"})
        used = {strip_choice(term_license(t)) for t in leaves(final)}
        for k, (t, p) in all_root_texts.items():
            if strip_choice(term_license(t)) in used:
                d.evidence.append(_ev(p, file_info.get(p), accepted, t))
    elif text_terms:
        terms = [t for t, _ in text_terms.values()]
        keys = {strip_choice(term_license(t)) for t in terms}
        if {"GPL-3.0", "LGPL-3.0"} <= keys:
            terms = [t for t in terms if strip_choice(term_license(t)) != "GPL-3.0"]
            d.flags.append("lgpl-3.0-with-gpl-3.0-text")
        final = join_and(terms)
        d.evidence_kind = kind
        for k, (t, p) in text_terms.items():
            if t in terms:
                d.evidence.append(_ev(p, file_info.get(p), accepted, t))
    else:
        final = None
        for level, path, expr in notices:
            if level in ("manifest", "readme", "license-file-notice", "notice-file"):
                try:
                    node = parse(expr)
                except ExpressionError:
                    continue
                if [i for i in license_ids(expr) if i not in idx.ignored_spdx]:
                    final = node
                    d.evidence_kind = level
                    d.evidence.append({"path": path, "role": level, "expression": expr, "use": "declares"})
                    break
        if final is None:
            subs = [t for i in sub.values() for t in i["terms"] if not (isinstance(t, tuple) and t[0] == "UNPAIRED")]
            if subs:
                final = join_and(subs)
                d.evidence_kind = "sub-folder"
                for p in sorted(sub):
                    d.evidence.append({"path": p, "role": "sub", "expression": render(join_and(sub[p]["terms"]))})
            else:
                debian = [t for p, i in file_info.items() if i["role"] == "debian" for t in i["terms"]
                          if not (isinstance(t, tuple) and t[0] == "UNPAIRED")]
                if debian:
                    final = join_and(debian)
                    d.evidence_kind = "debian"
    if final is None:
        d.flags.append("no-license-found")
        _bundled(d, file_info, idx, set())
        _confidence(d, second)
        return d

    # a license file with much text that no rule matches (a custom license around a known text)
    for e in d.evidence:
        f = idx.files.get(e.get("path"), {})
        pct = f.get("percentage_of_license_text")
        if e.get("role") in ("primary", "alt", "named", "secondary") and isinstance(pct, (int, float)) \
                and pct < LOW_TEXT_PCT and f"low-license-text:{e['path']}:{pct:g}" not in d.flags:
            d.flags.append(f"low-license-text:{e['path']}:{pct:g}")

    # 4. the GNU version choice from notices only
    final = _apply_choices(d, final, file_info, idx, headers, reuse_ok)
    d.license = render(final)
    used = {base_id(term_license(t)) for t in leaves(final)}
    _bundled(d, file_info, idx, used)
    rest = [(t, p) for p, i in file_info.items() for t in i.get("aggregate_rest", [])]
    for t, p in list(all_root_texts.values()) + rest:
        lic = base_id(term_license(t))
        if lic not in used and lic not in d.other_licenses:
            d.other_licenses.append(lic)
    _confidence(d, second)
    return d


def _first_text_term(info, idx):
    """The term of the first license text (by line) of a file, with its WITH exception."""
    texts = sorted((m for m in info["matches"] if _text_like(m)), key=lambda m: (m.start_line, m.end_line))
    for m in texts:
        ids = [i for i in _usable_ids(m.spdx, idx) if not _is_exception(i, idx)]
        if not ids:
            continue
        for t in info["terms"]:
            if not (isinstance(t, tuple) and t[0] == "UNPAIRED") and base_id(term_license(t)) == base_id(ids[0]):
                return t
        return ids[0]
    return None


def _title_relative(matches, near, lic, idx):
    """A near text of lic, and an exact notice just above it (at most 6 lines) that names a close relative
    of lic (the title line "BSD 3-Clause Clear License" above a near BSD-3-Clause text): the relative."""
    rel = set(relatives(lic)) - {lic}
    if gnu_base(lic):
        return None
    for m in matches:
        if m.exact and not _text_like(m) and m.start_line <= near.start_line and \
                near.start_line - m.end_line <= 6:
            for i in _usable_ids(m.spdx, idx):
                if i in rel:
                    return i
    return None


def _replace_lic(term, old, new):
    if isinstance(term, str):
        return new if term == old else term
    if term[0] == "WITH" and term[1] == old:
        return ("WITH", new, term[2])
    return term


def _attach_exceptions(node, root_texts):
    """In a notice expression, put back the WITH exception that the root texts give to a license."""
    with_map = {}
    for t, _ in root_texts.values():
        if isinstance(t, tuple) and t[0] == "WITH":
            with_map[strip_choice(t[1])] = t[2]

    def walk(n):
        if isinstance(n, str):
            exc = with_map.get(strip_choice(n))
            return ("WITH", n, exc) if exc else n
        if n[0] == "WITH":
            return n
        return (n[0], [walk(k) for k in n[1]])
    return walk(node)


def _ev(path, info, accepted, term):
    lic = term_license(term)
    hows = sorted({h for (p, l), h in accepted.items() if p == path and base_id(l) == base_id(lic)})
    ms = [m.brief() for m in (info or {}).get("matches", []) if base_id(lic) in
          {base_id(x) for x in _usable_ids(m.spdx, ScanIndex())}]
    return {"path": path, "role": (info or {}).get("role", "reuse"), "term": render(term),
            "accepted_as": hows or ["reuse"], "matches": ms[:6]}


def _bundled(d, file_info, idx, used):
    for path, info in sorted(file_info.items()):
        if info["role"] in ("bundled", "notice", "sub"):
            for t in info["terms"]:
                if isinstance(t, tuple) and t[0] == "UNPAIRED":
                    continue
                lic = base_id(term_license(t))
                if lic not in used and lic not in d.bundled_licenses:
                    d.bundled_licenses.append(lic)


def _apply_choices(d, final, file_info, idx, headers, reuse_ok=None):
    """Set -only/-or-later on the GNU ids of the value from notices; the rest have no suffix.

    A REUSE file name (LICENSES/GPL-2.0-or-later.txt, its text confirmed) counts as a notice."""
    claims = {}   # base -> level -> {choice: [paths]}

    def add(base, level, ch, path):
        claims.setdefault(base, {}).setdefault(level, {}).setdefault(ch, []).append(path)

    bases = sorted({gnu_base(term_license(t)) for t in leaves(final) if gnu_base(term_license(t))})
    if not bases:
        return final
    level_of = {"primary": "license-file", "alt": "license-file", "named": "license-file", "reuse": "license-file",
                "readme": "readme",
                "manifest": "manifest", "debian": "spec", "secondary": None, "sub": None, "bundled": None,
                "notice": "readme"}
    for path, info in file_info.items():
        level = level_of.get(info["role"])
        if info["role"] == "manifest" and path.lower().endswith(".spec"):
            level = "spec"
        if info["role"] == "manifest" and path.lower() in BUILD_SCRIPTS:
            continue
        if not level:
            continue
        for m in info["matches"]:
            for base in bases:
                ch = _choice_claims(m, base)
                if ch:
                    add(base, level, ch, path)
    for path, lic in (reuse_ok or {}).items():
        base, ch = gnu_base(lic), gnu_choice(lic)
        if base in bases and ch:
            add(base, "license-file", ch, path)
    for path, dec, raw in idx.packages:
        for base in bases:
            ch = _manifest_choice(raw, base)
            if ch:
                add(base, "manifest" if not path.lower().endswith(".spec") else "spec", ch, path)
    header_counts = {}
    if headers:
        hidx = index_scan(headers, role_of=lambda p: "header")
        per_file = {}
        for m in hidx.matches:
            for base in bases:
                ch = _choice_claims(m, base)
                if ch:
                    per_file.setdefault(base, {}).setdefault(m.path, set()).add(ch)
        for base, files in per_file.items():
            n_later = sum(1 for chs in files.values() if chs == {"or-later"})
            n_only = sum(1 for chs in files.values() if chs == {"only"})
            header_counts[base] = {"or-later": n_later, "only": n_only, "files": len(files)}
            tot = n_later + n_only
            if tot:
                if n_later / tot >= 0.9:
                    add(base, "header", "or-later", f"{n_later} headers")
                elif n_only / tot >= 0.9:
                    add(base, "header", "only", f"{n_only} headers")
                else:
                    add(base, "header", "or-later", f"{n_later} headers")
                    add(base, "header", "only", f"{n_only} headers")
    choice = {}
    for base in bases:
        lv = claims.get(base, {})
        got = None
        for level in CHOICE_LEVELS:
            if level in lv:
                chs = lv[level]
                got = (next(iter(chs)) if len(chs) == 1 else "mixed", level, {c: sorted(v)[:5] for c, v in chs.items()})
                break
        choice[base] = got
        d.version_choice_detail[base] = {"choice": got[0] if got else "unknown", "level": got[1] if got else None,
                                         "sources": got[2] if got else {}}
        if base in header_counts:
            d.version_choice_detail[base]["headers"] = header_counts[base]
    vals = {(c[0] if c else "unknown") for c in choice.values()}
    d.version_choice = vals.pop() if len(vals) == 1 else "several"
    if any(c is None for c in choice.values()) and headers is None:
        d.needs_header_pass = True

    def fix(lic):
        base = gnu_base(lic)
        if not base:
            return lic
        c = choice.get(base)
        if c and c[0] in ("or-later", "only"):
            if base.startswith("GFDL") and "invariants" in lic and c[0]:
                pass
            return f"{base}-{c[0]}"
        return base

    def walk(n):
        if isinstance(n, str):
            return fix(n)
        if n[0] == "WITH":
            return ("WITH", fix(n[1]), n[2])
        return (n[0], [walk(k) for k in n[1]])
    out = walk(final)
    if isinstance(out, tuple) and out[0] == "AND":
        out = join_and(out[1])
    return out


def agreement(ours: str, other: frozenset | None) -> str:
    """agree, partial, disagree or none, on base ids."""
    if other is None or not other:
        return "none"
    mine = base_set(ours)
    if not mine:
        return "disagree"
    if mine == other:
        return "agree"
    if mine & other:
        return "partial"
    return "disagree"


def _confidence(d: Decision, second: dict | None) -> None:
    second = second or {}
    lic_ids = frozenset(base_id(x) for x in second.get("licensee") or [] if x and x not in ("NOASSERTION", "other"))
    ask = set()
    ask_dropped = []
    ours = base_set(d.license)
    ask_files = second.get("askalono") or {}
    ev_paths = {e.get("path") for e in d.evidence}
    if ev_paths & set(ask_files):
        ask_files = {p: v for p, v in ask_files.items() if p in ev_paths}
    for path, vals in ask_files.items():
        for lic, score in vals:
            if score < 0.9:
                continue
            if base_id(lic).startswith("AGPL-1.0") and base_id(lic) not in ours:
                ask_dropped.append(f"{path}:{lic}")
                continue
            ask.add(base_id(lic))
    d.second_opinions = {"licensee": sorted(lic_ids), "askalono": sorted(ask),
                         "licensee_agreement": agreement(d.license, lic_ids),
                         "askalono_agreement": agreement(d.license, frozenset(ask))}
    if ask_dropped:
        d.second_opinions["askalono_dropped"] = ask_dropped
    if d.license == "NOASSERTION":
        d.confidence = "none"
        return
    hows = {h for e in d.evidence for h in e.get("accepted_as", [])}
    exact_only = hows and hows <= {"exact", "notice", "template-match", "template-within", "template-relative",
                                   "reuse"}
    disagree = d.second_opinions["licensee_agreement"] == "disagree" and \
        d.second_opinions["askalono_agreement"] in ("disagree", "none")
    unknown_ref = any(i.startswith("LicenseRef-scancode-") for i in ours)
    if unknown_ref and "near" in hows:
        d.confidence = "low"
        return
    unpaired = any(f.startswith("unpaired-exception") for f in d.flags)
    if d.evidence_kind == "license-file-notice":
        d.confidence = "low" if disagree else "medium"
    elif d.evidence_kind in ("text", "text+notice", "reuse") and exact_only and not unpaired:
        aggregate = any(f.startswith(("aggregate-license-file", "low-license-text")) for f in d.flags)
        d.confidence = "medium" if disagree or unknown_ref or aggregate else "high"
    elif d.evidence_kind in ("text", "text+notice", "reuse", "manifest", "license-file"):
        d.confidence = "low" if (disagree or unpaired) else "medium"
    else:
        d.confidence = "low"
