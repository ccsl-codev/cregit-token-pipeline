#!/usr/bin/env python3
"""Build the table that says, for each tokei language, whether it is a programming language.

    ./build_programming_table.py --languages-yml languages.yml [--tokei tokei]
                                 [--out data/tokei14_programming.tsv]

language_filter.py counts only the code lines of programming languages, and it
reads that decision from this table. The table has one row per language that
`tokei --languages` lists:

  tokei_language  the tokei name, as in `tokei --output json`
  programming     yes or no
  decided_by      name: the tokei name is a Linguist language name (compared
                  without case), and Linguist's `type` decides; programming is
                  yes only for `type: programming`. hand: an entry of HAND
                  below. unmapped: neither; programming is no, and the build
                  exits 1 so that a new tokei language gets a decision
  linguist        the Linguist language and its type, or the reason of a hand entry

The shipped table was built from tokei 14.0.0 and Linguist's
lib/linguist/languages.yml at commit 538da05f034fa5c83bd81df128d144e208898da3
(sha256 183243e30496ba53f5f8743b0e39c9f0e0bccc5a32639f7b541cc2285db0e043):

  curl -LO https://raw.githubusercontent.com/github-linguist/linguist/538da05f034fa5c83bd81df128d144e208898da3/lib/linguist/languages.yml
  nix develop --command ./build_programming_table.py --languages-yml languages.yml
"""
from __future__ import annotations

import argparse
import collections
import subprocess
import sys
from pathlib import Path

OUT = Path(__file__).resolve().parent / "data" / "tokei14_programming.tsv"
HEADER = ("tokei_language", "programming", "decided_by", "linguist")

# tokei languages whose name is not a Linguist language name (compared without
# case), decided by hand: name -> (programming?, the Linguist language it maps
# to, or "none: <reason>").
HAND = {
    "Alex": (True, "Lex"), "Arduino C++": (True, "C++"), "Ark TypeScript": (True, "ArkTS"),
    "Arturo": (True, "none: general-purpose language"), "ASP": (True, "Classic ASP"),
    "GNU Style Assembly": (True, "Unix Assembly"), "Autoconf": (True, "M4Sugar"), "Automake": (True, "Makefile"),
    "AXAML": (False, "XML"), "BASH": (True, "Shell"), "Batch": (True, "Batchfile"), "Bazel": (True, "Starlark"),
    "Bean": (False, "none: Beancount ledger data"), "C Header": (True, "C"), "C++ Header": (True, "C++"),
    "C Shell": (True, "Tcsh"), "Cabal": (False, "Cabal Config"), "Cassius": (False, "none: CSS template"),
    "CIL (SELinux)": (False, "CIL"), "ClojureC": (True, "Clojure"), "ClojureScript": (True, "Clojure"),
    "Cogent": (True, "none: general-purpose language"), "ColdFusion CFScript": (True, "ColdFusion CFC"),
    "Coq": (True, "Rocq Prover"), "C++ Module": (True, "C++"), "DAML": (True, "none: smart-contract language"),
    "Device Tree": (False, "none: hardware description data"), ".NET Resource": (False, "XML"),
    "Dream Maker": (True, "DM"), "Dust.js": (False, "none: HTML template"),
    "EdgeDB Schema Definition": (True, "EdgeQL"), "Ebuild": (True, "Gentoo Ebuild"),
    "8th": (True, "none: general-purpose language"), "Emacs Dev Env": (True, "Emacs Lisp"),
    "Emojicode": (True, "none: general-purpose language"), "FEN": (False, "none: chess position data"),
    "FlatBuffers Schema": (False, "FlatBuffers"), "Forge Config": (False, "none: configuration data"),
    "FORTRAN Legacy": (True, "Fortran"), "FORTRAN Modern": (True, "Fortran Free Form"),
    "GDB Script": (True, "GDB"), "Gherkin (Cucumber)": (True, "Gherkin"), "Gml": (True, "Game Maker Language"),
    "Go HTML": (False, "Go Template"), "Gwion": (True, "none: general-purpose language"),
    "Hamlet": (False, "none: HTML template"), "Happy": (True, "Yacc"),
    "Headache": (True, "none: general-purpose language"), "HEX": (False, "none: hex data"),
    "Hex0": (False, "none: hex data"), "Hex1": (False, "none: hex data"), "Hex2": (False, "none: hex data"),
    "HICAD": (True, "none: CAD macro language"), "hledger": (False, "none: ledger data"),
    "Intel HEX": (False, "none: hex data"), "JSLT": (True, "none: JSON transform language, like jq"),
    "Jinja2": (False, "Jinja"), "JSX": (True, "JavaScript"), "Julius": (True, "JavaScript"),
    "Jupyter Notebooks": (False, "Jupyter Notebook"), "K": (True, "none: array language"),
    "Kaem": (True, "none: bootstrap shell"), "Kakoune script": (True, "KakouneScript"),
    "Korn shell": (True, "Shell"), "KV Language": (False, "kvlang"), "LALRPOP": (True, "Yacc"),
    "Lingua Franca": (True, "none: general-purpose language"), "LD Script": (True, "Linker Script"),
    "Lucius": (False, "none: CSS template"), "M1 Assembly": (True, "Assembly"),
    "Madlang": (True, "none: text-generation language"), "Menhir": (True, "Yacc"),
    "Metal Shading Language": (True, "Metal"), "Mlatu": (True, "none: general-purpose language"),
    "Module-Definition": (False, "none: Windows .def export list"), "MSBuild": (False, "XML"),
    "Not Quite Perl": (True, "Raku"), "NuGet Config": (False, "XML"),
    "Lauterbach PRACTICE Script": (True, "none: debugger script language"), "PRQL": (False, "SQL (data)"),
    "PSL Assertion": (True, "none: hardware property language"), "Pacman's makepkg": (True, "Shell"),
    "Pest": (True, "none: PEG grammar, like Yacc"), "Phix": (True, "none: general-purpose language"),
    "PO File": (False, "Gettext Catalog"), "Poke": (True, "none: GNU poke language"),
    "Polly": (True, "none: general-purpose language"), "Protocol Buffers": (False, "Protocol Buffer"),
    "QCL": (True, "none: quantum programming language"), "Rusty Object Notation": (False, "RON"),
    "RPM Specfile": (False, "RPM Spec"), "Rakefile": (True, "Ruby"), "Razor": (False, "HTML+Razor"),
    "Ruby HTML": (False, "HTML+ERB"), "SIL": (True, "none: general-purpose language"), "Scons": (True, "Python"),
    "Standard ML (SML)": (True, "Standard ML"), "Specman e": (True, "none: hardware verification language"),
    "Spice Netlist": (False, "none: circuit netlist data"), "Stratego/XT": (True, "none: transformation language"),
    "Tera": (False, "none: HTML template"), "Plain Text": (False, "Text"),
    "TTCN-3": (True, "none: test language"), "UMPL": (True, "none: general-purpose language"),
    "Uiua": (True, "none: array language"), "Unison": (True, "none: general-purpose language"),
    "Unreal Markdown": (False, "Markdown"), "Unreal Plugin": (False, "JSON"), "Unreal Project": (False, "JSON"),
    "Unreal Script": (True, "UnrealScript"), "Unreal Shader": (True, "HLSL"), "Unreal Shader Header": (True, "HLSL"),
    "Ur/Web": (True, "UrWeb"), "Ur/Web Project": (False, "none: project file data"), "VB6/VBA": (True, "VBA"),
    "Apache Velocity": (False, "Velocity Template Language"), "Verilog Args File": (False, "none: argument list"),
    "Virgil": (True, "none: general-purpose language"), "Visual Basic": (True, "Visual Basic .NET"),
    "Visual Studio Project": (False, "XML"), "Visual Studio Solution": (False, "Microsoft Visual Studio Solution"),
    "WebGPU Shader Language": (True, "WGSL"), "The WenYan Programming Language": (True, "none: general-purpose language"),
    "Wolfram": (True, "Wolfram Language"), "XSL": (True, "XSLT"), "XAML": (False, "XML"),
    "Xcode Config": (False, "none: build configuration data"), "ZenCode": (True, "ZenScript"),
    "ZoKrates": (True, "none: zero-knowledge proof language"), "Zsh": (True, "Shell"),
}


def tokei_languages(tokei: str) -> list[str]:
    """The language names in the table that `tokei --languages` prints, in its order."""
    out = subprocess.run([tokei, "--languages"], capture_output=True, text=True,
                         check=True).stdout.splitlines()
    head = next(line for line in out if "Language" in line and "Extensions" in line)
    a, b = head.index("Language"), head.index("Extensions")
    names = []
    for line in out:
        if not line.startswith("┃") or line is head or "━" in line:
            continue
        name = line[a:b].strip()
        if name and name != "Language":
            names.append(name)
    return names


def linguist(text: str) -> dict[str, str | None]:
    """Language name -> type, from a plain parse of the top-level keys of languages.yml."""
    langs: dict[str, str | None] = {}
    cur = None
    for line in text.splitlines():
        if not line or line.startswith("#") or line.startswith("---"):
            continue
        if not line.startswith(" ") and line.rstrip().endswith(":"):
            cur = line.rstrip()[:-1].strip().strip('"').strip("'")
            langs[cur] = None
        elif cur and line.startswith("  type:"):
            langs[cur] = line.split(":", 1)[1].strip()
    return langs


def build(names: list[str], types: dict[str, str | None]) -> tuple[list[tuple[str, ...]], list[str]]:
    """The table rows, and the names that neither Linguist nor HAND decides."""
    lower = {k.lower(): k for k in types}
    rows, unmapped = [], []
    for name in names:
        if name.lower() in lower:
            k = lower[name.lower()]
            t = types[k]
            rows.append((name, "yes" if t == "programming" else "no", "name", f"{k} ({t})"))
        elif name in HAND:
            prog, why = HAND[name]
            if why in types:
                why = f"{why} ({types[why]})"
            rows.append((name, "yes" if prog else "no", "hand", why))
        else:
            unmapped.append(name)
            rows.append((name, "no", "unmapped", ""))
    return rows, unmapped


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--languages-yml", required=True, metavar="PATH",
                    help="Linguist's lib/linguist/languages.yml")
    ap.add_argument("--tokei", default="tokei", metavar="PATH", help="the tokei executable")
    ap.add_argument("--out", default=str(OUT), metavar="TSV", help="default: %(default)s")
    args = ap.parse_args(argv)

    rows, unmapped = build(tokei_languages(args.tokei),
                           linguist(Path(args.languages_yml).read_text()))
    Path(args.out).write_text("".join("\t".join(r) + "\n" for r in [HEADER, *rows]))
    counts = collections.Counter((r[2], r[1]) for r in rows)
    print(f"{len(rows)} tokei languages: {dict(sorted(counts.items()))}", file=sys.stderr)
    unused = sorted(set(HAND) - {r[0] for r in rows})
    if unused:
        print("HAND entries that are not tokei names: " + "; ".join(unused), file=sys.stderr)
    if unmapped:
        print("unmapped, add a HAND entry: " + "; ".join(unmapped), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
