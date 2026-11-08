"""Tests of license_rules: the SPDX expressions, the key-file roles and the decision rules.

The scans are small synthetic ScanCode JSON documents in the shape of ScanCode 32.5.0 output
(-l -p --classify --license-references): files[].license_detections[].matches[], license_references
and license_rule_references.
"""

import pytest

import license_rules as R


# --- builders --------------------------------------------------------------------------------------

def m(spdx, rule, kind="text", matcher="2-aho", score=100.0, cov=100.0, start=1, end=20, text=""):
    return {"spdx": spdx, "rule": rule, "kind": kind, "matcher": matcher, "score": score, "cov": cov,
            "start": start, "end": end, "text": text}


def f(path, *matches, package_data=None):
    return {"path": path, "matches": list(matches), "package_data": package_data or []}


EXCEPTIONS = {"fmt-exception", "Classpath-exception-2.0", "LLVM-exception"}
UNKNOWN = {"LicenseRef-scancode-unknown-license-reference": "unknown-license-reference",
           "LicenseRef-scancode-warranty-disclaimer": "warranty-disclaimer"}


def scan(*files):
    rules, lics, out_files = {}, {}, []
    for fl in files:
        dets = []
        for x in fl["matches"]:
            rules[x["rule"]] = {"identifier": x["rule"], **{f"is_license_{k}": k == x["kind"] for k in
                                                           ("text", "notice", "reference", "tag", "intro", "clue")}}
            for lic in R.license_ids(x["spdx"]):
                key = UNKNOWN.get(lic, lic.lower())
                lics[lic] = {"key": key, "spdx_license_key": lic, "is_exception": lic in EXCEPTIONS,
                             "is_unknown": lic in UNKNOWN, "is_generic": False}
            dets.append({"license_expression_spdx": x["spdx"], "matches": [{
                "license_expression_spdx": x["spdx"], "license_expression": x["spdx"].lower(),
                "matcher": x["matcher"], "score": x["score"], "match_coverage": x["cov"],
                "rule_identifier": x["rule"], "start_line": x["start"], "end_line": x["end"],
                "matched_text": x["text"]}]})
        out_files.append({"path": fl["path"], "type": "file", "license_detections": dets,
                          "package_data": fl["package_data"]})
    return {"files": out_files, "license_rule_references": list(rules.values()),
            "license_references": list(lics.values()), "summary": {}}


MIT = m("MIT", "mit.LICENSE")
APACHE = m("Apache-2.0", "apache-2.0.LICENSE")
GPL2_TEXT = m("GPL-2.0-only", "gpl-2.0.LICENSE")
GPL2_PLUS_NOTICE = m("GPL-2.0-or-later", "gpl-2.0-plus_12.RULE", kind="notice", start=1, end=4)


# --- expressions -----------------------------------------------------------------------------------

def test_parse_and_render_round_trip():
    for e in ["MIT", "MIT OR Apache-2.0", "GPL-2.0-only WITH Classpath-exception-2.0",
              "(MIT AND BSD-3-Clause) OR GPL-2.0-only", "MIT AND (Apache-2.0 OR BSD-2-Clause)"]:
        assert R.render(R.parse(e)) == e


def test_parse_lower_case_operators_and_errors():
    assert R.render(R.parse("mit or apache-2.0")) == "mit OR apache-2.0"
    for bad in ["", "MIT AND", "(MIT", "MIT)", "AND MIT", "MIT WITH", "MIT $ BSD"]:
        with pytest.raises(R.ExpressionError):
            R.parse(bad)


def test_license_ids_and_join_and():
    assert R.license_ids("MIT AND (MIT OR GPL-2.0-only WITH Classpath-exception-2.0)") == ["MIT", "GPL-2.0-only"]
    assert R.join_and([]) is None
    assert R.render(R.join_and(["MIT", "Apache-2.0", "MIT", ("AND", ["BSD-3-Clause", "Zlib"])])) == \
        "Apache-2.0 AND BSD-3-Clause AND MIT AND Zlib"


def test_gnu_ids():
    assert R.gnu_base("GPL-2.0-or-later") == "GPL-2.0"
    assert R.gnu_base("LGPL-2.1+") == "LGPL-2.1"
    assert R.gnu_base("GFDL-1.3-invariants-only") == "GFDL-1.3"
    assert R.gnu_base("GPL-2.0-with-classpath-exception") is None
    assert R.gnu_base("MIT") is None
    assert R.gnu_choice("GPL-3.0-only") == "only"
    assert R.gnu_choice("AGPL-3.0+") == "or-later"
    assert R.gnu_choice("GPL-3.0") is None
    assert R.gnu_choice("MIT") is None
    assert R.base_id("LGPL-3.0-or-later") == "LGPL-3.0"
    assert R.base_id("Apache-2.0") == "Apache-2.0"


def test_base_set_and_split_value():
    assert R.base_set("GPL-2.0-or-later OR MIT") == {"GPL-2.0", "MIT"}
    assert R.base_set("NOASSERTION") == set()
    assert R.base_set(None) == set()
    assert R.base_set("MIT;BSD-3-Clause") == {"MIT", "BSD-3-Clause"}
    assert R.split_value("BSD-3-Clause;CC0-1.0;GPL-2.0-or-later") == {"BSD-3-Clause", "CC0-1.0", "GPL-2.0"}
    assert R.split_value("NOASSERTION") == set()
    assert R.split_value("") == set()


def test_relatives():
    assert R.relatives("Apache-2.0")[0] == "Apache-2.0"
    assert "ECL-2.0" in R.relatives("Apache-2.0")
    assert R.relatives("Unknown-Id") == ["Unknown-Id"]


# --- roles -----------------------------------------------------------------------------------------

@pytest.mark.parametrize("path, role", [
    ("LICENSE", "primary"), ("LICENSE.txt", "primary"), ("COPYING", "primary"), ("LICENCE.md", "primary"),
    ("COPYRIGHT", "primary"), ("UNLICENSE", "primary"), ("LICENSES.txt", "primary"),
    ("LICENSE-MIT", "alt"), ("LICENSE-APACHE", "alt"), ("COPYING.LESSER", "alt"), ("LICENSE.LGPL2.1", "alt"),
    ("COPYING.LIB", "alt"), ("LICENSE.catch2", "secondary"), ("LICENSE-3RD-PARTY.txt", "secondary"),
    ("LICENSE.fmtlib", "secondary"), ("NOTICE", "notice"), ("NOTICE.txt", "notice"),
    ("GPL.txt", "named"), ("MIT-LICENSE", "named"), ("lgpl-2.1.txt", "named"),
    ("LICENSES/MIT.txt", "reuse"), ("LICENCES/GPL-2.0-or-later.txt", "reuse"),
    ("licenses/zlib.txt", "bundled"), ("third_party/foo/LICENSE", "bundled"), ("vendor/LICENSE", "bundled"),
    ("docs/LICENSE", "sub"), ("packages/core/COPYING", "sub"),
    ("README.md", "readme"), ("readme", "readme"), ("README-dev.rst", "readme"),
    ("package.json", "manifest"), ("Cargo.toml", "manifest"), ("foo.gemspec", "manifest"),
    ("rpm/foo.spec", "manifest"), ("sub/package.json", "other"),
    ("debian/copyright", "debian"), ("REUSE.toml", "reuse-meta"), (".reuse/dep5", "reuse-meta"),
    ("license.go", "other"), ("src/main.c", "other"), (".license-scan-headers/a.c", "header"),
])
def test_path_role(path, role):
    assert R.path_role(path) == role


def test_legal_suffix_and_names():
    assert R.legal_suffix("LICENSE") == ""
    assert R.legal_suffix("LICENSE.txt") == ""
    assert R.legal_suffix("LICENSE.catch2") == "catch2"
    assert R.legal_suffix("COPYING-MIT.md") == "MIT"
    assert R.legal_suffix("MIT-LICENSE") == "MIT-LICENSE"
    assert R.is_legal_name("COPYING", 2)
    assert not R.is_legal_name("copyright.py", 0)
    assert not R.is_legal_name("bsd.mk", 0)
    assert R.is_legal_name("BSD.txt", 0)
    assert not R.is_legal_name("BSD.txt", 1)
    assert R.is_manifest_name("Cargo.toml") and R.is_manifest_name("x.cabal") and not R.is_manifest_name("x.c")


# --- decisions -------------------------------------------------------------------------------------

def test_single_exact_mit():
    d = R.decide(scan(f("LICENSE", MIT)), second={"licensee": ["MIT"], "askalono": {"LICENSE": [["MIT", 1.0]]}})
    assert d.license == "MIT"
    assert d.confidence == "high"
    assert d.evidence_kind == "text"
    assert d.version_choice == "n/a"
    assert d.second_opinions["licensee_agreement"] == "agree"
    assert d.second_opinions["askalono_agreement"] == "agree"
    assert d.evidence[0]["accepted_as"] == ["exact"]
    assert d.as_dict()["license"] == "MIT"


def test_notice_chooses_among_root_texts_and_drops_bundled():
    d = R.decide(scan(
        f("LICENSE", m("BSD-3-Clause", "bsd-new.LICENSE")),
        f("LICENSE.catch2", m("BSL-1.0", "boost-1.0.LICENSE")),
        f("LICENSE.zlib", m("Zlib", "zlib.LICENSE")),
        f("README.md", m("BSD-3-Clause", "bsd-new_10.RULE", kind="reference")),
        f("third_party/x/LICENSE", MIT)))
    assert d.license == "BSD-3-Clause"
    assert d.evidence_kind == "text+notice"
    assert "BSL-1.0" in d.other_licenses
    assert d.bundled_licenses == ["MIT"]


def test_dual_license_from_manifest():
    d = R.decide(scan(
        f("LICENSE-MIT", MIT), f("LICENSE-APACHE", APACHE),
        f("Cargo.toml", package_data=[{"declared_license_expression_spdx": "MIT OR Apache-2.0",
                                        "extracted_license_statement": "MIT OR Apache-2.0"}])))
    assert d.license == "MIT OR Apache-2.0"
    assert d.evidence_kind == "text+notice"
    assert d.confidence == "high"


def test_two_root_texts_without_notice_give_and():
    d = R.decide(scan(f("LICENSE-MIT", MIT), f("LICENSE-APACHE", APACHE)))
    assert d.license == "Apache-2.0 AND MIT"


def test_gpl_text_alone_has_no_choice_and_asks_for_headers():
    d = R.decide(scan(f("COPYING", GPL2_TEXT)))
    assert d.license == "GPL-2.0"
    assert d.version_choice == "unknown"
    assert d.needs_header_pass
    hdr = scan(*[f(f"src/f{i}.c", m("GPL-2.0-or-later", "gpl-2.0-plus_3.RULE", kind="notice",
                                    text="either version 2 of the License, or (at your option) any later version"))
                 for i in range(3)])
    d2 = R.decide(scan(f("COPYING", GPL2_TEXT)), headers=hdr)
    assert d2.license == "GPL-2.0-or-later"
    assert d2.version_choice == "or-later"
    assert d2.version_choice_detail["GPL-2.0"]["level"] == "header"
    assert d2.version_choice_detail["GPL-2.0"]["headers"] == {"or-later": 3, "only": 0, "files": 3}
    assert not d2.needs_header_pass


def test_headers_mixed_and_only():
    later = [f(f"a{i}.c", m("GPL-2.0-or-later", "r1", kind="notice", text="or any later version")) for i in range(5)]
    only = [f(f"b{i}.c", m("GPL-2.0-only", "r2", kind="notice", text="version 2 only")) for i in range(5)]
    d = R.decide(scan(f("COPYING", GPL2_TEXT)), headers=scan(*later, *only))
    assert d.license == "GPL-2.0"
    assert d.version_choice == "mixed"
    d = R.decide(scan(f("COPYING", GPL2_TEXT)), headers=scan(*only))
    assert d.license == "GPL-2.0-only"
    bare_tag = [f("c.c", m("GPL-2.0-only", "spdx_tag", kind="tag", matcher="1-spdx-id",
                           text="SPDX-License-Identifier: GPL-2.0"))]
    d = R.decide(scan(f("COPYING", GPL2_TEXT)), headers=scan(*bare_tag))
    assert d.license == "GPL-2.0"
    assert d.version_choice == "unknown"
    assert not d.needs_header_pass


def test_notice_in_license_file_gives_choice():
    d = R.decide(scan(f("COPYING", GPL2_PLUS_NOTICE, m("GPL-2.0-only", "gpl-2.0.LICENSE", start=5, end=300))))
    assert d.license == "GPL-2.0-or-later"
    assert d.version_choice_detail["GPL-2.0"]["level"] == "license-file"


def test_readme_notice_of_another_version_does_not_set_choice():
    d = R.decide(scan(f("COPYING", m("GPL-3.0-only", "gpl-3.0.LICENSE")),
                      f("README", m("GPL-1.0-or-later", "gpl_70.RULE", kind="reference"))))
    assert d.license == "GPL-3.0"
    assert d.version_choice == "unknown"


def test_manifest_raw_statement_gives_choice():
    d = R.decide(scan(f("COPYING", m("LGPL-2.1-only", "lgpl-2.1.LICENSE")),
                      f("package.json", package_data=[{"declared_license_expression_spdx": "LGPL-2.1-or-later",
                                                       "extracted_license_statement": "LGPL-2.1+"}])))
    assert d.license == "LGPL-2.1-or-later"
    assert d.version_choice_detail["LGPL-2.1"]["level"] == "manifest"


def test_exception_and_becomes_with():
    d = R.decide(scan(f("LICENSE", m("MIT", "mit.LICENSE", start=1, end=20),
                        m("fmt-exception", "object-form-exception-to-mit.LICENSE", start=22, end=27))))
    assert d.license == "MIT WITH fmt-exception"


def test_exception_alone_is_flagged():
    d = R.decide(scan(f("LICENSE", m("LLVM-exception", "llvm-exception.LICENSE"))))
    assert d.license == "NOASSERTION"
    assert any(x.startswith("unpaired-exception") for x in d.flags)


def test_exception_on_notice_choice():
    d = R.decide(scan(
        f("LICENSE", m("Apache-2.0", "apache-2.0.LICENSE", start=1, end=200),
          m("LLVM-exception", "llvm-exception.LICENSE", start=201, end=220)),
        f("LICENSE.catch2", m("BSL-1.0", "boost-1.0.LICENSE")),
        f("README.md", m("Apache-2.0", "apache-2.0_5.RULE", kind="reference"))))
    assert d.license == "Apache-2.0 WITH LLVM-exception"


def test_near_match_asks_for_template_then_relative_wins():
    near = m("Apache-2.0", "apache-2.0.LICENSE", matcher="3-seq", score=94.0, cov=95.0)
    d = R.decide(scan(f("LICENSE", near)))
    assert d.template_requests and d.template_requests[0][0] == "LICENSE"
    assert "ECL-2.0" in d.template_requests[0][1]
    assert d.confidence == "medium"
    tmpl = {"LICENSE": {"Apache-2.0": {"match": False, "within": False}, "ECL-2.0": {"match": True}}}
    d2 = R.decide(scan(f("LICENSE", near)), templates=tmpl)
    assert d2.license == "ECL-2.0"
    assert any(x.startswith("template-relative") for x in d2.flags)
    assert d2.confidence == "high"
    d3 = R.decide(scan(f("LICENSE", near)), templates={"LICENSE": {"Apache-2.0": {"within": True}}})
    assert d3.license == "Apache-2.0"
    assert d3.evidence[0]["accepted_as"] == ["template-within"]
    d4 = R.decide(scan(f("LICENSE", near)), templates={"LICENSE": {"Apache-2.0": {"match": False}}})
    assert d4.license == "Apache-2.0"
    assert d4.evidence[0]["accepted_as"] == ["near"]
    assert not d4.template_requests


def test_reuse_folder_confirmed_by_text():
    d = R.decide(scan(f("LICENSES/GPL-2.0-or-later.txt", GPL2_TEXT),
                      f("LICENSES/CC0-1.0.txt", m("CC0-1.0", "cc0-1.0.LICENSE")),
                      f("LICENSES/BSD-2-Clause.txt", m("MIT", "mit.LICENSE"))))
    assert d.license == "CC0-1.0 AND GPL-2.0-or-later"
    assert d.evidence_kind == "reuse"
    assert "reuse-unconfirmed:LICENSES/BSD-2-Clause.txt" in d.flags


def test_lgpl3_with_gpl3_text():
    d = R.decide(scan(f("COPYING", m("GPL-3.0-only", "gpl-3.0.LICENSE")),
                      f("COPYING.LESSER", m("LGPL-3.0-only", "lgpl-3.0.LICENSE"))))
    assert d.license == "LGPL-3.0"
    assert "lgpl-3.0-with-gpl-3.0-text" in d.flags


def test_named_files_only_when_no_primary():
    d = R.decide(scan(f("COPYING", GPL2_TEXT, GPL2_PLUS_NOTICE), f("lgpl-2.1.txt", m("LGPL-2.1-only", "lgpl-2.1.LICENSE"))))
    assert d.license == "GPL-2.0-or-later"
    assert "LGPL-2.1" in d.other_licenses
    d = R.decide(scan(f("GPL.txt", m("GPL-3.0-only", "gpl-3.0.LICENSE"))))
    assert d.license == "GPL-3.0"


def test_unknown_references_are_ignored():
    unk = m("LicenseRef-scancode-unknown-license-reference", "unknown-license-reference_1.RULE", kind="reference")
    d = R.decide(scan(f("LICENSE", unk, MIT)))
    assert d.license == "MIT"
    d = R.decide(scan(f("LICENSE", unk)))
    assert d.license == "NOASSERTION"
    assert d.confidence == "none"
    assert "no-license-found" in d.flags


def test_fallbacks_manifest_readme_sub_debian():
    pj = f("package.json", package_data=[{"declared_license_expression_spdx": "MIT", "extracted_license_statement": "MIT"}])
    d = R.decide(scan(pj))
    assert (d.license, d.evidence_kind, d.confidence) == ("MIT", "manifest", "medium")
    d = R.decide(scan(f("README.md", m("BSD-2-Clause", "bsd-simplified_3.RULE", kind="notice"))))
    assert (d.license, d.evidence_kind, d.confidence) == ("BSD-2-Clause", "readme", "low")
    d = R.decide(scan(f("packages/a/LICENSE", MIT), f("packages/b/LICENSE", APACHE)))
    assert (d.license, d.evidence_kind, d.confidence) == ("Apache-2.0 AND MIT", "sub-folder", "low")
    d = R.decide(scan(f("debian/copyright", m("GPL-2.0-or-later", "gpl-2.0-plus_40.RULE", kind="notice"))))
    assert (d.license, d.evidence_kind) == ("GPL-2.0-or-later", "debian")


def test_secondary_only_and_spec_choice():
    d = R.decide(scan(f("LICENSE.mylib", MIT)))
    assert d.license == "MIT"
    d = R.decide(scan(f("COPYING", GPL2_TEXT), f("pkg.spec", m("GPL-2.0-or-later", "gpl-2.0-plus_7.RULE", kind="tag"))))
    assert d.license == "GPL-2.0-or-later"
    assert d.version_choice_detail["GPL-2.0"]["level"] == "spec"


def test_second_opinions_drop_askalono_agpl_and_disagreement_lowers_confidence():
    d = R.decide(scan(f("COPYING", GPL2_TEXT, GPL2_PLUS_NOTICE)),
                 second={"licensee": ["GPL-2.0"], "askalono": {"COPYING": [["AGPL-1.0-only", 0.95]]}})
    assert d.second_opinions["askalono"] == []
    assert d.second_opinions["askalono_dropped"] == ["COPYING:AGPL-1.0-only"]
    assert d.confidence == "high"
    d = R.decide(scan(f("LICENSE", MIT)), second={"licensee": ["ISC"], "askalono": {"LICENSE": [["ISC", 0.99]]}})
    assert d.confidence == "medium"
    d = R.decide(scan(f("LICENSE", MIT)), second={"licensee": [], "askalono": {"LICENSE": [["MIT", 0.5]]}})
    assert d.second_opinions["askalono_agreement"] == "none"


def test_agreement():
    assert R.agreement("MIT", frozenset({"MIT"})) == "agree"
    assert R.agreement("MIT AND Zlib", frozenset({"MIT"})) == "partial"
    assert R.agreement("MIT", frozenset({"ISC"})) == "disagree"
    assert R.agreement("NOASSERTION", frozenset({"ISC"})) == "disagree"
    assert R.agreement("MIT", frozenset()) == "none"
    assert R.agreement("MIT", None) == "none"


def test_index_scan_skips_directories_and_reads_packages():
    s = scan(f("LICENSE", MIT))
    s["files"].append({"path": "src", "type": "directory"})
    s["files"].append({"path": "setup.py", "type": "file", "package_data": [
        {"declared_license_expression_spdx": "", "extracted_license_statement": ["MIT"]}]})
    idx = R.index_scan(s)
    assert "src" not in idx.files
    assert idx.packages == [("setup.py", "", "['MIT']")]
    assert idx.matches[0].brief()["matcher"] == "2-aho"


# --- rules added after the review of the 184 test projects ----------------------------------------

def test_spdx_document_and_reuse_readme_are_not_license_texts():
    assert R.path_role("LICENSE.spdx") == "other"
    assert R.path_role("LICENSES/README.md") == "reuse-meta"
    d = R.decide(scan(f("LICENSE", APACHE), f("LICENSE.spdx", m("CC0-1.0", "cc0-1.0.LICENSE"))))
    assert d.license == "Apache-2.0"


def test_text_file_notices_do_not_add_licenses_and_unversioned_gpl_goes():
    d = R.decide(scan(f("COPYING", m("GPL-3.0-or-later", "gpl-3.0-plus.LICENSE", matcher="1-hash"),
                        m("GPL-1.0-or-later", "gpl_70.RULE", kind="reference", start=30, end=30),
                        m("CC-BY-SA-4.0", "cc-by-sa-4.0_3.RULE", kind="reference", start=31, end=31)),
                      f("README.md", m("GPL-1.0-or-later", "gpl_7.RULE", kind="reference"))))
    assert d.license == "GPL-3.0-or-later"
    d = R.decide(scan(f("LICENSE", m("GPL-2.0-only", "gpl-2.0_3.RULE", kind="notice"),
                        m("GPL-1.0-or-later", "gpl_70.RULE", kind="reference", start=5, end=5),
                        m("GPL-2.0-only WITH Classpath-exception-2.0", "gpl-2.0-classpath_1.RULE", kind="reference"),
                        m("Classpath-exception-2.0", "classpath_4.RULE", kind="reference", start=9, end=9))))
    assert d.license == "GPL-2.0-only WITH Classpath-exception-2.0"


def test_weak_fragments_are_dropped():
    d = R.decide(scan(f("EULA.txt", m("LicenseRef-scancode-proprietary-license", "proprietary_72.RULE",
                                      kind="reference"),
                        m("AMD-newlib", "amd-historical1.RULE", matcher="3-seq", score=10.5, cov=10.5))))
    assert d.license == "LicenseRef-scancode-proprietary-license"
    assert d.evidence_kind == "license-file-notice"


def test_reuse_before_alt_files_and_exceptions_not_paired():
    d = R.decide(scan(f("COPYING.DOC", m("GFDL-1.2-only", "gfdl-1.2.LICENSE")),
                      f("LICENSES/GPL-2.0-or-later.txt", GPL2_TEXT),
                      f("LICENSES/Qt-GPL-exception-1.0.txt", m("Qt-GPL-exception-1.0", "qt-gpl-exception-1.0.LICENSE"))))
    assert d.license == "GPL-2.0-or-later"
    assert "reuse-exception-not-paired:Qt-GPL-exception-1.0" in d.flags


def test_build_scripts_are_not_notices_and_manifest_files_are():
    root = [f("LICENSE.GPL2", GPL2_TEXT), f("LICENSE.LGPL2.1", m("LGPL-2.1-only", "lgpl-2.1.LICENSE"))]
    d = R.decide(scan(*root, f("meson.build", m("GPL-2.0-only", "gpl-2.0_9.RULE", kind="tag"))))
    assert d.license == "GPL-2.0 AND LGPL-2.1"
    d = R.decide(scan(*root, f("README", m("LGPL-2.1-or-later", "lgpl-2.1-plus_5.RULE", kind="tag"))))
    assert d.license == "LGPL-2.1-or-later"
    d = R.decide(scan(f("Cargo.toml", m("GPL-3.0-or-later", "gpl-3.0-plus_9.RULE", kind="reference"))))
    assert (d.license, d.evidence_kind) == ("GPL-3.0-or-later", "manifest")


def test_notice_must_keep_the_primary_license():
    d = R.decide(scan(f("LICENSE.txt", m("GPL-3.0-only", "gpl-3.0.LICENSE")),
                      f("LICENSE_AL.txt", APACHE),
                      f("README.md", m("Apache-2.0", "apache-2.0_7.RULE", kind="reference"))))
    assert d.license == "GPL-3.0"
    assert "Apache-2.0" in d.other_licenses


def test_aggregate_license_file_gives_its_first_text():
    d = R.decide(scan(f("LICENSE", m("GPL-2.0-only", "gpl-2.0.LICENSE", start=10, end=300),
                        m("MIT", "mit.LICENSE", start=400, end=420), m("Zlib", "zlib.LICENSE", start=500, end=520),
                        m("BSD-3-Clause", "bsd-new.LICENSE", start=600, end=630))))
    assert d.license == "GPL-2.0"
    assert d.confidence == "medium"
    assert set(d.other_licenses) >= {"MIT", "Zlib", "BSD-3-Clause"}


def test_title_of_a_close_relative_beats_a_near_text():
    d = R.decide(scan(f("LICENSE", m("BSD-3-Clause-Clear", "bsd-3-clause-clear_4.RULE", kind="reference", start=1, end=1),
                        m("BSD-3-Clause", "bsd-new.LICENSE", matcher="3-seq", score=96, cov=96, start=6, end=30))),
                 templates={"LICENSE": {"BSD-3-Clause": {"match": False}, "BSD-3-Clause-Clear": {"match": False}}})
    assert d.license == "BSD-3-Clause-Clear"
    assert "title-relative:LICENSE:BSD-3-Clause->BSD-3-Clause-Clear" in d.flags


def test_plural_licenses_file_is_secondary_next_to_a_license():
    d = R.decide(scan(f("LICENSE.TXT", MIT), f("LICENSES.TXT", APACHE, m("Zlib", "zlib.LICENSE", start=30, end=50))))
    assert d.license == "MIT"
    d = R.decide(scan(f("LICENSES.txt", APACHE)))
    assert d.license == "Apache-2.0"


def test_primary_notice_file_chooses_among_alt_texts():
    d = R.decide(scan(f("COPYRIGHT", m("Apache-2.0", "apache-2.0_589.RULE", kind="notice", score=91),
                        m("MIT", "mit_27.RULE", kind="reference", start=4, end=4)),
                      f("LICENSE-APACHE", APACHE), f("LICENSE-MIT", MIT)))
    assert d.license == "Apache-2.0 AND MIT"
    assert d.evidence_kind == "text"


def test_low_license_text_share_lowers_confidence():
    s = scan(f("LICENSE", m("MIT", "mit.LICENSE", start=71, end=90)))
    s["files"][0]["percentage_of_license_text"] = 44.7
    d = R.decide(s)
    assert d.license == "MIT"
    assert d.confidence == "medium"
    assert "low-license-text:LICENSE:44.7" in d.flags


def test_long_notice_rule_is_a_text_and_notice_file_chooses_an_or():
    epl = m("EPL-2.0", "epl-2.0_10.RULE", kind="notice", matcher="3-seq", score=99.3, cov=99.3, start=2, end=285)
    s = scan(f("LICENSE", epl, m("BSD-3-Clause", "edl-1.0_2.RULE", start=295, end=318)),
             f("NOTICE.md", m("EPL-2.0 OR BSD-3-Clause", "epl-2.0_or_bsd-new_1.RULE", kind="notice", matcher="3-seq",
                              score=94.6, cov=94.6, start=19, end=22),
               m("EPL-2.0 OR BSD-3-Clause", "spdx-epl-or-bsd", kind="unknown", matcher="1-spdx-id", start=24, end=24)))
    for r in s["license_rule_references"]:
        if r["identifier"] == "epl-2.0_10.RULE":
            r["length"] = 2119
    d = R.decide(s, templates={"LICENSE": {"EPL-2.0": {"within": True}}})
    assert d.license == "EPL-2.0 OR BSD-3-Clause"
    assert d.evidence_kind == "text+notice"
    idx = R.index_scan(s)
    assert [x.kind for x in idx.matches if x.matcher == "1-spdx-id"] == ["tag"]


def test_or_tag_in_the_license_file_and_a_notice_only_copying():
    d = R.decide(scan(f("LICENSE", m("GPL-2.0-only OR BSD-2-Clause", "spdx-gpl-or-bsd", kind="unknown",
                                     matcher="1-spdx-id", start=2, end=2),
                        m("GPL-2.0-only", "gpl-2.0.LICENSE", start=4, end=300))))
    assert d.license == "GPL-2.0-only OR BSD-2-Clause"
    d = R.decide(scan(f("COPYING", m("GPL-2.0-only", "spdx-gpl", kind="unknown", matcher="1-spdx-id", start=3, end=3)),
                      f("LICENSES/MIT", MIT)))
    assert d.license == "GPL-2.0-only"
    assert d.evidence_kind == "license-file-notice"
