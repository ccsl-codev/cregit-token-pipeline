#!/usr/bin/env python3
"""Rename top-level columns of a Parquet file by rewriting only its footer.

The output is the input bytes [0, footer_start) unchanged, then a new Thrift
FileMetaData, then the 4-byte footer length and "PAR1".  The "PAR1" magic, the
column chunks, pages, dictionaries, Bloom filters and page indexes keep their
bytes and their offsets, so every offset in the footer stays valid.

The new footer differs from the old one in three places only:

  * the SchemaElement.name of each renamed top-level column,
  * path_in_schema[0] in the ColumnMetaData of every leaf under it,
  * the "ARROW:schema" key-value (base64 of the IPC-serialized Arrow schema),
    where the top-level field is renamed and all metadata is kept.

The Thrift compact protocol codec below keeps every field, its type and the
form of its header, so it encodes a parsed footer back to the same bytes.  The
tool checks that round trip on every input, and the same for the Arrow schema
(pyarrow must serialize the decoded schema back to the stored bytes), before
it changes anything.

It refuses: an encrypted footer ("PARE") or encrypted column metadata, a name
that is not a top-level column, an empty new name, a rename to the same name,
a mapping that gives two columns one name, and (unless --allow-mentions) an
old name that also appears in another key-value or in Arrow metadata, which
this tool would leave unchanged.

Usage:
  parquet_rename.py SRC DST --rename OLD=NEW [--rename OLD=NEW ...]
  parquet_rename.py SRC DST --map mapping.json
  parquet_rename.py SRC DST --tail-only ...   write only the new footer tail

With no mapping the output is byte-identical to the input.  One JSON line on
stdout describes the result.  pyarrow is needed only when the file has an
"ARROW:schema" key-value.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import fcntl
import json
import os
import struct
import sys

MAGIC = b"PAR1"
MAGIC_ENCRYPTED = b"PARE"
ARROW_SCHEMA_KEY = b"ARROW:schema"
FICLONE = 0x40049409
CHUNK = 64 << 20

# Field ids from parquet.thrift.
FMD_SCHEMA, FMD_ROW_GROUPS, FMD_KV = 2, 4, 5
FMD_ENCRYPTION_ALGORITHM, FMD_FOOTER_SIGNING_KEY = 8, 9
SE_NAME, SE_NUM_CHILDREN = 4, 5
RG_COLUMNS = 1
CC_META_DATA, CC_CRYPTO_METADATA, CC_ENCRYPTED_COLUMN_METADATA = 3, 8, 9
CMD_PATH_IN_SCHEMA, CMD_KV, CMD_BLOOM_FILTER_OFFSET = 3, 8, 14
KV_KEY, KV_VALUE = 1, 2


class RenameError(Exception):
    """The file or the mapping is one this tool refuses."""


# ---------------------------------------------------------------- Thrift --
# Compact protocol type ids.
T_STOP, T_TRUE, T_FALSE, T_I8, T_I16, T_I32, T_I64, T_DOUBLE = 0, 1, 2, 3, 4, 5, 6, 7
T_BINARY, T_LIST, T_SET, T_MAP, T_STRUCT, T_UUID = 8, 9, 10, 11, 12, 13


class Field:
    """One struct field.  `short` is True when the header used the delta form."""

    __slots__ = ("fid", "ttype", "value", "short")

    def __init__(self, fid, ttype, value, short):
        self.fid, self.ttype, self.value, self.short = fid, ttype, value, short


class TList:
    __slots__ = ("ttype", "etype", "items", "short")

    def __init__(self, ttype, etype, items, short):
        self.ttype, self.etype, self.items, self.short = ttype, etype, items, short


class TMap:
    __slots__ = ("ktype", "vtype", "items")

    def __init__(self, ktype, vtype, items):
        self.ktype, self.vtype, self.items = ktype, vtype, items


class Reader:
    def __init__(self, buf: bytes):
        self.b = buf
        self.p = 0

    def byte(self) -> int:
        if self.p >= len(self.b):
            raise ValueError("footer ends inside a value")
        v = self.b[self.p]
        self.p += 1
        return v

    def varint(self) -> int:
        shift = res = 0
        while True:
            c = self.byte()
            res |= (c & 0x7F) << shift
            if not c & 0x80:
                return res
            shift += 7

    def zigzag(self) -> int:
        n = self.varint()
        return (n >> 1) ^ -(n & 1)

    def raw(self, n: int) -> bytes:
        v = self.b[self.p:self.p + n]
        if len(v) != n:
            raise ValueError("value runs past the footer")
        self.p += n
        return v

    def value(self, t: int, in_container: bool = False):
        if t in (T_TRUE, T_FALSE):
            # In a struct field the value is the type nibble; in a container
            # it is one byte.  Keep the raw byte.
            return self.byte() if in_container else None
        if t == T_I8:
            return self.byte()
        if t in (T_I16, T_I32, T_I64):
            return self.zigzag()
        if t == T_DOUBLE:
            return self.raw(8)
        if t == T_UUID:
            return self.raw(16)
        if t == T_BINARY:
            return self.raw(self.varint())
        if t in (T_LIST, T_SET):
            h = self.byte()
            etype, size = h & 0x0F, h >> 4
            short = size != 15
            if not short:
                size = self.varint()
            return TList(t, etype, [self.value(etype, True) for _ in range(size)], short)
        if t == T_MAP:
            size = self.varint()
            if size == 0:
                return TMap(None, None, [])
            kv = self.byte()
            kt, vt = kv >> 4, kv & 0x0F
            return TMap(kt, vt, [(self.value(kt, True), self.value(vt, True))
                                 for _ in range(size)])
        if t == T_STRUCT:
            return self.struct()
        raise ValueError(f"unknown compact type {t} at byte {self.p}")

    def struct(self) -> list:
        fields = []
        last = 0
        while True:
            h = self.byte()
            if h == T_STOP:
                return fields
            t, delta = h & 0x0F, h >> 4
            if delta:
                fid, short = last + delta, True
            else:
                fid, short = self.zigzag(), False
            val = None if t in (T_TRUE, T_FALSE) else self.value(t)
            fields.append(Field(fid, t, val, short))
            last = fid


class Writer:
    def __init__(self):
        self.out = bytearray()

    def varint(self, n: int):
        while True:
            if n < 0x80:
                self.out.append(n)
                return
            self.out.append((n & 0x7F) | 0x80)
            n >>= 7

    def zigzag(self, v: int, bits: int):
        self.varint(((v << 1) ^ (v >> (bits - 1))) & ((1 << bits) - 1))

    def value(self, t: int, v, in_container: bool = False):
        if t in (T_TRUE, T_FALSE):
            if in_container:
                self.out.append(v)
        elif t == T_I8:
            self.out.append(v)
        elif t == T_I16:
            self.zigzag(v, 16)
        elif t == T_I32:
            self.zigzag(v, 32)
        elif t == T_I64:
            self.zigzag(v, 64)
        elif t in (T_DOUBLE, T_UUID):
            self.out += v
        elif t == T_BINARY:
            self.varint(len(v))
            self.out += v
        elif t in (T_LIST, T_SET):
            n = len(v.items)
            if v.short:
                if n >= 15:
                    raise ValueError("short list header with 15 or more items")
                self.out.append((n << 4) | v.etype)
            else:
                self.out.append(0xF0 | v.etype)
                self.varint(n)
            for it in v.items:
                self.value(v.etype, it, True)
        elif t == T_MAP:
            self.varint(len(v.items))
            if v.items:
                self.out.append((v.ktype << 4) | v.vtype)
                for k, x in v.items:
                    self.value(v.ktype, k, True)
                    self.value(v.vtype, x, True)
        elif t == T_STRUCT:
            self.struct(v)
        else:
            raise ValueError(f"unknown compact type {t}")

    def struct(self, fields: list):
        last = 0
        for f in fields:
            delta = f.fid - last
            if f.short:
                if not 0 < delta <= 15:
                    raise ValueError("short field header with a bad delta")
                self.out.append((delta << 4) | f.ttype)
            else:
                self.out.append(f.ttype)
                self.zigzag(f.fid, 16)
            if f.ttype not in (T_TRUE, T_FALSE):
                self.value(f.ttype, f.value)
            last = f.fid
        self.out.append(T_STOP)


def parse_footer(footer: bytes) -> list:
    r = Reader(footer)
    meta = r.struct()
    if r.p != len(footer):
        raise ValueError(f"footer has {len(footer) - r.p} bytes after FileMetaData")
    return meta


def encode(meta: list) -> bytes:
    w = Writer()
    w.struct(meta)
    return bytes(w.out)


def get(struct_fields: list, fid: int):
    """The value of field `fid` in a parsed struct, or None."""
    for f in struct_fields:
        if f.fid == fid:
            return f.value
    return None


def has(struct_fields: list, fid: int) -> bool:
    return any(f.fid == fid for f in struct_fields)


def set_value(struct_fields: list, fid: int, value) -> None:
    for f in struct_fields:
        if f.fid == fid:
            f.value = value
            return
    raise KeyError(fid)


# --------------------------------------------------------------- Parquet --
def read_footer(path: str) -> tuple[int, int, bytes]:
    """Return (file size, footer start, footer bytes) of a plaintext footer."""
    with open(path, "rb") as fh:
        size = os.fstat(fh.fileno()).st_size
        if size < 12:
            raise RenameError(f"{path}: {size} bytes is too small for a Parquet file")
        head = fh.read(4)
        fh.seek(size - 8)
        tail = fh.read(8)
        flen = struct.unpack("<I", tail[:4])[0]
        if tail[4:] == MAGIC_ENCRYPTED:
            raise RenameError(f"{path}: the footer is encrypted (PARE); refused")
        if head != MAGIC or tail[4:] != MAGIC:
            raise RenameError(f"{path}: no PAR1 magic; not a Parquet file")
        start = size - 8 - flen
        if flen == 0 or start < 4:
            raise RenameError(f"{path}: footer length {flen} does not fit the file")
        fh.seek(start)
        footer = fh.read(flen)
    return size, start, footer


def parse_checked(footer: bytes) -> list:
    """Parse a footer and prove the codec gives the same bytes back."""
    try:
        meta = parse_footer(footer)
    except ValueError as e:
        raise RenameError(f"the footer does not decode: {e}") from e
    if encode(meta) != footer:
        raise RenameError("the footer does not encode back to the same bytes; refused")
    return meta


def refuse_encryption(meta: list) -> None:
    if has(meta, FMD_ENCRYPTION_ALGORITHM) or has(meta, FMD_FOOTER_SIGNING_KEY):
        raise RenameError("the file has encryption metadata; refused")
    for rg in (get(meta, FMD_ROW_GROUPS) or TList(T_LIST, T_STRUCT, [], True)).items:
        for cc in get(rg, RG_COLUMNS).items:
            if has(cc, CC_CRYPTO_METADATA) or has(cc, CC_ENCRYPTED_COLUMN_METADATA):
                raise RenameError("a column chunk is encrypted; refused")


def top_level(schema: list) -> list[tuple[int, str]]:
    """(index in the flat schema list, name) of every top-level column.

    The schema is a depth-first list; element 0 is the root and each group
    element carries num_children.
    """

    def skip(i: int) -> int:
        n = get(schema[i], SE_NUM_CHILDREN) or 0
        i += 1
        for _ in range(n):
            i = skip(i)
        return i

    out = []
    i = 1
    for _ in range(get(schema[0], SE_NUM_CHILDREN) or 0):
        out.append((i, get(schema[i], SE_NAME).decode("utf-8")))
        i = skip(i)
    if i != len(schema):
        raise RenameError(f"the schema tree covers {i} of {len(schema)} elements")
    return out


def validate_mapping(mapping: dict[str, str], names: list[str]) -> None:
    present = set(names)
    for old, new in mapping.items():
        if old not in present:
            raise RenameError(f"no top-level column {old!r}")
        if not new:
            raise RenameError(f"empty new name for {old!r}")
        if new == old:
            raise RenameError(f"{old!r} is renamed to itself")
    after = [mapping.get(n, n) for n in names]
    dup = sorted({n for n in after if after.count(n) > 1})
    if dup:
        raise RenameError(f"the mapping makes duplicate column names: {dup}")


def kv_items(struct_fields: list, fid: int) -> list[list]:
    v = get(struct_fields, fid)
    return v.items if v is not None else []


def arrow_schema_of(value: bytes):
    """Decode an ARROW:schema value and prove pyarrow re-encodes it exactly."""
    import pyarrow as pa  # needed only for files that carry ARROW:schema

    try:
        raw = base64.b64decode(value, validate=True)
    except binascii.Error as e:
        raise RenameError(f"ARROW:schema is not valid base64: {e}") from e
    if base64.b64encode(raw) != value:
        raise RenameError("ARROW:schema is not canonical base64; refused")
    schema = pa.ipc.read_schema(pa.py_buffer(raw))
    if schema.serialize().to_pybytes() != raw:
        raise RenameError("pyarrow does not re-encode ARROW:schema to the same bytes; refused")
    return schema


def renamed_arrow_schema(schema, mapping: dict[str, str]):
    import pyarrow as pa

    fields = [f.with_name(mapping.get(f.name, f.name)) for f in schema]
    return pa.schema(fields, metadata=schema.metadata)


def encode_arrow_schema(schema) -> bytes:
    return base64.b64encode(schema.serialize().to_pybytes())


def arrow_metadata_mentions(schema, olds: list[bytes]) -> list[str]:
    out = []

    def scan(where, md):
        for k, v in (md or {}).items():
            for o in olds:
                if o in k or o in v:
                    out.append(f"{where} metadata {k.decode('utf-8', 'replace')!r}")

    scan("ARROW:schema schema", schema.metadata)

    def walk(field, prefix):
        scan(f"ARROW:schema field {prefix}{field.name!r}", field.metadata)
        t = field.type
        for i in range(getattr(t, "num_fields", 0)):
            walk(t.field(i), f"{prefix}{field.name}.")

    for f in schema:
        walk(f, "")
    return out


def mentions(meta: list, arrow_schema, olds: list[bytes]) -> list[str]:
    """Places outside the renamed ones where an old name appears."""
    out = []
    for kv in kv_items(meta, FMD_KV):
        key, val = get(kv, KV_KEY), get(kv, KV_VALUE) or b""
        if key == ARROW_SCHEMA_KEY:
            continue
        if any(o in key or o in val for o in olds):
            out.append(f"key-value {key.decode('utf-8', 'replace')!r}")
    for rg in kv_items(meta, FMD_ROW_GROUPS):
        for cc in get(rg, RG_COLUMNS).items:
            cmd = get(cc, CC_META_DATA)
            for kv in kv_items(cmd, CMD_KV) if cmd else []:
                key, val = get(kv, KV_KEY), get(kv, KV_VALUE) or b""
                if any(o in key or o in val for o in olds):
                    out.append(f"column key-value {key.decode('utf-8', 'replace')!r}")
    if arrow_schema is not None:
        out += arrow_metadata_mentions(arrow_schema, olds)
    return sorted(set(out))


def rename_footer(footer: bytes, mapping: dict[str, str],
                  allow_mentions: bool = False) -> tuple[bytes, dict]:
    """Return the new footer bytes and a short report.

    With an empty mapping the result is the input, after both round trips.
    """
    meta = parse_checked(footer)
    refuse_encryption(meta)
    schema = get(meta, FMD_SCHEMA).items
    tops = top_level(schema)
    names = [n for _, n in tops]
    validate_mapping(mapping, names)

    arrow_kv = None
    arrow_schema = None
    for kv in kv_items(meta, FMD_KV):
        if get(kv, KV_KEY) == ARROW_SCHEMA_KEY:
            if arrow_kv is not None:
                raise RenameError("two ARROW:schema key-values; refused")
            arrow_kv = kv
    if arrow_kv is not None:
        arrow_schema = arrow_schema_of(get(arrow_kv, KV_VALUE))
        if arrow_schema.names != names:
            raise RenameError("ARROW:schema field names differ from the Parquet schema")

    olds = [o.encode("utf-8") for o in mapping]
    found = mentions(meta, arrow_schema, olds)
    if found and not allow_mentions:
        raise RenameError("an old name also appears in " + "; ".join(found)
                          + " (not renamed by this tool; pass --allow-mentions to keep them)")

    enc = {o.encode("utf-8"): n.encode("utf-8") for o, n in mapping.items()}
    for i, name in tops:
        if name in mapping:
            set_value(schema[i], SE_NAME, enc[name.encode("utf-8")])
    leaves = 0
    for rg in kv_items(meta, FMD_ROW_GROUPS):
        for cc in get(rg, RG_COLUMNS).items:
            path = get(get(cc, CC_META_DATA), CMD_PATH_IN_SCHEMA).items
            if path[0] in enc:
                path[0] = enc[path[0]]
                leaves += 1
    if arrow_kv is not None and mapping:
        set_value(arrow_kv, KV_VALUE,
                  encode_arrow_schema(renamed_arrow_schema(arrow_schema, mapping)))
    new = encode(meta)
    return new, dict(renamed=dict(mapping), column_chunks_renamed=leaves,
                     arrow_schema=arrow_kv is not None, mentions_kept=found)


# ------------------------------------------------------------------ files --
def copy_prefix(src_fd: int, dst_fd: int, length: int, mode: str = "auto") -> str:
    """Copy bytes [0, length) of src to dst.  Returns the method used."""
    if mode == "auto":
        try:
            fcntl.ioctl(dst_fd, FICLONE, src_fd)
            os.ftruncate(dst_fd, length)
            return "reflink"
        except OSError:
            os.ftruncate(dst_fd, 0)
        off = 0
        try:
            while off < length:
                n = os.copy_file_range(src_fd, dst_fd, min(CHUNK, length - off), off, off)
                if n <= 0:
                    raise OSError("copy_file_range returned 0")
                off += n
            return "copy_file_range"
        except OSError:  # pragma: no cover - file systems without copy_file_range
            os.ftruncate(dst_fd, 0)
    off = 0
    while off < length:
        buf = os.pread(src_fd, min(CHUNK, length - off), off)
        if not buf:
            raise OSError("short read of the data prefix")
        os.pwrite(dst_fd, buf, off)
        off += len(buf)
    return "read-write"


def rename_file(src: str, dst: str, mapping: dict[str, str], *, copy: str = "auto",
                allow_mentions: bool = False, tail_only: bool = False,
                fsync: bool = True) -> dict:
    """Write the renamed file (or only its new tail) to dst."""
    if os.path.exists(dst) and os.path.samefile(src, dst):
        raise RenameError("the output is the input file; refused")
    size, start, footer = read_footer(src)
    new, report = rename_footer(footer, mapping, allow_mentions)
    tail = new + struct.pack("<I", len(new)) + MAGIC
    tmp = dst + ".tmp"
    dfd = os.open(tmp, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        if tail_only:
            method = "tail-only"
            os.write(dfd, tail)
        else:
            sfd = os.open(src, os.O_RDONLY)
            try:
                method = copy_prefix(sfd, dfd, start, copy)
            finally:
                os.close(sfd)
            os.pwrite(dfd, tail, start)
            os.ftruncate(dfd, start + len(tail))
        if fsync:
            os.fsync(dfd)
    except BaseException:
        os.close(dfd)
        os.unlink(tmp)
        raise
    os.close(dfd)
    os.replace(tmp, dst)
    report.update(src=src, dst=dst, in_size=size, prefix_len=start,
                  in_footer_len=len(footer), out_footer_len=len(new),
                  out_size=start + len(tail), copy=method)
    return report


def parse_mapping(pairs: list[str], map_file: str | None) -> dict[str, str]:
    mapping: dict[str, str] = {}
    if map_file:
        with open(map_file, encoding="utf-8") as fh:
            loaded = json.load(fh)
        if not isinstance(loaded, dict) or not all(
                isinstance(k, str) and isinstance(v, str) for k, v in loaded.items()):
            raise RenameError(f"{map_file}: expected a JSON object of old -> new names")
        mapping.update(loaded)
    for p in pairs:
        old, sep, new = p.partition("=")
        if not sep:
            raise RenameError(f"--rename {p!r}: expected OLD=NEW")
        if old in mapping:
            raise RenameError(f"{old!r} is named twice in the mapping")
        mapping[old] = new
    return mapping


def mapping_args(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--rename", action="append", default=[], metavar="OLD=NEW",
                    help="rename top-level column OLD to NEW (repeatable)")
    ap.add_argument("--map", metavar="JSON", help="a JSON object of old -> new names")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("src")
    ap.add_argument("dst")
    mapping_args(ap)
    ap.add_argument("--copy", choices=("auto", "read-write"), default="auto",
                    help="auto: reflink, then copy_file_range, then read-write")
    ap.add_argument("--allow-mentions", action="store_true",
                    help="keep other key-values that mention an old name unchanged")
    ap.add_argument("--tail-only", action="store_true",
                    help="write only the new footer, its length and PAR1 to DST; the "
                         "full file is SRC[0:prefix_len] followed by DST")
    ap.add_argument("--no-fsync", action="store_true")
    a = ap.parse_args(argv)
    try:
        mapping = parse_mapping(a.rename, a.map)
        rep = rename_file(a.src, a.dst, mapping, copy=a.copy,
                          allow_mentions=a.allow_mentions, tail_only=a.tail_only,
                          fsync=not a.no_fsync)
    except RenameError as e:
        print(json.dumps(dict(src=a.src, dst=a.dst, ok=False, error=str(e))))
        return 2
    rep["ok"] = True
    print(json.dumps(rep))
    return 0


if __name__ == "__main__":
    sys.exit(main())
