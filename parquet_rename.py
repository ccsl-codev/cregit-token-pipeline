#!/usr/bin/env python3
"""Rename or drop top-level columns of a Parquet file without re-encoding a page.

Rename only: the footer is the only part that changes.

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

Drop (with or without renames, in the same pass): the output is "PAR1", then
every region the footer points at for a kept column (column chunk, Bloom
filter, column index, offset index), copied in its original order, then the
new footer.  Chunks, Bloom filters and column indexes are copied byte for
byte; page headers (data page V1 and V2, dictionary page) hold sizes, counts,
encodings and statistics but no file offset, so no page changes.  The offset
indexes hold absolute page offsets; they are decoded, moved and re-encoded.
The new footer drops the SchemaElements, ColumnChunks and column_orders of
the dropped columns, fixes the root num_children, moves every offset
(file_offset, data/index/dictionary page offsets, bloom_filter_offset,
column_index_offset, offset_index_offset and length, RowGroup file_offset),
recomputes the RowGroup total_byte_size and total_compressed_size, remaps
sorting_columns, and drops the fields from ARROW:schema.

It refuses: an encrypted footer ("PARE") or encrypted column metadata, a name
that is not a top-level column, an empty new name, a rename to the same name,
a mapping that gives two columns one name, and (unless --allow-mentions) an
old or dropped name that also appears in another key-value or in Arrow
metadata, which this tool would leave unchanged.  A drop also refuses: every
column, a column both renamed and dropped, a dropped sort column, a column
chunk in another file, and a data part that the footer's regions do not tile
exactly (gaps, overlaps, or bytes no offset reaches), or sizes and offsets
that do not add up the way the format defines them.

Usage:
  parquet_rename.py SRC DST --rename OLD=NEW [--rename OLD=NEW ...]
  parquet_rename.py SRC DST --map mapping.json
  parquet_rename.py SRC DST --drop NAME [--drop NAME ...] [--rename OLD=NEW ...]
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
CMD_TOTAL_UNCOMPRESSED_SIZE, CMD_TOTAL_COMPRESSED_SIZE = 6, 7
CMD_DATA_PAGE_OFFSET, CMD_INDEX_PAGE_OFFSET, CMD_DICTIONARY_PAGE_OFFSET = 9, 10, 11
CMD_BLOOM_FILTER_LENGTH = 15
CC_FILE_PATH, CC_FILE_OFFSET = 1, 2
CC_OFFSET_INDEX_OFFSET, CC_OFFSET_INDEX_LENGTH = 4, 5
CC_COLUMN_INDEX_OFFSET, CC_COLUMN_INDEX_LENGTH = 6, 7
RG_TOTAL_BYTE_SIZE, RG_SORTING_COLUMNS, RG_FILE_OFFSET, RG_TOTAL_COMPRESSED_SIZE = 2, 4, 5, 6
FMD_COLUMN_ORDERS = 7
SC_COLUMN_IDX = 1
OI_PAGE_LOCATIONS, PL_OFFSET, PL_COMPRESSED_PAGE_SIZE, PL_FIRST_ROW_INDEX = 1, 1, 2, 3
BF_NUM_BYTES = 1
PH_TYPE, PH_COMPRESSED_PAGE_SIZE = 1, 3
PAGE_DATA, PAGE_DATA_V2 = 0, 3
CI_NULL_PAGES = 1
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


def schema_spans(schema: list) -> list[tuple[int, int, str, int]]:
    """(first index, end index, name, leaf count) of every top-level column.

    The schema is a depth-first list; element 0 is the root and each group
    element carries num_children.  An element without num_children is a leaf.
    """

    def skip(i: int) -> tuple[int, int]:
        if not has(schema[i], SE_NUM_CHILDREN):
            return i + 1, 1
        j, leaves = i + 1, 0
        for _ in range(get(schema[i], SE_NUM_CHILDREN)):
            j, n = skip(j)
            leaves += n
        return j, leaves

    out = []
    i = 1
    for _ in range(get(schema[0], SE_NUM_CHILDREN) or 0):
        end, leaves = skip(i)
        out.append((i, end, get(schema[i], SE_NAME).decode("utf-8"), leaves))
        i = end
    if i != len(schema):
        raise RenameError(f"the schema tree covers {i} of {len(schema)} elements")
    return out


def top_level(schema: list) -> list[tuple[int, str]]:
    """(index in the flat schema list, name) of every top-level column."""
    return [(i, name) for i, _, name, _ in schema_spans(schema)]


def validate_mapping(mapping: dict[str, str], names: list[str],
                     drops: list[str] | tuple = ()) -> None:
    present = set(names)
    for old, new in mapping.items():
        if old not in present:
            raise RenameError(f"no top-level column {old!r}")
        if not new:
            raise RenameError(f"empty new name for {old!r}")
        if new == old:
            raise RenameError(f"{old!r} is renamed to itself")
    for d in drops:
        if d not in present:
            raise RenameError(f"no top-level column {d!r} to drop")
        if d in mapping:
            raise RenameError(f"{d!r} is both renamed and dropped")
    if len(set(drops)) != len(drops):
        raise RenameError("a column is named twice in the drop list")
    if drops and len(set(drops)) == len(names):
        raise RenameError("the drop list names every column")
    after = [mapping.get(n, n) for n in names if n not in drops]
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


def renamed_arrow_schema(schema, mapping: dict[str, str], drops=()):
    import pyarrow as pa

    fields = [f.with_name(mapping.get(f.name, f.name)) for f in schema if f.name not in drops]
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


def prepare(meta: list, mapping: dict[str, str], drops, allow_mentions: bool) -> dict:
    """Validate a parsed footer and the mapping; find ARROW:schema."""
    refuse_encryption(meta)
    schema = get(meta, FMD_SCHEMA).items
    spans = schema_spans(schema)
    names = [n for _, _, n, _ in spans]
    validate_mapping(mapping, names, drops)

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

    olds = [o.encode("utf-8") for o in list(mapping) + list(drops)]
    found = mentions(meta, arrow_schema, olds)
    if found and not allow_mentions:
        raise RenameError("an old or dropped name also appears in " + "; ".join(found)
                          + " (not changed by this tool; pass --allow-mentions to keep them)")
    return dict(schema=schema, spans=spans, names=names, arrow_kv=arrow_kv,
                arrow_schema=arrow_schema, found=found)


def apply_renames(meta: list, ctx: dict, mapping: dict[str, str], drops=()) -> int:
    """Rename schema names, path_in_schema[0] and ARROW:schema in place."""
    enc = {o.encode("utf-8"): n.encode("utf-8") for o, n in mapping.items()}
    for i, _, name, _ in ctx["spans"]:
        if name in mapping:
            set_value(ctx["schema"][i], SE_NAME, enc[name.encode("utf-8")])
    leaves = 0
    for rg in kv_items(meta, FMD_ROW_GROUPS):
        for cc in get(rg, RG_COLUMNS).items:
            path = get(get(cc, CC_META_DATA), CMD_PATH_IN_SCHEMA).items
            if path[0] in enc:
                path[0] = enc[path[0]]
                leaves += 1
    if ctx["arrow_kv"] is not None and (mapping or drops):
        set_value(ctx["arrow_kv"], KV_VALUE, encode_arrow_schema(
            renamed_arrow_schema(ctx["arrow_schema"], mapping, drops)))
    return leaves


def rename_footer(footer: bytes, mapping: dict[str, str],
                  allow_mentions: bool = False) -> tuple[bytes, dict]:
    """Return the new footer bytes and a short report.

    With an empty mapping the result is the input, after both round trips.
    """
    meta = parse_checked(footer)
    ctx = prepare(meta, mapping, (), allow_mentions)
    leaves = apply_renames(meta, ctx, mapping)
    new = encode(meta)
    return new, dict(renamed=dict(mapping), column_chunks_renamed=leaves,
                     arrow_schema=ctx["arrow_kv"] is not None, mentions_kept=ctx["found"])


# ---------------------------------------------------------- column drops --
class Region:
    """A byte range of the data part that the footer points at."""

    __slots__ = ("kind", "off", "length", "rg", "col", "new_off", "data")

    def __init__(self, kind, off, length, rg, col):
        self.kind, self.off, self.length, self.rg, self.col = kind, off, length, rg, col
        self.new_off = None
        self.data = None  # re-encoded bytes (offset indexes only)


def chunk_span(cmd: list) -> tuple[int, int]:
    """(start, length) of a column chunk: its first page to the end of its pages."""
    offs = [get(cmd, f) for f in (CMD_DICTIONARY_PAGE_OFFSET, CMD_INDEX_PAGE_OFFSET,
                                  CMD_DATA_PAGE_OFFSET) if has(cmd, f)]
    offs = [o for o in offs if o >= len(MAGIC)]
    if not offs:
        raise RenameError("a column chunk has no page offset")
    return min(offs), get(cmd, CMD_TOTAL_COMPRESSED_SIZE)


def struct_at(fd: int, off: int, limit: int = 4096) -> tuple[list, int]:
    """Parse one Thrift struct at a file offset; return it and its length."""
    r = Reader(os.pread(fd, limit, off))
    try:
        return r.struct(), r.p
    except ValueError as e:
        raise RenameError(f"no Thrift struct at offset {off}: {e}") from e


def bloom_length(fd: int, off: int) -> int:
    """Length of a Bloom filter (header + bitset) that has no bloom_filter_length."""
    header, n = struct_at(fd, off)
    return n + get(header, BF_NUM_BYTES)


def collect_regions(meta: list, fd: int, footer_start: int) -> list[Region]:
    """Every region the footer points at, in file order.

    Refuses a file whose regions overlap or leave bytes of the data part that
    no footer offset reaches: such bytes could not be carried over.
    """
    regions = []
    for r, rg in enumerate(kv_items(meta, FMD_ROW_GROUPS)):
        for c, cc in enumerate(get(rg, RG_COLUMNS).items):
            if has(cc, CC_FILE_PATH):
                raise RenameError("a column chunk lives in another file (file_path); refused")
            cmd = get(cc, CC_META_DATA)
            start, length = chunk_span(cmd)
            regions.append(Region("chunk", start, length, r, c))
            if has(cmd, CMD_BLOOM_FILTER_OFFSET):
                off = get(cmd, CMD_BLOOM_FILTER_OFFSET)
                n = (get(cmd, CMD_BLOOM_FILTER_LENGTH) if has(cmd, CMD_BLOOM_FILTER_LENGTH)
                     else bloom_length(fd, off))
                regions.append(Region("bloom", off, n, r, c))
            for kind, fo, fl in (("column_index", CC_COLUMN_INDEX_OFFSET, CC_COLUMN_INDEX_LENGTH),
                                 ("offset_index", CC_OFFSET_INDEX_OFFSET, CC_OFFSET_INDEX_LENGTH)):
                if has(cc, fo):
                    if not has(cc, fl):
                        raise RenameError(f"a {kind} has no length; refused")
                    regions.append(Region(kind, get(cc, fo), get(cc, fl), r, c))
    regions.sort(key=lambda x: x.off)
    pos = len(MAGIC)
    for reg in regions:
        if reg.off != pos:
            raise RenameError(f"the data part is not tiled by the footer's regions at byte "
                              f"{pos} (next {reg.kind} at {reg.off}); refused")
        pos = reg.off + reg.length
    if pos != footer_start:
        raise RenameError(f"bytes [{pos}, {footer_start}) are not reached by the footer; refused")
    return regions


def parse_offset_index(raw: bytes) -> list:
    try:
        oi = parse_footer(raw)
    except ValueError as e:
        raise RenameError(f"an offset index does not decode: {e}") from e
    if encode(oi) != raw:
        raise RenameError("an offset index does not encode back to the same bytes; refused")
    return oi


def shift(struct_fields: list, fid: int, lo: int, hi: int, delta: int, what: str) -> None:
    """Move an absolute offset that points into [lo, hi] by delta; keep a 0."""
    for f in struct_fields:
        if f.fid == fid:
            if f.value == 0:
                return
            if not lo <= f.value <= hi:
                raise RenameError(f"{what} {f.value} is outside its column chunk; refused")
            f.value += delta
            return


def canonical_list(lst: TList, items: list) -> None:
    lst.items = items
    lst.short = len(items) < 15


def leaf_plan(spans, drops) -> tuple[int, set, dict]:
    """(leaf count, dropped leaf indexes, old -> new index of the kept leaves)."""
    dropped, leaf = set(), 0
    for _, _, name, n in spans:
        if name in drops:
            dropped.update(range(leaf, leaf + n))
        leaf += n
    kept = [c for c in range(leaf) if c not in dropped]
    return leaf, dropped, {c: i for i, c in enumerate(kept)}


def layout(kept: list[Region], chunks: dict, oi_raw: dict) -> int:
    """Give every kept region its new offset; re-encode the offset indexes.

    An offset index can change length when its page offsets change, which
    moves the regions after it, so this repeats until the lengths settle.
    Returns the end of the data part.
    """
    lengths = {id(reg): reg.length for reg in kept}
    for _ in range(16):
        pos = len(MAGIC)
        for reg in kept:
            reg.new_off = pos
            pos += lengths[id(reg)]
        changed = False
        for reg in kept:
            if reg.kind != "offset_index":
                continue
            ch = chunks[(reg.rg, reg.col)]
            oi = parse_offset_index(oi_raw[id(reg)])
            for loc in kv_items(oi, OI_PAGE_LOCATIONS):
                shift(loc, PL_OFFSET, ch.off, ch.off + ch.length - 1,
                      ch.new_off - ch.off, "a page location")
            reg.data = encode(oi)
            if len(reg.data) != lengths[id(reg)]:
                lengths[id(reg)] = len(reg.data)
                changed = True
        if not changed:
            return pos
    raise RenameError("the offset index layout does not settle")  # pragma: no cover


def check_total(rg: list, fid: int, cmds: list, size_fid: int, dropped: set, what: str) -> None:
    if not has(rg, fid):
        return
    if get(rg, fid) != sum(get(m, size_fid) for m in cmds):
        raise RenameError(f"a row group {what} is not the sum of its columns; refused")
    set_value(rg, fid, sum(get(m, size_fid) for c, m in enumerate(cmds) if c not in dropped))


def drop_footer(meta: list, ctx: dict, mapping, drops, regions: list[Region],
                nleaves: int, dropped: set, new_index: dict) -> int:
    """Edit the parsed footer for the drop: offsets, sizes, schema, orders."""
    chunks = {(reg.rg, reg.col): reg for reg in regions if reg.kind == "chunk"}
    by_key = {(reg.kind, reg.rg, reg.col): reg for reg in regions if reg.col not in dropped}
    for r, rg in enumerate(kv_items(meta, FMD_ROW_GROUPS)):
        cols = get(rg, RG_COLUMNS)
        if len(cols.items) != nleaves:
            raise RenameError("a row group does not have one column chunk per leaf")
        for sc in kv_items(rg, RG_SORTING_COLUMNS):
            idx = get(sc, SC_COLUMN_IDX)
            if idx in dropped:
                raise RenameError(f"leaf column {idx} is a sort column and would be dropped")
            set_value(sc, SC_COLUMN_IDX, new_index[idx])
        cmds = [get(cc, CC_META_DATA) for cc in cols.items]
        check_total(rg, RG_TOTAL_BYTE_SIZE, cmds, CMD_TOTAL_UNCOMPRESSED_SIZE, dropped,
                    "total_byte_size")
        check_total(rg, RG_TOTAL_COMPRESSED_SIZE, cmds, CMD_TOTAL_COMPRESSED_SIZE, dropped,
                    "total_compressed_size")
        if has(rg, RG_FILE_OFFSET):
            if get(rg, RG_FILE_OFFSET) != min(chunks[(r, c)].off for c in range(nleaves)):
                raise RenameError("a row group file_offset is not its first chunk; refused")
            set_value(rg, RG_FILE_OFFSET, min(chunks[(r, c)].new_off for c in new_index))
        new_cols = []
        for c, cc in enumerate(cols.items):
            if c in dropped:
                continue
            ch = chunks[(r, c)]
            lo, hi, d = ch.off, ch.off + ch.length, ch.new_off - ch.off
            cmd = cmds[c]
            for fid in (CMD_DATA_PAGE_OFFSET, CMD_INDEX_PAGE_OFFSET, CMD_DICTIONARY_PAGE_OFFSET):
                shift(cmd, fid, lo, hi - 1, d, "a page offset")
            shift(cc, CC_FILE_OFFSET, lo, hi, d, "a column chunk file_offset")
            if ("bloom", r, c) in by_key:
                set_value(cmd, CMD_BLOOM_FILTER_OFFSET, by_key[("bloom", r, c)].new_off)
            if ("column_index", r, c) in by_key:
                set_value(cc, CC_COLUMN_INDEX_OFFSET, by_key[("column_index", r, c)].new_off)
            if ("offset_index", r, c) in by_key:
                oi = by_key[("offset_index", r, c)]
                set_value(cc, CC_OFFSET_INDEX_OFFSET, oi.new_off)
                set_value(cc, CC_OFFSET_INDEX_LENGTH, len(oi.data))
            new_cols.append(cc)
        canonical_list(cols, new_cols)

    orders = get(meta, FMD_COLUMN_ORDERS)
    if orders is not None:
        if len(orders.items) != nleaves:
            raise RenameError("column_orders does not have one entry per leaf")
        canonical_list(orders, [o for c, o in enumerate(orders.items) if c not in dropped])

    renamed = apply_renames(meta, ctx, mapping, drops)
    schema, spans = ctx["schema"], ctx["spans"]
    kept_elems = [schema[0]]
    for i, end, name, _ in spans:
        if name not in drops:
            kept_elems += schema[i:end]
    set_value(schema[0], SE_NUM_CHILDREN, len(spans) - len(drops))
    canonical_list(get(meta, FMD_SCHEMA), kept_elems)
    return renamed


def rewrite_with_drops(src: str, dst: str, mapping: dict[str, str], drops: list[str], *,
                       copy: str = "auto", allow_mentions: bool = False,
                       fsync: bool = True) -> dict:
    """Write src without the dropped columns, and with the renames, to dst.

    The kept column chunks, Bloom filters and column indexes are copied byte
    for byte, in their original order; offset indexes are re-encoded with
    their page offsets moved.  Page headers (data page V1 and V2, dictionary
    page) hold sizes, counts, encodings and statistics but no file offset, so
    no page changes.
    """
    size, start, footer = read_footer(src)
    meta = parse_checked(footer)
    ctx = prepare(meta, mapping, drops, allow_mentions)
    nleaves, dropped, new_index = leaf_plan(ctx["spans"], drops)

    sfd = os.open(src, os.O_RDONLY)
    try:
        regions = collect_regions(meta, sfd, start)
        kept = [reg for reg in regions if reg.col not in dropped]
        chunks = {(reg.rg, reg.col): reg for reg in regions if reg.kind == "chunk"}
        oi_raw = {id(reg): os.pread(sfd, reg.length, reg.off)
                  for reg in kept if reg.kind == "offset_index"}
        data_end = layout(kept, chunks, oi_raw)
        renamed = drop_footer(meta, ctx, mapping, drops, regions, nleaves, dropped, new_index)
        new = encode(meta)
        tail = new + struct.pack("<I", len(new)) + MAGIC

        tmp = dst + ".tmp"
        dfd = os.open(tmp, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            os.pwrite(dfd, MAGIC, 0)
            method = "copy_file_range" if copy == "auto" else "read-write"
            copied = 0
            for reg in kept:
                if reg.data is not None:
                    os.pwrite(dfd, reg.data, reg.new_off)
                else:
                    method = copy_range(sfd, dfd, reg.off, reg.new_off, reg.length, method)
                    copied += reg.length
            os.pwrite(dfd, tail, data_end)
            os.ftruncate(dfd, data_end + len(tail))
            if fsync:
                os.fsync(dfd)
        except BaseException:
            os.close(dfd)
            os.unlink(tmp)
            raise
        os.close(dfd)
        os.replace(tmp, dst)
    finally:
        os.close(sfd)
    kinds: dict[str, int] = {}
    for reg in kept:
        kinds[reg.kind] = kinds.get(reg.kind, 0) + 1
    return dict(renamed=dict(mapping), dropped=list(drops), column_chunks_renamed=renamed,
                arrow_schema=ctx["arrow_kv"] is not None, mentions_kept=ctx["found"],
                src=src, dst=dst, in_size=size, in_footer_len=len(footer),
                out_footer_len=len(new), out_size=data_end + len(tail),
                columns_kept=len(ctx["spans"]) - len(drops), leaves_dropped=len(dropped),
                regions_kept=kinds, bytes_copied=copied, copy=method)


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


def copy_range(src_fd: int, dst_fd: int, src_off: int, dst_off: int, length: int,
               method: str) -> str:
    """Copy length bytes between offsets; returns the method that worked."""
    if method == "copy_file_range":
        done = 0
        try:
            while done < length:
                n = os.copy_file_range(src_fd, dst_fd, length - done,
                                       src_off + done, dst_off + done)
                if n <= 0:
                    raise OSError("copy_file_range returned 0")
                done += n
            return method
        except OSError:  # pragma: no cover - file systems without copy_file_range
            method = "read-write"
    done = 0
    while done < length:
        buf = os.pread(src_fd, min(CHUNK, length - done), src_off + done)
        if not buf:
            raise OSError("short read of a column chunk")
        os.pwrite(dst_fd, buf, dst_off + done)
        done += len(buf)
    return method


def rename_file(src: str, dst: str, mapping: dict[str, str], *, drops=(),
                copy: str = "auto", allow_mentions: bool = False, tail_only: bool = False,
                fsync: bool = True) -> dict:
    """Write the renamed file (or only its new tail) to dst.

    With drops, the kept column chunks are copied to new offsets instead
    (rewrite_with_drops); their pages still keep their bytes.
    """
    if os.path.exists(dst) and os.path.samefile(src, dst):
        raise RenameError("the output is the input file; refused")
    if drops:
        if tail_only:
            raise RenameError("--tail-only cannot drop columns: the data part changes")
        return rewrite_with_drops(src, dst, mapping, list(drops), copy=copy,
                                  allow_mentions=allow_mentions, fsync=fsync)
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
    ap.add_argument("--drop", action="append", default=[], metavar="NAME",
                    help="drop top-level column NAME (repeatable)")


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
        rep = rename_file(a.src, a.dst, mapping, drops=a.drop, copy=a.copy,
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
