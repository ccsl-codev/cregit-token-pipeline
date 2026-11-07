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

Replace values (with or without drops and renames, in the same pass): --set
NAME=VALUE gives a top-level string column one value in every row, and
--set-null NAME makes it null.  The tool re-encodes only that column's chunk
in each row group, with pyarrow, and keeps the physical type, the logical
type, the codec, the encoding family (dictionary, PLAIN or DELTA_*), the data
page version, page checksums and the rows per page.  It writes a new
statistics struct with the fields the old one had, a new column index and
offset index if the old chunk had them, and a new Bloom filter if the old
chunk had one.  Every other region is copied byte for byte as for a drop.
The schema and ARROW:schema do not change, because the type does not change.
A replacement refuses: a nested or repeated column, a column that is not a
string (BYTE_ARRAY with STRING or UTF8), a null for a required column, a
column that is also dropped, a column with more than one distinct value
(unless --force-multi), and a file with no rows.

Usage:
  parquet_rename.py SRC DST --rename OLD=NEW [--rename OLD=NEW ...]
  parquet_rename.py SRC DST --map mapping.json
  parquet_rename.py SRC DST --drop NAME [--drop NAME ...] [--rename OLD=NEW ...]
  parquet_rename.py SRC DST --set NAME=VALUE [--set-null NAME] [--drop ...] [--rename ...]
  parquet_rename.py --values values.tsv --out-dir DIR [--drop ...] [--rename ...]
  parquet_rename.py SRC DST --tail-only ...   write only the new footer tail

values.tsv has FILE<TAB>COLUMN<TAB>OLD<TAB>NEW lines; \\N is a null.  The
output of FILE is DIR/<basename of FILE>.  A file is refused when its column
does not hold OLD (with --force-multi: when OLD is not one of its values).

With no mapping the output is byte-identical to the input.  One JSON line on
stdout describes the result, one per file.  pyarrow is needed only when the
file has an "ARROW:schema" key-value, or for a replacement.
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

# More field ids and enum values, for value replacement.
SE_TYPE, SE_REPETITION, SE_CONVERTED_TYPE, SE_LOGICAL_TYPE = 1, 3, 6, 10
RG_NUM_ROWS = 3
CMD_TYPE, CMD_ENCODINGS, CMD_CODEC, CMD_NUM_VALUES = 1, 2, 4, 5
CMD_STATISTICS, CMD_ENCODING_STATS, CMD_SIZE_STATISTICS = 12, 13, 16
ST_MAX, ST_MIN, ST_NULL_COUNT, ST_DISTINCT_COUNT, ST_MAX_VALUE, ST_MIN_VALUE = 1, 2, 3, 4, 5, 6
ST_MAX_EXACT, ST_MIN_EXACT = 7, 8
PH_CRC, PH_DATA_PAGE_HEADER, PH_DATA_PAGE_HEADER_V2 = 4, 5, 8
DPH_NUM_VALUES, DPH2_NUM_ROWS = 1, 3
PAGE_DICTIONARY = 2
BYTE_ARRAY = 6
REQUIRED, OPTIONAL, REPEATED = 0, 1, 2
CONVERTED_UTF8, LOGICAL_STRING = 0, 1
ENC_RLE, ENC_BIT_PACKED, ENC_PLAIN_DICTIONARY, ENC_RLE_DICTIONARY = 3, 4, 2, 8
DATA_ENCODINGS = {0: "PLAIN", 6: "DELTA_LENGTH_BYTE_ARRAY", 7: "DELTA_BYTE_ARRAY"}
CODEC_NAMES = {0: "NONE", 1: "SNAPPY", 2: "GZIP", 4: "BROTLI", 6: "ZSTD", 7: "LZ4"}
NULL_TOKEN = "\\N"


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

    __slots__ = ("kind", "off", "length", "rg", "col", "new_off", "data", "base")

    def __init__(self, kind, off, length, rg, col):
        self.kind, self.off, self.length, self.rg, self.col = kind, off, length, rg, col
        self.new_off = None
        self.data = None  # new bytes (offset indexes, and the regions of a replaced column)
        self.base = off  # the offset that the page offsets of this chunk's data count from


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
                shift(loc, PL_OFFSET, ch.base, ch.base + ch.length - 1,
                      ch.new_off - ch.base, "a page location")
            reg.data = encode(oi)
            if len(reg.data) != lengths[id(reg)]:
                lengths[id(reg)] = len(reg.data)
                changed = True
        if not changed:
            return pos
    raise RenameError("the offset index layout does not settle")  # pragma: no cover


def check_total(rg: list, fid: int, cmds: list, new_cmds: list, size_fid: int, dropped: set,
                what: str) -> None:
    if not has(rg, fid):
        return
    if get(rg, fid) != sum(get(m, size_fid) for m in cmds):
        raise RenameError(f"a row group {what} is not the sum of its columns; refused")
    set_value(rg, fid, sum(get(m, size_fid) for c, m in enumerate(new_cmds) if c not in dropped))


def drop_footer(meta: list, ctx: dict, mapping, drops, regions: list[Region],
                nleaves: int, dropped: set, new_index: dict, repl: dict | None = None) -> int:
    """Edit the parsed footer for the drop: offsets, sizes, schema, orders.

    repl maps (row group, leaf) to the Encoded chunk of a replaced column;
    its ColumnMetaData takes the place of the old one.
    """
    repl = repl or {}
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
        new_cmds = [merged_cmd(m, repl[(r, c)]) if (r, c) in repl else m
                    for c, m in enumerate(cmds)]
        check_total(rg, RG_TOTAL_BYTE_SIZE, cmds, new_cmds, CMD_TOTAL_UNCOMPRESSED_SIZE, dropped,
                    "total_byte_size")
        check_total(rg, RG_TOTAL_COMPRESSED_SIZE, cmds, new_cmds, CMD_TOTAL_COMPRESSED_SIZE,
                    dropped, "total_compressed_size")
        if has(rg, RG_FILE_OFFSET):
            if get(rg, RG_FILE_OFFSET) != min(chunks[(r, c)].off for c in range(nleaves)):
                raise RenameError("a row group file_offset is not its first chunk; refused")
            set_value(rg, RG_FILE_OFFSET, min(chunks[(r, c)].new_off for c in new_index))
        new_cols = []
        for c, cc in enumerate(cols.items):
            if c in dropped:
                continue
            ch = chunks[(r, c)]
            lo, hi, d = ch.base, ch.base + ch.length, ch.new_off - ch.base
            cmd = new_cmds[c]
            for fid in (CMD_DATA_PAGE_OFFSET, CMD_INDEX_PAGE_OFFSET, CMD_DICTIONARY_PAGE_OFFSET):
                shift(cmd, fid, lo, hi - 1, d, "a page offset")
            if (r, c) in repl:
                set_value(cc, CC_META_DATA, cmd)
                move_file_offset(cc, chunk_span(cmds[c]), ch)
            else:
                shift(cc, CC_FILE_OFFSET, lo, hi, d, "a column chunk file_offset")
            if ("bloom", r, c) in by_key:
                set_value(cmd, CMD_BLOOM_FILTER_OFFSET, by_key[("bloom", r, c)].new_off)
            if ("column_index", r, c) in by_key:
                ci = by_key[("column_index", r, c)]
                set_value(cc, CC_COLUMN_INDEX_OFFSET, ci.new_off)
                set_value(cc, CC_COLUMN_INDEX_LENGTH, ci.length)
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
                       fsync: bool = True, values: dict | None = None,
                       expect: dict | None = None, force_multi: bool = False) -> dict:
    """Write src without the dropped columns, and with the renames, to dst.

    The kept column chunks, Bloom filters and column indexes are copied byte
    for byte, in their original order; offset indexes are re-encoded with
    their page offsets moved.  Page headers (data page V1 and V2, dictionary
    page) hold sizes, counts, encodings and statistics but no file offset, so
    no page changes.  The regions of a column in `values` are replaced by
    new ones (replace_plan).
    """
    values = values or {}
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
        repl, replaced = replace_plan(src, sfd, meta, ctx, values, drops, expect or {},
                                      force_multi)
        put_replacements(kept, repl, oi_raw)
        data_end = layout(kept, chunks, oi_raw)
        renamed = drop_footer(meta, ctx, mapping, drops, regions, nleaves, dropped, new_index,
                              repl)
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
    rep = dict(replaced=replaced) if values else {}
    if values:
        rep["regions_reencoded"] = sum(1 for reg in kept if (reg.rg, reg.col) in repl)
    rep.update(renamed=dict(mapping), dropped=list(drops), column_chunks_renamed=renamed,
               arrow_schema=ctx["arrow_kv"] is not None, mentions_kept=ctx["found"],
               src=src, dst=dst, in_size=size, in_footer_len=len(footer),
               out_footer_len=len(new), out_size=data_end + len(tail),
               columns_kept=len(ctx["spans"]) - len(drops), leaves_dropped=len(dropped),
               regions_kept=kinds, bytes_copied=copied, copy=method)
    return rep


# ------------------------------------------------------- value replacement --
class Encoded:
    """A column chunk that pyarrow wrote for a replaced column, cut from its file.

    cmd is the encoded ColumnMetaData; its page offsets count from base, the
    chunk's offset in pyarrow's file.  bloom, column_index and offset_index
    are None when pyarrow wrote none.
    """

    __slots__ = ("cmd", "base", "chunk", "bloom", "column_index", "offset_index",
                 "value", "rows")

    def __init__(self, cmd, base, chunk, bloom, column_index, offset_index, value, rows):
        self.cmd, self.base, self.chunk, self.bloom = cmd, base, chunk, bloom
        self.column_index, self.offset_index = column_index, offset_index
        self.value, self.rows = value, rows


def parse_values(pairs: list[str], nulls: list[str]) -> dict:
    """{name: value} from --set NAME=VALUE and --set-null NAME (value None)."""
    values: dict = {}
    for p in pairs:
        name, sep, value = p.partition("=")
        if not sep:
            raise RenameError(f"--set {p!r}: expected NAME=VALUE")
        if name in values:
            raise RenameError(f"{name!r} is set twice")
        values[name] = value
    for name in nulls:
        if name in values:
            raise RenameError(f"{name!r} is set twice")
        values[name] = None
    return values


def tsv_value(text: str):
    return None if text == NULL_TOKEN else text


def read_values_tsv(tsv: str) -> dict[str, dict[str, tuple]]:
    """FILE -> {COLUMN: (OLD, NEW)} from FILE<TAB>COLUMN<TAB>OLD<TAB>NEW lines."""
    out: dict[str, dict[str, tuple]] = {}
    with open(tsv, encoding="utf-8") as fh:
        for n, line in enumerate(fh, 1):
            line = line.rstrip("\n")
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) != 4:
                raise RenameError(f"{tsv}:{n}: expected FILE<TAB>COLUMN<TAB>OLD<TAB>NEW")
            file, col, old, new = parts
            cols = out.setdefault(file, {})
            if col in cols:
                raise RenameError(f"{tsv}:{n}: {col!r} is set twice for one file")
            cols[col] = (tsv_value(old), tsv_value(new))
    return out


def is_string_leaf(elem: list) -> bool:
    """BYTE_ARRAY with the STRING logical type, or else the UTF8 converted type."""
    if get(elem, SE_TYPE) != BYTE_ARRAY:
        return False
    logical = get(elem, SE_LOGICAL_TYPE)
    if logical is not None:
        return [f.fid for f in logical] == [LOGICAL_STRING]
    return get(elem, SE_CONVERTED_TYPE) == CONVERTED_UTF8


def arrow_string_field(schema, name: str) -> bool:
    import pyarrow as pa

    t = schema.field(name).type
    if pa.types.is_dictionary(t):
        t = t.value_type
    return pa.types.is_string(t) or pa.types.is_large_string(t) or pa.types.is_string_view(t)


def replace_targets(ctx: dict, values: dict, drops) -> dict[int, tuple]:
    """Leaf index -> (name, value, nullable) of every replaced column.

    Refuses a column that is not there, nested or repeated, also dropped, not
    a string, or required when the value is a null.
    """
    schema, targets, leaf = ctx["schema"], {}, 0
    for i, _, name, n in ctx["spans"]:
        if name in values:
            elem, value = schema[i], values[name]
            if n != 1 or has(elem, SE_NUM_CHILDREN) or get(elem, SE_REPETITION) == REPEATED:
                raise RenameError(f"{name!r} is a nested or repeated column; refused")
            if name in drops:
                raise RenameError(f"{name!r} is both set and dropped")
            if not is_string_leaf(elem):
                raise RenameError(f"{name!r} is not a string column; a string value does not fit")
            nullable = get(elem, SE_REPETITION) == OPTIONAL
            if value is None and not nullable:
                raise RenameError(f"{name!r} is a required column; a null does not fit")
            if value is not None:
                try:
                    value.encode("utf-8")
                except UnicodeEncodeError as e:
                    raise RenameError(f"the value for {name!r} is not valid UTF-8") from e
            arrow = ctx["arrow_schema"]
            if arrow is not None and not arrow_string_field(arrow, name):
                raise RenameError(f"ARROW:schema does not give {name!r} a string type; refused")
            targets[leaf] = (name, value, nullable)
        leaf += n
    missing = sorted(set(values) - {t[0] for t in targets.values()})
    if missing:
        raise RenameError(f"no top-level column {missing[0]!r} to set")
    return targets


def old_values(src: str, name: str) -> set:
    """The distinct values of a column (None for a null), read with pyarrow."""
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(src)
    seen: set = set()
    for i in range(pf.metadata.num_row_groups):
        seen.update(pc.unique(pf.read_row_group(i, columns=[name]).column(0)).to_pylist())
    return seen


def header_at(fd: int, off: int) -> tuple[list, int]:
    """A page header; tries a larger read when the first one cuts it."""
    try:
        return struct_at(fd, off)
    except RenameError:
        return struct_at(fd, off, 1 << 20)


def page_shape(fd: int, cmd: list) -> tuple[int, bool, int]:
    """(data page version, page checksums, most rows in one data page) of a chunk."""
    start, length = chunk_span(cmd)
    pos, end = start, start + length
    versions, crc, rows = set(), False, 0
    while pos < end:
        header, hlen = header_at(fd, pos)
        kind = get(header, PH_TYPE)
        crc = crc or has(header, PH_CRC)
        if kind == PAGE_DATA:
            versions.add(1)
            rows = max(rows, get(get(header, PH_DATA_PAGE_HEADER) or [], DPH_NUM_VALUES) or 0)
        elif kind == PAGE_DATA_V2:
            versions.add(2)
            rows = max(rows, get(get(header, PH_DATA_PAGE_HEADER_V2) or [], DPH2_NUM_ROWS) or 0)
        pos += hlen + (get(header, PH_COMPRESSED_PAGE_SIZE) or 0)
    if pos != end or len(versions) != 1:
        raise RenameError("a chunk to replace mixes data page versions or its pages do not "
                          "fill it; refused")
    return versions.pop(), crc, rows


def encoding_family(cmd: list) -> tuple[str, bool]:
    """("dictionary", PLAIN_DICTIONARY only) or (PLAIN or DELTA_*, False)."""
    encs = set(get(cmd, CMD_ENCODINGS).items)
    if encs & {ENC_RLE_DICTIONARY, ENC_PLAIN_DICTIONARY}:
        return "dictionary", ENC_RLE_DICTIONARY not in encs
    data = encs - {ENC_RLE, ENC_BIT_PACKED}
    if len(data) == 1 and min(data) in DATA_ENCODINGS:
        return DATA_ENCODINGS[min(data)], False
    raise RenameError(f"the encodings {sorted(encs)} of a chunk to replace are not supported")


def encode_column(name: str, value, nullable: bool, shape: tuple) -> Encoded:
    """Write one row group of the value with pyarrow, in memory, and cut out its chunk."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    rows, codec, family, plain_dict, version, crc, page_rows, bloom, page_index = shape
    if codec not in CODEC_NAMES:
        raise RenameError(f"codec {codec} of {name!r} cannot be written; refused")
    typ = pa.string()
    arr = pa.nulls(rows, typ) if value is None else pa.repeat(pa.scalar(value, typ), rows)
    table = pa.Table.from_arrays([arr], schema=pa.schema([pa.field(name, typ, nullable)]))
    kw = dict(row_group_size=rows, compression=CODEC_NAMES[codec],
              use_dictionary=family == "dictionary", data_page_version=f"{version}.0",
              version="1.0" if plain_dict else "2.6", write_statistics=True,
              write_page_index=page_index, write_page_checksum=crc,
              max_rows_per_page=page_rows, store_schema=False)
    if family != "dictionary":
        kw["column_encoding"] = {name: family}
    if bloom:
        kw["bloom_filter_options"] = {name: {"ndv": 1, "fpp": 0.001}}
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, **kw)
    buf = sink.getvalue().to_pybytes()
    meta = parse_footer(buf[-8 - struct.unpack("<I", buf[-8:-4])[0]:-8])
    rgs = kv_items(meta, FMD_ROW_GROUPS)
    if len(rgs) != 1 or get(rgs[0], RG_NUM_ROWS) != rows:
        raise RenameError(f"pyarrow did not write {rows} rows of {name!r} in one row group")
    cc = get(rgs[0], RG_COLUMNS).items[0]
    cmd = get(cc, CC_META_DATA)
    if encoding_family(cmd)[0] != family:
        raise RenameError(f"pyarrow did not write {name!r} with {family} encoding; refused")
    base, length = chunk_span(cmd)
    bloom_bytes = None
    if has(cmd, CMD_BLOOM_FILTER_OFFSET):
        off = get(cmd, CMD_BLOOM_FILTER_OFFSET)
        n = get(cmd, CMD_BLOOM_FILTER_LENGTH)
        if n is None:
            r = Reader(buf[off:])
            n = r.p + get(r.struct(), BF_NUM_BYTES)
        bloom_bytes = buf[off:off + n]

    def cut(fo: int, fl: int):
        return buf[get(cc, fo):get(cc, fo) + get(cc, fl)] if has(cc, fo) else None

    return Encoded(encode(cmd), base, buf[base:base + length], bloom_bytes,
                   cut(CC_COLUMN_INDEX_OFFSET, CC_COLUMN_INDEX_LENGTH),
                   cut(CC_OFFSET_INDEX_OFFSET, CC_OFFSET_INDEX_LENGTH), value, rows)


def normalized(fields: list) -> list:
    """Fields in id order, each with the short header form where it fits."""
    fields = sorted(fields, key=lambda f: f.fid)
    last = 0
    for f in fields:
        f.short = 0 < f.fid - last <= 15
        last = f.fid
    return fields


def rebuilt_statistics(old: list, value, rows: int) -> list:
    """A Statistics struct with the old one's fields, for a column of one value."""
    v = None if value is None else value.encode("utf-8")
    out = []
    for f in old:
        if f.fid in (ST_MAX, ST_MIN, ST_MAX_VALUE, ST_MIN_VALUE):
            if v is not None:
                out.append(Field(f.fid, T_BINARY, v, True))
        elif f.fid in (ST_MAX_EXACT, ST_MIN_EXACT):
            if v is not None:
                out.append(Field(f.fid, T_TRUE, None, True))
        elif f.fid == ST_NULL_COUNT:
            out.append(Field(f.fid, f.ttype, rows if v is None else 0, True))
        elif f.fid == ST_DISTINCT_COUNT:
            out.append(Field(f.fid, f.ttype, 0 if v is None else 1, True))
        else:
            raise RenameError(f"statistics field {f.fid} of a replaced column is not known")
    return normalized(out)


CMD_KEEP_OLD = (CMD_TYPE, CMD_PATH_IN_SCHEMA, CMD_CODEC, CMD_KV)
CMD_FROM_NEW = (CMD_ENCODINGS, CMD_NUM_VALUES, CMD_TOTAL_UNCOMPRESSED_SIZE,
                CMD_TOTAL_COMPRESSED_SIZE, CMD_DATA_PAGE_OFFSET, CMD_INDEX_PAGE_OFFSET,
                CMD_DICTIONARY_PAGE_OFFSET)
CMD_IF_BOTH = (CMD_ENCODING_STATS, CMD_BLOOM_FILTER_OFFSET, CMD_SIZE_STATISTICS)


def merged_cmd(old: list, e: Encoded) -> list:
    """The ColumnMetaData of a replaced chunk.

    Type, path, codec and key-values stay; sizes, encodings, value count and
    page offsets come from pyarrow; encoding_stats, size_statistics and the
    Bloom filter fields stay present only where they were; the statistics
    keep their fields with new values.
    """
    new = parse_footer(e.cmd)
    for fid, what in ((CMD_TYPE, "type"), (CMD_CODEC, "codec"), (CMD_NUM_VALUES, "value count")):
        if get(new, fid) != get(old, fid):
            raise RenameError(f"pyarrow wrote another {what} for a replaced column; refused")
    known = CMD_KEEP_OLD + CMD_FROM_NEW + CMD_IF_BOTH + (CMD_STATISTICS, CMD_BLOOM_FILTER_LENGTH)
    unknown = sorted(f.fid for f in old if f.fid not in known)
    if unknown:
        raise RenameError(f"ColumnMetaData field {unknown[0]} of a replaced column is not known")
    out = [f for f in old if f.fid in CMD_KEEP_OLD]
    out += [f for f in new if f.fid in CMD_FROM_NEW or (f.fid in CMD_IF_BOTH and has(old, f.fid))]
    if has(old, CMD_BLOOM_FILTER_LENGTH):
        out.append(Field(CMD_BLOOM_FILTER_LENGTH, T_I32, len(e.bloom), True))
    if has(old, CMD_STATISTICS):
        out.append(Field(CMD_STATISTICS, T_STRUCT,
                         rebuilt_statistics(get(old, CMD_STATISTICS), e.value, e.rows), True))
    return normalized(out)


def move_file_offset(cc: list, old_span: tuple[int, int], ch: Region) -> None:
    """A replaced chunk's file_offset: 0, its start or its end, as it was."""
    fo = get(cc, CC_FILE_OFFSET)
    if not fo:
        return
    start, length = old_span
    if fo == start:
        set_value(cc, CC_FILE_OFFSET, ch.new_off)
    elif fo == start + length:
        set_value(cc, CC_FILE_OFFSET, ch.new_off + ch.length)
    else:
        raise RenameError("a column chunk file_offset is not its start or its end; refused")


def replace_plan(src: str, fd: int, meta: list, ctx: dict, values: dict, drops, expect: dict,
                 force_multi: bool) -> tuple[dict, dict]:
    """(row group, leaf) -> Encoded for every chunk of a replaced column, and a report.

    pyarrow writes one chunk per distinct row group shape (rows, codec,
    encoding family, page version, checksums, rows per page, Bloom filter,
    page index); row groups of the same shape share it.
    """
    if not values:
        return {}, {}
    targets = replace_targets(ctx, values, drops)
    rgs = kv_items(meta, FMD_ROW_GROUPS)
    if not rgs or not all(get(rg, RG_NUM_ROWS) for rg in rgs):
        raise RenameError("the file or one of its row groups has no rows; refused")
    repl, report, cache = {}, {}, {}
    for leaf, (name, value, nullable) in targets.items():
        seen = old_values(src, name)
        if len(seen) > 1 and not force_multi:
            raise RenameError(f"{name!r} has {len(seen)} distinct values; "
                              "pass --force-multi to replace them all")
        if name in expect:
            want = expect[name]
            if force_multi and want not in seen:
                raise RenameError(f"{want!r} is not one of the {len(seen)} values of {name!r}")
            if not force_multi and seen != {want}:
                raise RenameError(f"the old value of {name!r} is {next(iter(seen))!r}, "
                                  f"not {want!r}; refused")
        report[name] = dict(new=value, old=next(iter(seen)) if len(seen) == 1 else None,
                            old_distinct=len(seen))
        for r, rg in enumerate(rgs):
            cc = get(rg, RG_COLUMNS).items[leaf]
            cmd = get(cc, CC_META_DATA)
            version, crc, page_rows = page_shape(fd, cmd)
            family, plain_dict = encoding_family(cmd)
            shape = (get(rg, RG_NUM_ROWS), get(cmd, CMD_CODEC), family, plain_dict, version, crc,
                     max(page_rows, 1), has(cmd, CMD_BLOOM_FILTER_OFFSET),
                     has(cc, CC_OFFSET_INDEX_OFFSET) or has(cc, CC_COLUMN_INDEX_OFFSET))
            if (name, shape) not in cache:
                cache[(name, shape)] = encode_column(name, value, nullable, shape)
            repl[(r, leaf)] = cache[(name, shape)]
    return repl, report


def put_replacements(kept: list[Region], repl: dict, oi_raw: dict) -> None:
    """Point the kept regions of a replaced column at their new bytes."""
    for reg in kept:
        e = repl.get((reg.rg, reg.col))
        if e is None:
            continue
        data = dict(chunk=e.chunk, bloom=e.bloom, column_index=e.column_index,
                    offset_index=e.offset_index)[reg.kind]
        if data is None:
            raise RenameError(f"pyarrow wrote no {reg.kind} for a replaced column; refused")
        reg.length = len(data)
        if reg.kind == "offset_index":
            oi_raw[id(reg)] = data
        else:
            reg.data = data
        if reg.kind == "chunk":
            reg.base = e.base


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
                fsync: bool = True, values: dict | None = None, expect: dict | None = None,
                force_multi: bool = False) -> dict:
    """Write the renamed file (or only its new tail) to dst.

    With drops or values, the kept column chunks are copied to new offsets
    instead (rewrite_with_drops); their pages still keep their bytes, and only
    the chunks of a column in values are new.  expect gives the old value
    that each column in values must hold.
    """
    if os.path.exists(dst) and os.path.samefile(src, dst):
        raise RenameError("the output is the input file; refused")
    if drops or values:
        if tail_only:
            raise RenameError("--tail-only cannot drop columns or set values: the data part "
                              "changes")
        return rewrite_with_drops(src, dst, mapping, list(drops), copy=copy,
                                  allow_mentions=allow_mentions, fsync=fsync, values=values,
                                  expect=expect, force_multi=force_multi)
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


def value_args(ap: argparse.ArgumentParser) -> None:
    ap.add_argument("--set", action="append", default=[], metavar="NAME=VALUE",
                    help="give string column NAME the value VALUE in every row (repeatable)")
    ap.add_argument("--set-null", action="append", default=[], metavar="NAME",
                    help="make column NAME null in every row (repeatable)")
    ap.add_argument("--values", metavar="TSV",
                    help="FILE<TAB>COLUMN<TAB>OLD<TAB>NEW lines; \\N is a null; needs --out-dir")
    ap.add_argument("--out-dir", metavar="DIR", help="output folder for --values")
    ap.add_argument("--force-multi", action="store_true",
                    help="replace a column that holds more than one distinct value")


def value_jobs(a) -> list[tuple[str, str, dict, dict]]:
    """(src, dst, values, expected old values) for each file to write."""
    if a.values:
        if a.src or a.dst or a.set or a.set_null:
            raise RenameError("--values takes no SRC DST, --set or --set-null")
        if not a.out_dir:
            raise RenameError("--values needs --out-dir")
        jobs, names = [], set()
        for src, cols in read_values_tsv(a.values).items():
            dst = os.path.join(a.out_dir, os.path.basename(src))
            if dst in names:
                raise RenameError(f"two files in {a.values} have the name "
                                  f"{os.path.basename(src)!r}")
            names.add(dst)
            jobs.append((src, dst, {c: new for c, (_, new) in cols.items()},
                         {c: old for c, (old, _) in cols.items()}))
        return jobs
    if not a.src or not a.dst:
        raise RenameError("give SRC and DST, or --values with --out-dir")
    return [(a.src, a.dst, parse_values(a.set, a.set_null), {})]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("src", nargs="?")
    ap.add_argument("dst", nargs="?")
    mapping_args(ap)
    value_args(ap)
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
        jobs = value_jobs(a)
    except RenameError as e:
        print(json.dumps(dict(src=a.src, dst=a.dst, ok=False, error=str(e))))
        return 2
    status = 0
    for src, dst, values, expect in jobs:
        try:
            rep = rename_file(src, dst, mapping, drops=a.drop, copy=a.copy,
                              allow_mentions=a.allow_mentions, tail_only=a.tail_only,
                              fsync=not a.no_fsync, values=values, expect=expect,
                              force_multi=a.force_multi)
        except (RenameError, OSError) as e:
            print(json.dumps(dict(src=src, dst=dst, ok=False, error=str(e))), flush=True)
            status = 2
            continue
        rep["ok"] = True
        print(json.dumps(rep), flush=True)
    return status


if __name__ == "__main__":
    sys.exit(main())
