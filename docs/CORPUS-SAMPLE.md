# Corpus sample

Generated 2026-09-14 by `select_corpus.py sample`. Do not edit by hand.

- Frame: **3949** eligible projects in `candidates.csv`.
- Target: **400** projects. Drawn: **400**.
- Seed: **20261110**. Floor: **2** per cell.

Allocation takes a floor from every cell first, then shares the rest
out by largest remainder. A cell is never asked for more than it holds,
so a thin cell caps the draw instead of failing it.

| stratum | language | size | in frame | drawn | share |
| --- | --- | --- | --- | --- | --- |
| community | C | L | 7 | 3 | 43% |
| community | C | M | 44 | 6 | 14% |
| community | C | S | 543 | 49 | 9% |
| community | C++ | L | 7 | 3 | 43% |
| community | C++ | M | 58 | 7 | 12% |
| community | C++ | S | 651 | 59 | 9% |
| community | Java | M | 29 | 4 | 14% |
| community | Java | S | 636 | 57 | 9% |
| community | Rust | M | 7 | 3 | 43% |
| community | Rust | S | 373 | 34 | 9% |
| company-owned | C | L | 5 | 2 | 40% |
| company-owned | C | M | 15 | 3 | 20% |
| company-owned | C | S | 305 | 28 | 9% |
| company-owned | C++ | L | 8 | 3 | 38% |
| company-owned | C++ | M | 16 | 3 | 19% |
| company-owned | C++ | S | 385 | 35 | 9% |
| company-owned | Java | L | 2 | 2 | 100% |
| company-owned | Java | M | 18 | 3 | 17% |
| company-owned | Java | S | 394 | 36 | 9% |
| company-owned | Rust | M | 2 | 2 | 100% |
| company-owned | Rust | S | 139 | 14 | 10% |
| foundation | C | L | 1 | 1 | 100% |
| foundation | C | M | 3 | 2 | 67% |
| foundation | C | S | 7 | 2 | 29% |
| foundation | C++ | L | 1 | 1 | 100% |
| foundation | C++ | M | 2 | 2 | 100% |
| foundation | C++ | S | 24 | 4 | 17% |
| foundation | Java | M | 14 | 3 | 21% |
| foundation | Java | S | 214 | 21 | 10% |
| foundation | Rust | L | 1 | 1 | 100% |
| foundation | Rust | M | 2 | 2 | 100% |
| foundation | Rust | S | 36 | 5 | 14% |
| **total** | | | **3949** | **400** | **10%** |
