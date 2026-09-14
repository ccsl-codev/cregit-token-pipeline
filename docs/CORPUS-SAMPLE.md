# Corpus sample

Generated 2026-09-14 by `select_corpus.py sample`. Do not edit by hand.

- Frame: **3840** eligible projects in `candidates.csv`.
- Target: **200** projects. Drawn: **200**.
- Seed: **20261110**. Floor: **2** per cell.

Allocation takes a floor from every cell first, then shares the rest
out by largest remainder. A cell is never asked for more than it holds,
so a thin cell caps the draw instead of failing it.

| stratum | language | size | in frame | drawn | share |
| --- | --- | --- | --- | --- | --- |
| community | C | L | 6 | 2 | 33% |
| community | C | M | 43 | 4 | 9% |
| community | C | S | 522 | 21 | 4% |
| community | C++ | L | 7 | 2 | 29% |
| community | C++ | M | 55 | 4 | 7% |
| community | C++ | S | 628 | 25 | 4% |
| community | Java | M | 29 | 3 | 10% |
| community | Java | S | 623 | 25 | 4% |
| community | Rust | M | 7 | 2 | 29% |
| community | Rust | S | 354 | 15 | 4% |
| company-owned | C | L | 3 | 2 | 67% |
| company-owned | C | M | 15 | 3 | 20% |
| company-owned | C | S | 301 | 13 | 4% |
| company-owned | C++ | L | 8 | 2 | 25% |
| company-owned | C++ | M | 15 | 3 | 20% |
| company-owned | C++ | S | 385 | 16 | 4% |
| company-owned | Java | L | 2 | 2 | 100% |
| company-owned | Java | M | 17 | 3 | 18% |
| company-owned | Java | S | 387 | 16 | 4% |
| company-owned | Rust | M | 2 | 2 | 100% |
| company-owned | Rust | S | 139 | 7 | 5% |
| foundation | C | L | 1 | 1 | 100% |
| foundation | C | M | 3 | 2 | 67% |
| foundation | C | S | 7 | 2 | 29% |
| foundation | C++ | L | 1 | 1 | 100% |
| foundation | C++ | M | 2 | 2 | 100% |
| foundation | C++ | S | 23 | 3 | 13% |
| foundation | Java | M | 12 | 2 | 17% |
| foundation | Java | S | 206 | 10 | 5% |
| foundation | Rust | L | 1 | 1 | 100% |
| foundation | Rust | M | 1 | 1 | 100% |
| foundation | Rust | S | 35 | 3 | 9% |
| **total** | | | **3840** | **200** | **5%** |
