#!/usr/bin/env bash
# compare.sh LABEL: compare a gate result with the baseline base-1.
G=/local/home/ellianco/Projects/cregit-workspace/refactor-gate/results
if diff $G/base-1/SHA256SUMS $G/$1/SHA256SUMS; then echo "GATE $1: IDENTICAL ($(wc -l < $G/$1/SHA256SUMS) entries)"; else
  echo "GATE $1: DIFFERS"; diff -r $G/base-1/norm $G/$1/norm | head -40; exit 1; fi
