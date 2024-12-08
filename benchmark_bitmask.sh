#!/bin/bash
hyperfine \
  --warmup 1 \
  --runs 3 \
  "python3 bitmask_test.py" \
  "python3 lexicographic_test.py" \
  --export-markdown results.md

# Display results
cat results.md
