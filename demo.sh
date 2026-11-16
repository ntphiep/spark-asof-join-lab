#!/usr/bin/env bash
# Prints the saved results for the terminal screenshot in the post.
cd "$(dirname "$0")"
echo '$ python summarize.py'
.venv/bin/python summarize.py
echo
read -r -p '' _
