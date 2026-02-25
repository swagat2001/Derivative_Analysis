#!/usr/bin/env python3
"""
remove_emojis.py
----------------
Scans all text-based source files in the project and removes emoji characters.
Targets: .html, .py, .js, .css, .ts, .json, .txt, .md
Skips:   binary files, __pycache__, .git, node_modules, venv directories.

Usage:
    python remove_emojis.py              # dry-run (preview only, no changes)
    python remove_emojis.py --apply      # apply changes
"""

import os
import re
import sys
import argparse

# ─── Emoji Unicode ranges ──────────────────────────────────────────────────────
# This regex covers all major emoji blocks including:
#   • Emoticons / Misc Symbols and Pictographs
#   • Transport and Map Symbols
#   • Supplemental Symbols and Pictographs
#   • Dingbats, Enclosed characters, Regional indicators
#   • Variation selectors and Zero-width joiners used in emoji sequences
EMOJI_RE = re.compile(
    "["
    "\U0001F600-\U0001F64F"   # emoticons
    "\U0001F300-\U0001F5FF"   # misc symbols & pictographs
    "\U0001F680-\U0001F6FF"   # transport & map symbols
    "\U0001F700-\U0001F77F"   # alchemical symbols
    "\U0001F780-\U0001F7FF"   # geometric shapes extended
    "\U0001F800-\U0001F8FF"   # supplemental arrows-C
    "\U0001F900-\U0001F9FF"   # supplemental symbols & pictographs
    "\U0001FA00-\U0001FA6F"   # chess symbols
    "\U0001FA70-\U0001FAFF"   # symbols and pictographs extended-A
    "\U00002702-\U000027B0"   # dingbats
    "\U000024C2-\U0001F251"   # enclosed characters
    "\U0001F004"              # mahjong tile
    "\U0001F0CF"              # playing card black joker
    "\U0001F1E0-\U0001F1FF"   # regional indicator symbols (flags)
    "\U00002500-\U00002BEF"   # misc technical / box drawing / arrows
    "\u200d"                  # zero-width joiner (used in emoji sequences)
    "\uFE0F"                  # variation selector-16 (emoji presentation)
    "\u20E3"                  # combining enclosing keycap
    "]+",
    flags=re.UNICODE
)

# File extensions to process
TARGET_EXTENSIONS = {
    '.html', '.htm',
    '.py',
    '.js', '.ts', '.jsx', '.tsx',
    '.css', '.scss', '.sass',
    '.json',
    '.txt', '.md',
    '.jinja', '.jinja2', '.j2',
}

# Directories to skip entirely
SKIP_DIRS = {
    '__pycache__', '.git', '.hg', '.svn',
    'node_modules', 'venv', '.venv', 'env',
    '.tox', '.mypy_cache', '.pytest_cache',
    'dist', 'build', '.idea', '.vscode',
}

# Root of the project to scan
PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'Analysis_Tools')


def remove_emojis(text: str) -> str:
    return EMOJI_RE.sub('', text)


def process_file(path: str, apply: bool) -> tuple[bool, int]:
    """
    Returns (was_changed, emoji_count)
    """
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            original = f.read()
    except Exception as e:
        print(f"  [SKIP] {path}: {e}")
        return False, 0

    cleaned = remove_emojis(original)
    if cleaned == original:
        return False, 0

    # Count emojis removed
    emojis_found = EMOJI_RE.findall(original)
    count = sum(len(e) for e in emojis_found)

    if apply:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(cleaned)

    return True, count


def scan(root: str, apply: bool):
    total_files = 0
    changed_files = 0
    total_emojis = 0

    for dirpath, dirnames, filenames in os.walk(root):
        # Prune skip directories in-place
        dirnames[:] = [d for d in dirnames if d not in SKIP_DIRS]

        for filename in filenames:
            ext = os.path.splitext(filename)[1].lower()
            if ext not in TARGET_EXTENSIONS:
                continue

            filepath = os.path.join(dirpath, filename)
            total_files += 1
            changed, count = process_file(filepath, apply)
            if changed:
                changed_files += 1
                total_emojis += count
                rel = os.path.relpath(filepath, root)
                action = "UPDATED" if apply else "WOULD UPDATE"
                print(f"  [{action}] {rel}  ({count} emoji chars removed)")

    print()
    print("=" * 60)
    print(f"Files scanned       : {total_files}")
    print(f"Files with emojis   : {changed_files}")
    print(f"Total emoji chars   : {total_emojis}")
    if not apply:
        print()
        print("  >> DRY RUN — no files were modified.")
        print("  >> Run with --apply to apply changes.")
    else:
        print()
        print("  >> Done! All emoji characters removed.")
    print("=" * 60)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Remove emojis from all project source files.')
    parser.add_argument('--apply', action='store_true',
                        help='Apply changes (default is dry-run/preview)')
    parser.add_argument('--root', default=PROJECT_ROOT,
                        help=f'Root directory to scan (default: {PROJECT_ROOT})')
    args = parser.parse_args()

    print(f"Scanning: {args.root}")
    print(f"Mode    : {'APPLY (changes will be saved)' if args.apply else 'DRY RUN (preview only)'}")
    print()

    scan(args.root, args.apply)




# # Preview what would change
# python remove_emojis.py

# # Apply changes
# python remove_emojis.py --apply

# # Scan a different directory
# python remove_emojis.py --root path/to/dir --apply
