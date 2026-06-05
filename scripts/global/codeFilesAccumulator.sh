
#!/bin/bash
# scripts/global/codeFilesAccumulator.sh
# Usage:
#   scripts/global/codeFilesAccumulator.sh <file1> [file2 ...] [a]
# Notes:
# - Run from project root.
# - Pass "a" (any position) to append; otherwise starts fresh at 1.
# - Missing/inaccessible files are reported to stderr; script continues.
# - Exit code is 1 if any file errors occurred; 0 otherwise.

set -o pipefail

# Resolve script and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RELATIVE_ROOT="$(basename "$PROJECT_ROOT")"

OUTPUT_FILE="$SCRIPT_DIR/codeFiles.txt"
SCRIPT_NAME="$(basename "$0")"

APPEND_MODE=false
counter=1
declare -a TARGET_FILES=()
had_error=false

# ---- Parse args (allow "a" anywhere) ----
if [ $# -eq 0 ]; then
  echo "Usage: scripts/global/$SCRIPT_NAME <file1> [file2 ...] [a]" >&2
  exit 1
fi

for arg in "$@"; do
  if [[ "$arg" == "a" ]]; then
    APPEND_MODE=true
  else
    CANDIDATE_PATH="${arg#/}"
    ABS_PATH="$PROJECT_ROOT/$CANDIDATE_PATH"
    # Store even if missing; we’ll validate later so we can report nicely and continue
    TARGET_FILES+=("$ABS_PATH")
  fi
done

if [ ${#TARGET_FILES[@]} -eq 0 ]; then
  echo "Error: no files provided. (Hint: place 'a' after your file args if appending.)" >&2
  exit 1
fi

# ---- Determine starting counter ----
if [ "$APPEND_MODE" = false ]; then
  : > "$OUTPUT_FILE"
else
  if [ -f "$OUTPUT_FILE" ]; then
    last_num=$(grep -oP '^\d+\.' "$OUTPUT_FILE" | tail -1 | cut -d. -f1)
    if [[ $last_num =~ ^[0-9]+$ ]]; then
      counter=$((last_num + 1))
    fi
  fi
fi

cd "$PROJECT_ROOT" || { echo "Error: unable to cd to project root at $PROJECT_ROOT" >&2; exit 1; }

# ---- Process files ----
for file in "${TARGET_FILES[@]}"; do
  REL_PATH="${file#$PROJECT_ROOT/}"

  # Validate file
  if [ ! -e "$file" ]; then
    echo "Warning: file not found -> $REL_PATH" >&2
    had_error=true
    continue
  fi
  if [ ! -f "$file" ]; then
    echo "Warning: not a regular file -> $REL_PATH" >&2
    had_error=true
    continue
  fi
  if [ ! -r "$file" ]; then
    echo "Warning: file not readable -> $REL_PATH" >&2
    had_error=true
    continue
  fi

  # Write header + content
  {
    echo "----------------------------------------------"
    echo "$counter.  $REL_PATH"
    echo "----------------------------------------------"
    echo ""

    if [ -s "$file" ]; then
      # If cat fails (rare), we still continue
      if ! cat "$file"; then
        echo "[ERROR READING FILE CONTENT]"
        had_error=true
      fi
    else
      echo "[EMPTY FILE]"
    fi

    echo
    echo
  } >> "$OUTPUT_FILE"

  ((counter++))
done

echo "Code files accumulated at: $RELATIVE_ROOT/scripts/global/$(basename "$OUTPUT_FILE")"

# Exit non-zero if any errors occurred
$had_error && exit 1 || exit 0
