#!/bin/bash
set -euo pipefail

# -----------------------------
# Paths / constants
# -----------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RELATIVE_ROOT="$(basename "$PROJECT_ROOT")"

SCRIPT_NAME="$(basename "$0")"
OUTPUT_ZIP="$SCRIPT_DIR/latestCode.zip"

# Exclusions relative to project root
declare -a EXCLUDE_FILES=(
  "$SCRIPT_NAME"
  "scripts/global/$(basename "$OUTPUT_ZIP")"
)

# -----------------------------
# Options / args
# -----------------------------
SCAN_DIR="$PROJECT_ROOT"
ROOT_ONLY=false
ECHO_ONLY=false
TARGET_FILES=()

while [ $# -gt 0 ]; do
  case "$1" in
    r) ROOT_ONLY=true ;;
    e) ECHO_ONLY=true ;;
    *)
      CANDIDATE_PATH="${1#/}"
      ABS_PATH="$PROJECT_ROOT/$CANDIDATE_PATH"
      if [[ -d "$ABS_PATH" ]]; then
        SCAN_DIR="$ABS_PATH"
      elif [[ -f "$ABS_PATH" ]]; then
        TARGET_FILES+=("$ABS_PATH")
      else
        echo "Error: Invalid argument '$1' (not a file/dir under project root)"
        exit 1
      fi
      ;;
  esac
  shift
done

echo "Scanning: $SCAN_DIR"
echo "Zip output: $RELATIVE_ROOT/scripts/global/$(basename "$OUTPUT_ZIP")"
[ "$ROOT_ONLY" = true ] && echo "Root-only scan (non-recursive)"
[ "$ECHO_ONLY" = true ] && echo "Echo-only (will not write zip)"
echo

cd "$PROJECT_ROOT" || exit 1

# -----------------------------
# Ignore handling
# -----------------------------
IGNORE_CHECK_DIR="$(mktemp -d)"
git -C "$IGNORE_CHECK_DIR" init -q

FILELIST="$(mktemp)"
cleanup() {
  rm -f "$FILELIST" 2>/dev/null || true
  rm -rf "$IGNORE_CHECK_DIR" 2>/dev/null || true
}
trap cleanup EXIT

# Build PRUNE_DIRS from directory patterns (ending with /) in .codegenignore
PRUNE_DIRS=()
CODEGENIGNORE="$SCRIPT_DIR/.codegenignore"
if [ -f "$CODEGENIGNORE" ]; then
  while IFS= read -r line || [ -n "$line" ]; do
    line="${line%%$'\r'}"
    line="${line#"${line%%[![:space:]]*}"}"
    line="${line%"${line##*[![:space:]]}"}"

    [ -z "$line" ] && continue
    case "$line" in
      \#*) continue ;;
      !*)  continue ;;
    esac

    case "$line" in
      */)
        dirpat="${line%/}"
        [[ "$dirpat" == /* ]] && dirpat="${dirpat#/}"

        if [[ "$dirpat" == *"/"* ]]; then
          PRUNE_DIRS+=("$PROJECT_ROOT/$dirpat")
        else
          PRUNE_DIRS+=("*/$dirpat")
        fi
        ;;
    esac
  done < "$CODEGENIGNORE"
fi

# -----------------------------
# Find command
# -----------------------------
FIND_FROM_LIST=()

if [ "${#TARGET_FILES[@]}" -gt 0 ]; then
  FIND_FROM_LIST=("${TARGET_FILES[@]}")
else
  if [ "$ROOT_ONLY" = true ]; then
    FIND_CMD=(find "$SCAN_DIR" -maxdepth 1 -type f ! -path "*/.git/*" -print0)
  else
    if [ "${#PRUNE_DIRS[@]}" -gt 0 ]; then
      FIND_CMD=(find "$SCAN_DIR" "(" -type d "(")
      for i in "${!PRUNE_DIRS[@]}"; do
        FIND_CMD+=(-path "${PRUNE_DIRS[$i]}")
        if [ "$i" -lt $((${#PRUNE_DIRS[@]} - 1)) ]; then
          FIND_CMD+=(-o)
        fi
      done
      FIND_CMD+=(")" -prune ")" -o -type f ! -path "*/.git/*" -print0)
    else
      FIND_CMD=(find "$SCAN_DIR" -type f ! -path "*/.git/*" -print0)
    fi
  fi
fi

# -----------------------------
# Collect included files (project-root-relative paths)
# -----------------------------
add_if_included() {
  local abs="$1"
  local rel="${abs#$PROJECT_ROOT/}"

  # Skip node_modules
  if [[ "$rel" == node_modules/* ]]; then
    return
  fi

  # Skip excluded files
  for excluded in "${EXCLUDE_FILES[@]}"; do
    if [[ "$rel" == "$excluded" ]]; then
      return
    fi
  done

  # Respect .codegenignore (including negations)
  if [ -f "$CODEGENIGNORE" ]; then
    if git -C "$IGNORE_CHECK_DIR" -c core.excludesfile="$CODEGENIGNORE" check-ignore -q --no-index "$rel"; then
      return
    fi
  fi

  printf '%s\n' "$rel" >> "$FILELIST"
}

if [ "${#FIND_FROM_LIST[@]}" -gt 0 ]; then
  for f in "${FIND_FROM_LIST[@]}"; do
    [ -f "$f" ] && add_if_included "$f"
  done
else
  while IFS= read -r -d '' file; do
    add_if_included "$file"
  done < <("${FIND_CMD[@]}")
fi

COUNT_INCLUDED="$(wc -l < "$FILELIST" | tr -d ' ')"

if [ "$COUNT_INCLUDED" -eq 0 ]; then
  echo "No files matched (after exclusions/.codegenignore). Nothing to zip."
  exit 0
fi

echo "Included files: $COUNT_INCLUDED"

if [ "$ECHO_ONLY" = true ]; then
  echo
  echo "---- Files that would be zipped (project-root-relative) ----"
  cat "$FILELIST"
  exit 0
fi

# -----------------------------
# Create ZIP
#   - Prefer 'zip' if installed
#   - Otherwise use Windows PowerShell (Windows 11 + Git Bash)
# -----------------------------
rm -f "$OUTPUT_ZIP"

if command -v zip >/dev/null 2>&1; then
  zip -q "$OUTPUT_ZIP" -@ < "$FILELIST"
else
  if ! command -v powershell.exe >/dev/null 2>&1; then
    echo "Error: 'zip' not found and 'powershell.exe' not found. Cannot create zip."
    exit 1
  fi

  # Convert paths for PowerShell if possible
  if command -v cygpath >/dev/null 2>&1; then
    WIN_PROJECT_ROOT="$(cygpath -w "$PROJECT_ROOT")"
    WIN_OUTPUT_ZIP="$(cygpath -w "$OUTPUT_ZIP")"
    WIN_FILELIST="$(cygpath -w "$FILELIST")"
  elif command -v wslpath >/dev/null 2>&1; then
    WIN_PROJECT_ROOT="$(wslpath -w "$PROJECT_ROOT")"
    WIN_OUTPUT_ZIP="$(wslpath -w "$OUTPUT_ZIP")"
    WIN_FILELIST="$(wslpath -w "$FILELIST")"
  else
    WIN_PROJECT_ROOT="$PROJECT_ROOT"
    WIN_OUTPUT_ZIP="$OUTPUT_ZIP"
    WIN_FILELIST="$FILELIST"
  fi

  powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "
    \$ErrorActionPreference = 'Stop'
    Add-Type -AssemblyName System.IO.Compression.FileSystem

    \$root = '$WIN_PROJECT_ROOT'
    \$zip  = '$WIN_OUTPUT_ZIP'
    \$list = Get-Content '$WIN_FILELIST' | Where-Object { \$_ -and \$_.Trim() -ne '' }

    if (Test-Path \$zip) { Remove-Item \$zip -Force }

    # Create empty zip
    [System.IO.Compression.ZipFile]::Open(\$zip, 'Create').Dispose()

    foreach (\$rel in \$list) {
      \$abs = Join-Path \$root \$rel
      if (Test-Path \$abs) {
        \$zipObj = [System.IO.Compression.ZipFile]::Open(\$zip, 'Update')
        try {
          [System.IO.Compression.ZipFileExtensions]::CreateEntryFromFile(\$zipObj, \$abs, \$rel) | Out-Null
        } finally {
          \$zipObj.Dispose()
        }
      }
    }
  "
fi

echo "Zip written to: $RELATIVE_ROOT/scripts/global/$(basename "$OUTPUT_ZIP")"

# -----------------------------
# Best-effort: open the folder that contains the zip
# -----------------------------
OUTPUT_DIR="$(cd "$(dirname "$OUTPUT_ZIP")" 2>/dev/null && pwd)"

if command -v open >/dev/null 2>&1; then
  # macOS (reveal the file)
  open -R "$OUTPUT_ZIP" >/dev/null 2>&1 || open "$OUTPUT_DIR" >/dev/null 2>&1 || true
elif command -v xdg-open >/dev/null 2>&1; then
  # Linux
  xdg-open "$OUTPUT_DIR" >/dev/null 2>&1 || true
elif command -v explorer.exe >/dev/null 2>&1; then
  # Windows (Git Bash / MSYS2 / Cygwin / WSL)
  if command -v wslpath >/dev/null 2>&1; then
    WIN_OUTPUT_FILE="$(wslpath -w "$OUTPUT_ZIP")"
    WIN_OUTPUT_DIR="$(wslpath -w "$OUTPUT_DIR")"
  elif command -v cygpath >/dev/null 2>&1; then
    WIN_OUTPUT_FILE="$(cygpath -w "$OUTPUT_ZIP")"
    WIN_OUTPUT_DIR="$(cygpath -w "$OUTPUT_DIR")"
  else
    # Fallback: try converting /c/... to C:\...
    if [[ "$OUTPUT_ZIP" =~ ^/([a-zA-Z])/(.*) ]]; then
      drive="${BASH_REMATCH[1]}"
      rest="${BASH_REMATCH[2]//\//\\}"
      WIN_OUTPUT_FILE="${drive^^}:\\${rest}"
      WIN_OUTPUT_DIR="${WIN_OUTPUT_FILE%\\*}"
    else
      WIN_OUTPUT_FILE="$OUTPUT_ZIP"
      WIN_OUTPUT_DIR="$OUTPUT_DIR"
    fi
  fi

  explorer.exe "${WIN_OUTPUT_DIR}" >/dev/null 2>&1 || true
elif command -v cmd.exe >/dev/null 2>&1; then
  # Another Windows fallback
  if command -v cygpath >/dev/null 2>&1; then
    WIN_OUTPUT_DIR="$(cygpath -w "$OUTPUT_DIR")"
    cmd.exe /c start "" explorer.exe "${WIN_OUTPUT_DIR}" >/dev/null 2>&1 || true
  else
    cmd.exe /c start "" explorer.exe "${OUTPUT_DIR}" >/dev/null 2>&1 || true
  fi
fi
