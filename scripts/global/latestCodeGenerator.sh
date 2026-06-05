
#!/bin/bash

# Get the script's directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RELATIVE_ROOT="$(basename "$PROJECT_ROOT")"

# Output file goes in the same directory as the script
OUTPUT_FILE="$SCRIPT_DIR/latestCode.txt"
SCRIPT_NAME="$(basename "$0")"

# Define files to exclude (relative to project root)
declare -a EXCLUDE_FILES=(
    "$SCRIPT_NAME"
    "scripts/global/$(basename "$OUTPUT_FILE")"
)

# Initialize variables
APPEND_MODE=false
SCAN_DIR="$PROJECT_ROOT"
ROOT_ONLY=false          # <-- NEW
counter=1

# Parse arguments
while [ $# -gt 0 ]; do
    case "$1" in
        a)
            APPEND_MODE=true
            ;;
        r)
            ROOT_ONLY=true
            ;;
        e)
            ECHO_ONLY=true
            ;;
        *)
            CANDIDATE_PATH="${1#/}"                         
            ABS_PATH="$PROJECT_ROOT/$CANDIDATE_PATH"
            if [[ -d "$ABS_PATH" ]]; then
                SCAN_DIR="$ABS_PATH"
            elif [[ -f "$ABS_PATH" ]]; then
                TARGET_FILES+=("$ABS_PATH")
            elif [[ $1 =~ ^[0-9]+$ ]]; then
                counter=$1
            else
                echo "Error: Invalid argument '$1'"
                exit 1
            fi
            ;;
    esac
    shift
done

# Only reset output file if not in append mode
if [ "$APPEND_MODE" = false ]; then
    : > "$OUTPUT_FILE"
else
    # In append mode, find the last used counter value
    if [ -f "$OUTPUT_FILE" ]; then
        last_num=$(grep -oP '^\d+\.' "$OUTPUT_FILE" | tail -1 | cut -d. -f1)
        if [[ $last_num =~ ^[0-9]+$ ]]; then
            counter=$((last_num + 1))
        fi
    fi
fi

echo "Scanning directory: $SCAN_DIR"
echo "Summary output will be in: $RELATIVE_ROOT/scripts/global/$(basename "$OUTPUT_FILE")"
[ "$APPEND_MODE" = true ] && echo "Appending to existing file (starting from $counter)" || echo "Creating new file (starting from $counter)"
[ "$ROOT_ONLY" = true ] && echo "Root-only scan (non-recursive)"              # <-- NEW
echo

cd "$PROJECT_ROOT" || exit 1

# Temp git repo for ignore checks using .codegenignore only (no .gitignore)
IGNORE_CHECK_DIR="$(mktemp -d)"
trap 'rm -rf "$IGNORE_CHECK_DIR"' EXIT
git -C "$IGNORE_CHECK_DIR" init -q

# Read directory patterns (ending with /) from .codegenignore so find can prune them (don’t descend)
PRUNE_DIRS=()
if [ -f "$SCRIPT_DIR/.codegenignore" ]; then
    while IFS= read -r line || [ -n "$line" ]; do
        line="${line%%$'\r'}"

        # Trim leading/trailing whitespace
        line="${line#"${line%%[![:space:]]*}"}"
        line="${line%"${line##*[![:space:]]}"}"

        # Skip blank lines, comments, and negations
        [ -z "$line" ] && continue
        case "$line" in
            \#*) continue ;;
            !*)  continue ;;
        esac

        # Directory-only patterns end with a trailing slash
        case "$line" in
            */)
                dirpat="${line%/}"
                # Strip a leading slash (anchor marker)
                if [[ "$dirpat" == /* ]]; then
                    dirpat="${dirpat#/}"
                fi

                # If pattern contains a slash, treat as project-root relative; otherwise match anywhere
                if [[ "$dirpat" == *"/"* ]]; then
                    PRUNE_DIRS+=("$PROJECT_ROOT/$dirpat")
                else
                    PRUNE_DIRS+=("*/$dirpat")
                fi
                ;;
        esac
    done < "$SCRIPT_DIR/.codegenignore"
fi


# Build find command based on root-only choice
if [ "$ROOT_ONLY" = true ]; then
    FIND_CMD=(find "$SCAN_DIR" -maxdepth 1 -type f ! -path "*/.git/*" -print0)   # <-- NEW
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
        FIND_CMD=(find "$SCAN_DIR" -type f ! -path "*/.git/*" -print0)               # <-- NEW
    fi
fi

while IFS= read -r -d '' file; do
    # Get relative path from project root
    REL_PATH="${file#$PROJECT_ROOT/}"

    # Skip excluded files
    skip_file=false
    for excluded in "${EXCLUDE_FILES[@]}"; do
    # Explicitly skip all files under node_modules
    if [[ "$REL_PATH" == node_modules/* ]]; then skip_file=true; fi
        if [[ "$REL_PATH" == "$excluded" ]]; then
            skip_file=true
            break
        fi
    done

    if $skip_file; then
        continue
    fi

    # Skip files matched by .codegenignore, regardless of Git tracking status
    if [ -f "$SCRIPT_DIR/.codegenignore" ]; then
        if git -C "$IGNORE_CHECK_DIR" -c core.excludesfile="$SCRIPT_DIR/.codegenignore" check-ignore -q --no-index "$REL_PATH"; then
            continue
        fi
    fi

    {
        echo "----------------------------------------------"
        echo "$counter.  $REL_PATH"
        echo "----------------------------------------------"
        echo ""

        if [ -s "$file" ]; then
            cat "$file"
        else
            echo "[EMPTY FILE]"
        fi

        echo
        echo
    } >> "$OUTPUT_FILE"

    ((counter++))
done < <("${FIND_CMD[@]}")   # <-- NEW: use the dynamic find command

echo "Project summary written to: $RELATIVE_ROOT/scripts/global/$(basename "$OUTPUT_FILE")"

# Best-effort: open the folder that contains the output file in a new window using the OS default application
OUTPUT_DIR="$(cd "$(dirname "$OUTPUT_FILE")" 2>/dev/null && pwd)"
if command -v open >/dev/null 2>&1; then
    # macOS (reveal the file to ensure the correct folder opens)
    open -R "$OUTPUT_FILE" >/dev/null 2>&1 || open "$OUTPUT_DIR" >/dev/null 2>&1 || true
elif command -v xdg-open >/dev/null 2>&1; then
    # Linux
    xdg-open "$OUTPUT_DIR" >/dev/null 2>&1 || true
elif command -v explorer.exe >/dev/null 2>&1; then
    # Windows (WSL / Git Bash / Cygwin): open the containing folder and select the output file
    if command -v wslpath >/dev/null 2>&1; then
        WIN_OUTPUT_FILE="$(wslpath -w "$OUTPUT_FILE")"
        WIN_OUTPUT_DIR="$(wslpath -w "$OUTPUT_DIR")"
    elif command -v cygpath >/dev/null 2>&1; then
        WIN_OUTPUT_FILE="$(cygpath -w "$OUTPUT_FILE")"
        WIN_OUTPUT_DIR="$(cygpath -w "$OUTPUT_DIR")"
    else
        # Fallback: try converting /mnt/c/... to C:\...
        if [[ "$OUTPUT_FILE" =~ ^/mnt/([a-zA-Z])/(.*) ]]; then
            drive="${BASH_REMATCH[1]}"
            rest="${BASH_REMATCH[2]//\//\\}"
            WIN_OUTPUT_FILE="${drive^^}:\\${rest}"
            WIN_OUTPUT_DIR="${drive^^}:\\${rest%\\*}"
        else
            WIN_OUTPUT_FILE="$OUTPUT_FILE"
            WIN_OUTPUT_DIR="$OUTPUT_DIR"
        fi
    fi
    explorer.exe "${WIN_OUTPUT_DIR}" >/dev/null 2>&1 || true
elif command -v cmd.exe >/dev/null 2>&1; then
    # Git Bash / Cygwin fallback: open the containing folder
    if command -v cygpath >/dev/null 2>&1; then
        WIN_OUTPUT_DIR="$(cygpath -w "$OUTPUT_DIR")"
        cmd.exe /c start "" explorer.exe "${WIN_OUTPUT_DIR}" >/dev/null 2>&1 || true
    else
        cmd.exe /c start "" explorer.exe "${OUTPUT_DIR}" >/dev/null 2>&1 || true
    fi
fi

