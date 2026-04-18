#!/usr/bin/env bash
# PreToolUse Bash hook: block `git push` to main/master if pytest fails.
#
# Wired in .claude/settings.json. Reads the tool call JSON on stdin, inspects
# the command, and runs pytest only when the push targets main/master.
# Exits 2 on failure so Claude Code blocks the push and surfaces stderr.

set -uo pipefail

INPUT="$(cat)"
CMD="$(printf '%s' "$INPUT" | jq -r '.tool_input.command // empty')"

# Not a Bash tool call with a command — nothing to do.
[[ -z "$CMD" ]] && exit 0

# Match `git push` as a whole word (after ^, whitespace, or chain separator),
# so `grep gitpush` or `git pushover` don't trigger the hook.
if ! printf '%s' "$CMD" | grep -qE '(^|[[:space:]&;|()])git[[:space:]]+push([[:space:]]|$)'; then
  exit 0
fi

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || true)"
[[ -z "$REPO_ROOT" ]] && exit 0
cd "$REPO_ROOT"

# Pull out everything after `git push` to inspect the refspec.
REST="$(printf '%s' "$CMD" | sed -E 's/.*git[[:space:]]+push[[:space:]]*//')"
read -r -a TOKENS <<< "$REST"

POS=()
for tok in "${TOKENS[@]}"; do
  case "$tok" in
    -*) ;;                  # skip flags (--force, -u, etc.)
    *) POS+=("$tok") ;;     # positional: remote, refspec
  esac
done

TARGET=""
REFSPEC="${POS[1]:-}"
if [[ -n "$REFSPEC" ]]; then
  # `src:dst` — check the destination branch
  DST="${REFSPEC##*:}"
  TARGET="${DST##refs/heads/}"
fi

# Bare `git push` or `git push origin HEAD` — resolve to current branch.
if [[ -z "$TARGET" || "$TARGET" == "HEAD" ]]; then
  TARGET="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
fi

case "$TARGET" in
  main|master) ;;
  *) exit 0 ;;              # not guarding non-default branches
esac

echo "[pre-push hook] Running pytest before push to '$TARGET'..." >&2

if [[ -f .venv/bin/activate ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
fi

# Skip tests/test_integration.py — uses an outdated URL and is superseded by
# tests/test_authenticated_column_lineage.py which exercises the live API.
if pytest --ignore=tests/test_integration.py -q; then
  echo "[pre-push hook] Tests passed — allowing push to '$TARGET'." >&2
  exit 0
fi

echo "[pre-push hook] Tests FAILED — blocking push to '$TARGET'." >&2
echo "[pre-push hook] Fix the failing tests, or push to a non-protected branch." >&2
exit 2
