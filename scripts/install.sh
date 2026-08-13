#!/bin/sh
# relayfs installer — downloads the latest release binary, verifies its
# SHA-256 checksum, installs it into $PATH (default ~/.local/bin), and sets
# up PATH for new shells. No shell restart required for the current session.
#
#   curl -fsSL https://raw.githubusercontent.com/KumarDev7/relayfs/main/scripts/install.sh | sh
#
# Overrides:
#   RELAYFS_BIN_DIR  install directory (default: $HOME/.local/bin)
#   RELAYFS_VERSION  release tag to install, e.g. v0.1.0 (default: latest)
set -eu

[ -n "${HOME:-}" ] || { echo "error: HOME is not set" >&2; exit 1; }

REPO="KumarDev7/relayfs"
VERSION="${RELAYFS_VERSION:-latest}"
if [ "$VERSION" = "latest" ]; then
    BASE_URL="https://github.com/$REPO/releases/latest/download"
else
    BASE_URL="https://github.com/$REPO/releases/download/$VERSION"
fi

# --- platform detection -------------------------------------------------
OS="$(uname -s)"
ARCH="$(uname -m)"
case "$OS" in
    Linux) ;;
    Darwin)
        echo "error: no prebuilt macOS binary yet; build from source instead:" >&2
        echo "  cargo install --path ." >&2
        exit 1
        ;;
    *)
        echo "error: unsupported OS: $OS" >&2
        exit 1
        ;;
esac
case "$ARCH" in
    x86_64 | amd64) TARGET="x86_64-unknown-linux-gnu" ;;
    aarch64 | arm64) TARGET="aarch64-unknown-linux-gnu" ;;
    *)
        echo "error: unsupported architecture: $ARCH" >&2
        exit 1
        ;;
esac

# --- prerequisites ------------------------------------------------------
command -v curl >/dev/null 2>&1 || { echo "error: curl is required" >&2; exit 1; }
if command -v sha256sum >/dev/null 2>&1; then
    SHA="sha256sum"
elif command -v shasum >/dev/null 2>&1; then
    SHA="shasum -a 256"
else
    echo "error: sha256sum (or shasum) is required" >&2
    exit 1
fi

# --- download + verify --------------------------------------------------
BIN_DIR="${RELAYFS_BIN_DIR:-$HOME/.local/bin}"
mkdir -p "$BIN_DIR"

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

FILE="relayfs-$TARGET"
echo "Downloading $FILE (version $VERSION) ..."
curl -fsSL -o "$TMP/$FILE" "$BASE_URL/$FILE"
curl -fsSL -o "$TMP/sha256sums.txt" "$BASE_URL/relayfs-sha256sums.txt"

EXPECTED="$(grep -F "  $FILE" "$TMP/sha256sums.txt" | awk '{print $1}' || true)"
[ -n "$EXPECTED" ] || { echo "error: checksum not found for $FILE" >&2; exit 1; }
ACTUAL="$($SHA "$TMP/$FILE" | awk '{print $1}')"
if [ "$ACTUAL" != "$EXPECTED" ]; then
    echo "error: checksum mismatch for $FILE" >&2
    echo "  expected: $EXPECTED" >&2
    echo "  actual:   $ACTUAL" >&2
    exit 1
fi
echo "checksum OK"

install -m 0755 "$TMP/$FILE" "$BIN_DIR/relayfs"
echo "Installed $BIN_DIR/relayfs"
"$BIN_DIR/relayfs" --version

# --- PATH setup ---------------------------------------------------------
IN_PATH=0
case ":$PATH:" in
    *":$BIN_DIR:"*) IN_PATH=1 ;;
esac
if [ "$IN_PATH" = 0 ]; then
    LINE="export PATH=\"$BIN_DIR:\$PATH\""
    MARK="# added by relayfs installer"
    for RC in "$HOME/.bashrc" "$HOME/.zshrc" "$HOME/.profile"; do
        if [ -f "$RC" ] || [ "$RC" = "$HOME/.profile" ]; then
            if ! grep -qF "$MARK" "$RC" 2>/dev/null; then
                printf '\n%s\n%s\n' "$MARK" "$LINE" >>"$RC"
                echo "Added PATH export to $RC"
            fi
            break
        fi
    done
fi

echo
echo "relayfs installed successfully."
if [ "$IN_PATH" = 0 ]; then
    echo "To use it in THIS shell (no restart needed):"
    echo "  export PATH=\"$BIN_DIR:\$PATH\""
    echo "New shells pick it up automatically."
else
    echo "To use it in THIS shell (bash), refresh the command cache:"
    echo "  hash -r"
fi
echo "Get started: relayfs --help   (or   relayfs skill)"
