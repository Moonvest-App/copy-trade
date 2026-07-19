#!/bin/zsh
set -euo pipefail

PROJECT_DIR="${0:A:h:h}"
BUILD_VENV="$PROJECT_DIR/.build-arm64-venv"
BUILD_ROOT="$PROJECT_DIR/.pyinstaller-arm64"
BACKEND_DIST="$BUILD_ROOT/backend-dist"
DIST_DIR="$PROJECT_DIR/dist"
TARGET="$DIST_DIR/Moonvest.app"
BACKEND_NAME="Moonvest Backend"
ICON_SOURCE="$PROJECT_DIR/packaging/AppIcon.png"
PYTHON_SOURCE="/Library/Frameworks/Python.framework/Versions/3.13/bin/python3"

if [ ! -x "$PYTHON_SOURCE" ]; then
  PYTHON_SOURCE="$(command -v python3)"
fi

if [ ! -x "$BUILD_VENV/bin/python" ]; then
  arch -arm64 "$PYTHON_SOURCE" -m venv --system-site-packages "$BUILD_VENV"
fi

if ! "$BUILD_VENV/bin/python" -c 'import PyInstaller' 2>/dev/null; then
  "$BUILD_VENV/bin/python" -m pip install 'pyinstaller==6.21.0'
fi

ARCH="$($BUILD_VENV/bin/python -c 'import platform; print(platform.machine())')"
if [ "$ARCH" != "arm64" ]; then
  echo "ARM64 build requires an arm64 Python runtime; got $ARCH" >&2
  exit 1
fi

"$BUILD_VENV/bin/python" -c 'import pandas, moomoo' || {
  echo "Build Python cannot import pandas/moomoo; install requirements first." >&2
  exit 1
}

mkdir -p "$BUILD_ROOT/spec" "$BUILD_ROOT/work" "$BACKEND_DIST" "$DIST_DIR"

if [ ! -f "$ICON_SOURCE" ]; then
  echo "Missing app icon: $ICON_SOURCE" >&2
  exit 1
fi

# Build the Python service as an embedded helper. It never opens a browser when
# launched by the native shell.
arch -arm64 "$BUILD_VENV/bin/python" -m PyInstaller \
  --noconfirm \
  --clean \
  --console \
  --onedir \
  --name "$BACKEND_NAME" \
  --target-arch arm64 \
  --collect-data moomoo \
  --collect-submodules moomoo.common.pb \
  --exclude-module pandas.tests \
  --exclude-module torch \
  --exclude-module scipy \
  --exclude-module matplotlib \
  --exclude-module numba \
  --exclude-module pytest \
  --add-data "$PROJECT_DIR/opend_copytrader/static:opend_copytrader/static" \
  --paths "$PROJECT_DIR" \
  --distpath "$BACKEND_DIST" \
  --workpath "$BUILD_ROOT/work" \
  --specpath "$BUILD_ROOT/spec" \
  "$PROJECT_DIR/app.py"

STAGE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/moonvest-build.XXXXXX")"
trap 'rm -rf "$STAGE_ROOT"' EXIT
STAGE_APP="$STAGE_ROOT/Moonvest.app"
mkdir -p "$STAGE_APP/Contents/MacOS" "$STAGE_APP/Contents/Resources"

cp "$PROJECT_DIR/packaging/Info.plist" "$STAGE_APP/Contents/Info.plist"
ditto "$BACKEND_DIST/$BACKEND_NAME" "$STAGE_APP/Contents/Resources/backend"

# Build a macOS icon family from the user-provided source image.
ICONSET="$STAGE_ROOT/AppIcon.iconset"
mkdir -p "$ICONSET"
for ICON_SPEC in \
  "16 icon_16x16.png" \
  "32 icon_16x16@2x.png" \
  "32 icon_32x32.png" \
  "64 icon_32x32@2x.png" \
  "128 icon_128x128.png" \
  "256 icon_128x128@2x.png" \
  "256 icon_256x256.png" \
  "512 icon_256x256@2x.png" \
  "512 icon_512x512.png" \
  "1024 icon_512x512@2x.png"; do
  ICON_SIZE="${ICON_SPEC%% *}"
  ICON_NAME="${ICON_SPEC#* }"
  sips -z "$ICON_SIZE" "$ICON_SIZE" "$ICON_SOURCE" --out "$ICONSET/$ICON_NAME" >/dev/null
done
iconutil -c icns "$ICONSET" -o "$STAGE_APP/Contents/Resources/AppIcon.icns"

# Compile the visible application shell against macOS Cocoa and WebKit.
xcrun swiftc \
  -O \
  -target arm64-apple-macos12.0 \
  -framework Cocoa \
  -framework WebKit \
  "$PROJECT_DIR/packaging/MacApp.swift" \
  -o "$STAGE_APP/Contents/MacOS/Moonvest"

chmod +x \
  "$STAGE_APP/Contents/MacOS/Moonvest" \
  "$STAGE_APP/Contents/Resources/backend/$BACKEND_NAME"

# Finder metadata can invalidate a strict nested-code verification. Remove all
# extended attributes before applying the final ad-hoc signature.
xattr -cr "$STAGE_APP"
codesign --force --deep --sign - "$STAGE_APP"
codesign --verify --deep --strict "$STAGE_APP"

PREVIOUS="$BUILD_ROOT/Moonvest.previous.app"
rm -rf "$PREVIOUS"
if [ -d "$TARGET" ]; then
  mv "$TARGET" "$PREVIOUS"
fi
mv "$STAGE_APP" "$TARGET"

# Moving an .app into its final Finder-visible directory can attach FinderInfo
# again. Normalize and sign the final path as the last build operation.
xattr -cr "$TARGET"
codesign --force --deep --sign - "$TARGET"

# A file-provider workspace can immediately reattach FinderInfo to bundle
# directories. Verify an attribute-free private copy so the check reflects the
# app bytes and signature that will enter the release package.
VERIFY_APP="$STAGE_ROOT/Moonvest-verify.app"
ditto "$TARGET" "$VERIFY_APP"
xattr -cr "$VERIFY_APP"
codesign --verify --deep --strict "$VERIFY_APP"

echo "$TARGET"
