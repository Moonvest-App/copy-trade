#!/bin/zsh
set -euo pipefail

PROJECT_DIR="${0:A:h:h}"
APP_PATH="$PROJECT_DIR/dist/Moonvest.app"
INFO_PLIST="$APP_PATH/Contents/Info.plist"
GUIDE_PATH="$PROJECT_DIR/packaging/分享安装说明.txt"
RELEASE_DIR="$PROJECT_DIR/release"

if [ ! -d "$APP_PATH" ]; then
  echo "找不到 App，请先运行 ./scripts/build_macos_app.sh" >&2
  exit 1
fi

VERSION="$(/usr/libexec/PlistBuddy -c 'Print :CFBundleShortVersionString' "$INFO_PLIST")"
RELEASE_NAME="Moonvest-${VERSION}-macOS-arm64"
DMG_PATH="$RELEASE_DIR/$RELEASE_NAME.dmg"
ZIP_PATH="$RELEASE_DIR/$RELEASE_NAME.zip"
CHECKSUM_PATH="$RELEASE_DIR/$RELEASE_NAME-SHA256.txt"

MAIN_BINARY="$APP_PATH/Contents/MacOS/Moonvest"
BACKEND_BINARY="$APP_PATH/Contents/Resources/backend/Moonvest Backend"
if ! file "$MAIN_BINARY" "$BACKEND_BINARY" | grep -q 'arm64'; then
  echo "发布包不是 ARM64 构建" >&2
  exit 1
fi

SENSITIVE_FILES="$(find "$APP_PATH" -type f \( -name 'settings.json' -o -name '*credential*.json' -o -name '*.sqlite3' -o -name '*.sqlite3-wal' -o -name '*.sqlite3-shm' \) -print)"
if [ -n "$SENSITIVE_FILES" ]; then
  echo "发现不应进入分享包的本机数据：" >&2
  echo "$SENSITIVE_FILES" >&2
  exit 1
fi

mkdir -p "$RELEASE_DIR"
STAGE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/moonvest-release.XXXXXX")"
RW_DMG="$STAGE_ROOT/release-rw.dmg"
RW_MOUNT="$STAGE_ROOT/rw-mount"
cleanup() {
  hdiutil detach -quiet "$RW_MOUNT" 2>/dev/null || true
  rm -rf "$STAGE_ROOT"
}
trap cleanup EXIT

STAGE_DIR="$STAGE_ROOT/$RELEASE_NAME"
mkdir -p "$STAGE_DIR"
ditto "$APP_PATH" "$STAGE_DIR/Moonvest.app"
cp "$GUIDE_PATH" "$STAGE_DIR/分享安装说明.txt"
ln -s /Applications "$STAGE_DIR/Applications"
# The workspace may be managed by a file provider that reattaches FinderInfo
# to .app directories. The release-stage copy lives in a private temporary
# directory, so normalize and strictly verify that exact copy instead.
xattr -cr "$STAGE_DIR/Moonvest.app"
xattr -d com.apple.FinderInfo "$STAGE_DIR/Moonvest.app" 2>/dev/null || true
xattr -d com.apple.ResourceFork "$STAGE_DIR/Moonvest.app" 2>/dev/null || true
codesign --verify --deep --strict "$STAGE_DIR/Moonvest.app"

rm -f "$DMG_PATH" "$ZIP_PATH" "$CHECKSUM_PATH"
hdiutil create \
  -quiet \
  -fs HFS+ \
  -volname "Moonvest $VERSION" \
  -srcfolder "$STAGE_DIR" \
  -format UDRW \
  "$RW_DMG"

# Newer macOS versions may attach provenance attributes while populating an
# HFS image. Remove them inside the writable image so the installed app still
# passes strict code-signature verification.
mkdir -p "$RW_MOUNT"
hdiutil attach -quiet -nobrowse -readwrite -mountpoint "$RW_MOUNT" "$RW_DMG"
xattr -cr "$RW_MOUNT/Moonvest.app"
codesign --verify --deep --strict "$RW_MOUNT/Moonvest.app"
hdiutil detach -quiet "$RW_MOUNT"

hdiutil convert \
  -quiet \
  "$RW_DMG" \
  -format UDZO \
  -imagekey zlib-level=9 \
  -o "$DMG_PATH"

ditto -c -k --sequesterRsrc --keepParent "$STAGE_DIR/Moonvest.app" "$ZIP_PATH"

(
  cd "$RELEASE_DIR"
  shasum -a 256 "${DMG_PATH:t}" "${ZIP_PATH:t}" > "${CHECKSUM_PATH:t}"
)

echo "$DMG_PATH"
echo "$ZIP_PATH"
echo "$CHECKSUM_PATH"
