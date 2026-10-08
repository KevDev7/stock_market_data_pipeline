#!/bin/sh
# Export the editable source without rebuilding or changing its layout.
set -eu

diagram_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
if command -v drawio >/dev/null 2>&1; then
    diagram_cli=$(command -v drawio)
elif [ -x /Applications/draw.io.app/Contents/MacOS/draw.io ]; then
    diagram_cli=/Applications/draw.io.app/Contents/MacOS/draw.io
else
    echo "Draw.io Desktop is required for native exports." >&2
    exit 1
fi

diagram_source="$diagram_root/assets/stock-market-architecture.drawio"
for diagram_format in png svg; do
    diagram_export="$diagram_root/assets/stock-market-architecture.drawio.$diagram_format"
    "$diagram_cli" --export --format "$diagram_format" --embed-diagram \
        --border 20 --scale 1.5 --theme light --disable-gpu --timeout 45 \
        --output "$diagram_export" "$diagram_source"
    # Keep existing documentation links pointing at the same native export.
    cp "$diagram_export" "$diagram_root/assets/stock-market-architecture.$diagram_format"
done
