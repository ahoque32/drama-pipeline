#!/bin/bash
# Screenshot wrapper script - Uses Camoufox via OpenClaw API
# Usage: screenshot.sh <url> <output_path> [wait_time]

URL="$1"
OUTPUT_PATH="$2"
WAIT_TIME="${3:-3}"

if [ -z "$URL" ] || [ -z "$OUTPUT_PATH" ]; then
    echo "Usage: $0 <url> <output_path> [wait_time]"
    exit 1
fi

# Ensure output directory exists
mkdir -p "$(dirname "$OUTPUT_PATH")"

echo "[screenshot.sh] Capturing: $URL"
echo "[screenshot.sh] Output: $OUTPUT_PATH"
echo "[screenshot.sh] Wait time: ${WAIT_TIME}s"

# Check if we can use OpenClaw's camofox tools
# This script is called from within OpenClaw environment
# The actual screenshot will be triggered via the OpenClaw API

# For now, create a marker file indicating screenshot is needed
# The main assethunter.py will handle this via OpenClaw tools

MARKER_FILE="${OUTPUT_PATH}.screenshot_pending"
cat > "$MARKER_FILE" << EOF
{
  "url": "$URL",
  "output_path": "$OUTPUT_PATH",
  "wait_time": $WAIT_TIME,
  "requested_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

echo "[screenshot.sh] Created pending marker: $MARKER_FILE"
echo "[screenshot.sh] Note: Screenshot will be captured via OpenClaw Camoufox tools"

# Try to use playwright/puppeteer if available
if command -v npx &> /dev/null; then
    echo "[screenshot.sh] Attempting screenshot via npx playwright..."
    
    # Create a temporary node script
    TEMP_SCRIPT=$(mktemp /tmp/screenshot_XXXXXX.js)
    cat > "$TEMP_SCRIPT" << 'NODE_SCRIPT'
const { chromium } = require('playwright');

(async () => {
    const url = process.argv[2];
    const outputPath = process.argv[3];
    const waitTime = parseInt(process.argv[4]) || 3000;
    
    try {
        const browser = await chromium.launch({ headless: true });
        const page = await browser.newPage();
        
        // Mobile viewport for Shorts format
        await page.setViewportSize({ width: 1080, height: 1920 });
        
        await page.goto(url, { waitUntil: 'networkidle', timeout: 30000 });
        await page.waitForTimeout(waitTime);
        
        await page.screenshot({ 
            path: outputPath,
            fullPage: false
        });
        
        await browser.close();
        console.log('Screenshot saved:', outputPath);
        process.exit(0);
    } catch (err) {
        console.error('Screenshot failed:', err.message);
        process.exit(1);
    }
})();
NODE_SCRIPT

    # Try to run the script
    if npx playwright --version &> /dev/null; then
        node "$TEMP_SCRIPT" "$URL" "$OUTPUT_PATH" "$((WAIT_TIME * 1000))"
        RESULT=$?
    else
        echo "[screenshot.sh] Playwright not available via npx"
        RESULT=1
    fi
    
    rm -f "$TEMP_SCRIPT"
    
    if [ $RESULT -eq 0 ] && [ -f "$OUTPUT_PATH" ]; then
        echo "[screenshot.sh] Screenshot captured successfully"
        rm -f "$MARKER_FILE"
        exit 0
    fi
fi

# Fallback: Check if puppeteer is available globally
if command -v node &> /dev/null; then
    echo "[screenshot.sh] Trying puppeteer..."
    
    TEMP_SCRIPT=$(mktemp /tmp/screenshot_XXXXXX.js)
    cat > "$TEMP_SCRIPT" << 'NODE_SCRIPT'
const puppeteer = require('puppeteer');

(async () => {
    const url = process.argv[2];
    const outputPath = process.argv[3];
    const waitTime = parseInt(process.argv[4]) || 3000;
    
    try {
        const browser = await puppeteer.launch({ headless: 'new' });
        const page = await browser.newPage();
        
        await page.setViewport({ width: 1080, height: 1920 });
        await page.goto(url, { waitUntil: 'networkidle2', timeout: 30000 });
        await page.waitForTimeout(waitTime);
        
        await page.screenshot({ 
            path: outputPath,
            fullPage: false
        });
        
        await browser.close();
        console.log('Screenshot saved:', outputPath);
        process.exit(0);
    } catch (err) {
        console.error('Screenshot failed:', err.message);
        process.exit(1);
    }
})();
NODE_SCRIPT

    node "$TEMP_SCRIPT" "$URL" "$OUTPUT_PATH" "$((WAIT_TIME * 1000))" 2>/dev/null
    RESULT=$?
    rm -f "$TEMP_SCRIPT"
    
    if [ $RESULT -eq 0 ] && [ -f "$OUTPUT_PATH" ]; then
        echo "[screenshot.sh] Screenshot captured successfully"
        rm -f "$MARKER_FILE"
        exit 0
    fi
fi

# If we get here, no screenshot tool was available
echo "[screenshot.sh] No screenshot tool available - manual capture required"
echo "[screenshot.sh] Please capture manually: $URL"
exit 1
