import json
from pathlib import Path

"""
test parse done for lab to see if bob can function
"""

watermark_file = Path(__file__).parent / "watermarks.json"

data = json.loads(watermark_file.read_text())
products_updated_at = data.get("products_updated_at")

print(f"products_updated_at: {products_updated_at}")
