"""Poll NHGIS extract #1 until it completes, then download + unpack."""
import time
import zipfile
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DATA.mkdir(exist_ok=True)

key = (Path.home() / ".nhgis_api_key").read_text().strip()
number = 1
url = f"https://api.ipums.org/extracts/{number}?collection=nhgis&version=v2"

while True:
    r = requests.get(url, headers={"Authorization": key}, timeout=60)
    r.raise_for_status()
    j = r.json()
    status = j["status"]
    print(f"[{time.strftime('%H:%M:%S')}] status={status}")
    if status == "completed":
        break
    if status == "failed":
        raise SystemExit(f"failed: {j}")
    time.sleep(20)

links = j["downloadLinks"]
print("download links:", list(links.keys()))
# Table data
td = links.get("tableData", {}).get("url")
print("  table url:", td)
r = requests.get(td, headers={"Authorization": key}, timeout=600)
r.raise_for_status()
out = DATA / "nhgis_extract.zip"
out.write_bytes(r.content)
print(f"saved {out} ({len(r.content):,} bytes)")

# Unpack
udir = DATA / "nhgis_unpacked"
udir.mkdir(exist_ok=True)
with zipfile.ZipFile(out) as z:
    z.extractall(udir)
    print("extracted:", [n for n in z.namelist()])
