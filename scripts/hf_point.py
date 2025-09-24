import csv, io, json, math, os, sys, time
from datetime import datetime, timedelta, timezone
import requests

# ----- Config -----
# Target point (Alameda)
LAT0 = 37.7477
LON0 = -122.3020

# Grid spacing is 6 km; this box catches at least one cell
BOX_KM = 12

# Time window (UTC), keep small so CSV is fast but recent
HOURS = 6

# Units: 'cms' for cm/s, 'kts' for knots
UOM = "cms"

# Output path in repo (ensure directory exists)
OUT_PATH = "assets/data/hf_point.json"

# Upstream endpoint
BASE = "https://hfradar.ndbc.noaa.gov/tabdownload.php"

# HTTP timeouts + retries (server can be slow)
TIMEOUT = 60  # seconds
RETRIES = 5
RETRY_SLEEP = 6  # seconds
# ------------------

def round_down_hour(dt):
    return dt.replace(minute=0, second=0, microsecond=0)

def build_bbox(lat0, lon0, box_km):
    dlat = box_km / 111.0
    dlon = box_km / (111.0 * math.cos(math.radians(lat0)))
    return (lat0 - dlat, lon0 - dlon, lat0 + dlat, lon0 + dlon)

def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1 = math.radians(lat1); p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlon/2)**2
    return 2*R*math.asin(math.sqrt(a))

def fetch_csv(params):
    for i in range(RETRIES + 1):
        try:
            r = requests.get(BASE, params=params, timeout=TIMEOUT, headers={"Accept": "text/csv", "User-Agent": "Mozilla/5.0"})
            if r.status_code == 200 and r.text.strip():
                return r.text
            # retry on upstream errors
            if r.status_code >= 500 or r.status_code == 429:
                time.sleep(RETRY_SLEEP*(i+1))
                continue
            # non-retryable
            return ""
        except requests.RequestException:
            time.sleep(RETRY_SLEEP*(i+1))
            continue
    return ""

def parse_rows(text):
    # Strip comments / blanks; expect header: time,latitude,longitude,u,v
    lines = [ln.strip() for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    if len(lines) <= 1:
        return []
    rdr = csv.reader(lines)
    header = next(rdr, None)
    rows = []
    for row in rdr:
        if len(row) < 5: 
            continue
        t, la, lo, u, v = row[:5]
        try:
            rows.append({
                "time": t,
                "lat": float(la),
                "lon": float(lo),
                "u": float(u),
                "v": float(v)
            })
        except ValueError:
            pass
    return rows

def main():
    now = datetime.now(timezone.utc)
    to_dt = round_down_hour(now)
    from_dt = to_dt - timedelta(hours=HOURS)
    from_str = from_dt.strftime("%Y-%m-%d %H:00:00")
    to_str   = to_dt.strftime("%Y-%m-%d %H:00:00")

    lat1, lon1, lat2, lon2 = build_bbox(LAT0, LON0, BOX_KM)

    params = {
        "from": from_str,
        "to": to_str,
        "lat": f"{lat1}",
        "lng": f"{lon1}",
        "lat2": f"{lat2}",
        "lng2": f"{lon2}",
        "uom": UOM,
        "fmt": "csv"
    }

    csv_text = fetch_csv(params)
    out = {
        "target": {"lat": LAT0, "lon": LON0},
        "bbox": {"lat1": lat1, "lon1": lon1, "lat2": lat2, "lon2": lon2},
        "from": from_str, "to": to_str,
        "uom": UOM, "hours": HOURS,
        "n": 0
    }

    if not csv_text:
        # keep previous value if exists
        try:
            with open(OUT_PATH, "r", encoding="utf-8") as f:
                existing = json.load(f)
            # just touch the file to keep it committed
            with open(OUT_PATH, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)
            return
        except FileNotFoundError:
            with open(OUT_PATH, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2)
            return

    rows = parse_rows(csv_text)
    if not rows:
        # same keep-previous behavior
        try:
            with open(OUT_PATH, "r", encoding="utf-8") as f:
                existing = json.load(f)
            with open(OUT_PATH, "w", encoding="utf-8") as f:
                json.dump(existing, f, indent=2)
            return
        except FileNotFoundError:
            with open(OUT_PATH, "w", encoding="utf-8") as f:
                json.dump(out, f, indent=2)
            return

    # nearest single cell across the window (as requested)
    nearest = min(rows, key=lambda r: haversine_m(LAT0, LON0, r["lat"], r["lon"]))
    u = nearest["u"]; v = nearest["v"]
    speed = math.hypot(u, v)
    bearing = math.degrees(math.atan2(u, v))
    if bearing < 0: bearing += 360.0

    result = {
        "target": {"lat": LAT0, "lon": LON0},
        "nearest": nearest,             # includes lat, lon, time, u, v
        "from": from_str, "to": to_str,
        "u": u, "v": v,
        "speed": speed,
        "bearing": bearing,
        "uom": UOM,
        "n": 1
    }

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

if __name__ == "__main__":
    main()
