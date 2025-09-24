import csv, io, json, math, os, sys, time
from datetime import datetime, timedelta, timezone
import requests

# ------------ Config ------------
LAT0 = 37.7477
LON0 = -122.3020
UOM  = "cms"   # or "kts"
OUT_PATH = "assets/data/hf_point.json"
BASE = "https://hfradar.ndbc.noaa.gov/tabdownload.php"
TIMEOUT = 60
RETRIES = 5
RETRY_SLEEP = 6
TIERS = [
    (6, 24),   # hours, boxKm  ← try wider first to catch the 6 km grid
    (12, 24),
    (25, 24),
    (25, 36),
]
# --------------------------------

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
    last_text = ""
    for i in range(RETRIES + 1):
        try:
            r = requests.get(BASE, params=params, timeout=TIMEOUT,
                             headers={"Accept": "text/csv", "User-Agent": "Mozilla/5.0"})
            if r.status_code == 200 and r.text.strip():
                return r.text
            if r.status_code >= 500 or r.status_code == 429:
                time.sleep(RETRY_SLEEP*(i+1)); continue
            last_text = r.text
            break
        except requests.RequestException:
            time.sleep(RETRY_SLEEP*(i+1))
            continue
    return last_text

def parse_rows(text):
    if not text:
        return []
    # Keep only non-empty, non-comment lines
    lines = [ln.strip() for ln in text.splitlines() if ln.strip() and not ln.strip().startswith("#")]
    if len(lines) <= 1:
        return []
    rdr = csv.reader(lines)
    header = next(rdr, None)
    rows = []
    for row in rdr:
        if len(row) < 5: continue
        t, la, lo, u, v = row[:5]
        try:
            rows.append({"time": t, "lat": float(la), "lon": float(lo), "u": float(u), "v": float(v)})
        except ValueError:
            pass
    return rows

def write_json(obj):
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)

def load_existing():
    try:
        with open(OUT_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return None

def main():
    now = datetime.now(timezone.utc)
    end = round_down_hour(now)
    generated_at = datetime.now(timezone.utc).isoformat()

    # try each tier until we find data
    last_debug = {}
    for hours, box_km in TIERS:
        start = end - timedelta(hours=hours)
        from_str = start.strftime("%Y-%m-%d %H:00:00")
        to_str   = end.strftime("%Y-%m-%d %H:00:00")

        lat1, lon1, lat2, lon2 = build_bbox(LAT0, LON0, box_km)
        params = {
            "from": from_str, "to": to_str,
            "lat": f"{lat1}", "lng": f"{lon1}",
            "lat2": f"{lat2}", "lng2": f"{lon2}",
            "uom": UOM, "fmt": "csv"
        }
        url_preview = requests.Request('GET', BASE, params=params).prepare().url
        csv_text = fetch_csv(params)
        rows = parse_rows(csv_text)

        if rows:
            # nearest single grid cell (your requirement)
            nearest = min(rows, key=lambda r: haversine_m(LAT0, LON0, r["lat"], r["lon"]))
            u, v = nearest["u"], nearest["v"]
            speed = math.hypot(u, v)
            bearing = math.degrees(math.atan2(u, v)); 
            if bearing < 0: bearing += 360.0

            result = {
                "target": {"lat": LAT0, "lon": LON0},
                "nearest": nearest,  # includes time/lat/lon/u/v
                "from": from_str, "to": to_str,
                "hours": hours, "boxKm": box_km,
                "uom": UOM, "n": 1,
                "u": u, "v": v, "speed": speed, "bearing": bearing,
                "source_url": url_preview,
                "tier_used": {"hours": hours, "boxKm": box_km}
            }
            result["generated_at"] = generated_at
            write_json(result)
            return
        else:
            last_debug = {
                "target": {"lat": LAT0, "lon": LON0},
                "from": from_str, "to": to_str,
                "hours": hours, "boxKm": box_km,
                "uom": UOM, "n": 0,
                "error": "no rows",
                "source_url": url_preview
            }

    # If no tiers produced data, keep last good JSON if present
    existing = load_existing()
    if existing:
        existing["generated_at"] = generated_at
        write_json(existing)
    else:
        write_json(last_debug or {
            "target": {"lat": LAT0, "lon": LON0},
            "uom": UOM, "n": 0, "error": "no data",
            "generated_at": generated_at
        })


if __name__ == "__main__":
    main()
