"""
Incendie monitor - backend de cache
FastAPI : proxy cache pour EFFIS WMS (disque), actualités RSS,
NASA FIRMS et Open-Meteo (mémoire), service du frontend.

Lancement :  uvicorn server:app --host 127.0.0.1 --port 8081
"""
import asyncio
import hashlib
import json
import os
import re
import time
import urllib.parse
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

ROOT = Path(__file__).parent
TILE_DIR = ROOT / "cache" / "tiles"
TILE_DIR.mkdir(parents=True, exist_ok=True)

# TTL en secondes
TTL_NEWS = 300        # actus : 5 min
TTL_FIRMS = 600       # foyers satellites : 10 min
TTL_METEO = 900       # météo : 15 min
TTL_TILES = 3 * 3600  # tuiles EFFIS : 3 h (données journalières)

EFFIS_UPSTREAM = "https://maps.effis.emergency.copernicus.eu/effis"
FR_BBOX = "-5.8,41.2,9.9,51.3"

NEWS_QUERIES = [
    '"feu de forêt" OR "feux de forêt" OR "incendie de forêt" when:2d',
    'incendie pompiers hectares when:2d',
    'SDIS incendie OR "sécurité civile" feu when:2d',
]


def firms_key() -> str:
    key = os.environ.get("FIRMS_KEY", "")
    if key:
        return key
    cfg = ROOT / "config.js"
    if cfg.exists():
        m = re.search(r"FIRMS_KEY\s*=\s*'([a-f0-9]+)'", cfg.read_text())
        if m:
            return m.group(1)
    return ""


app = FastAPI(title="incendie-monitor")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"]
)
client = httpx.AsyncClient(
    timeout=30, follow_redirects=True,
    headers={"User-Agent": "incendie-monitor/1.0 (dashboard OSINT feux de foret)"},
)

# cache mémoire : key -> (expiration, valeur) ; _last garde le dernier bon
# résultat pour servir du "stale" si l'amont tombe (pas de page cassée)
_mem: dict = {}
_last: dict = {}
_locks: dict = {}


# ---------------- protection des API amont ----------------
# Le public consomme le cache ; les fetchs amont déclenchés par des visiteurs
# sont plafonnés globalement (anti-flood). Les IP de confiance (localhost +
# env TRUSTED_IPS) ne sont pas limitées.
TRUSTED_IPS = {x.strip() for x in os.environ.get("TRUSTED_IPS", "").split(",")
               if x.strip()} | {"127.0.0.1", "::1"}
# budgets séparés : un débordement de tuiles (couche WMS activée pendant une
# panne EFFIS par ex.) ne doit pas affamer les petites API vitales
UPSTREAM_LIMITS = {"tiles": 60, "api": 30}   # fetchs amont/minute
_budgets: dict = {"tiles": [], "api": []}


def client_ip(request: Request) -> str:
    # derrière Cloudflare, la vraie IP client est dans CF-Connecting-IP
    return (request.headers.get("cf-connecting-ip")
            or request.headers.get("x-real-ip")
            or (request.client.host if request.client else ""))


def make_allow(request: Request, pool: str = "api"):
    """Retourne un callable évalué au moment du fetch amont uniquement."""
    def allow():
        if client_ip(request) in TRUSTED_IPS:
            return True
        now = time.time()
        budget = _budgets[pool]
        while budget and budget[0] < now - 60:
            budget.pop(0)
        if len(budget) >= UPSTREAM_LIMITS[pool]:
            return False
        budget.append(now)
        return True
    return allow


async def cached(key: str, ttl: int, fetch, allow=None):
    """Renvoie (valeur, état) avec état HIT / MISS / STALE pour l'en-tête X-Cache.
    Si allow() refuse le fetch amont : sert la dernière valeur connue, sinon
    LookupError (le endpoint répond 503)."""
    now = time.time()
    hit = _mem.get(key)
    if hit and hit[0] > now:
        return hit[1], "HIT"
    lock = _locks.setdefault(key, asyncio.Lock())
    async with lock:
        hit = _mem.get(key)
        if hit and hit[0] > now:
            return hit[1], "HIT"
        if allow is not None and not allow():
            if key in _last:
                return _last[key], "STALE"
            raise LookupError("budget amont épuisé, cache vide")
        try:
            val = await fetch()
            _mem[key] = (now + ttl, val)
            _last[key] = val
            return val, "MISS"
        except Exception:
            if key in _last:          # amont en panne : on sert l'ancien
                return _last[key], "STALE"
            raise


@app.get("/api/health")
async def health():
    return {"ok": True, "firms_key": bool(firms_key())}


# ---------------- actualités ----------------
async def _fetch_news():
    async def one(query: str):
        url = ("https://news.google.com/rss/search?q="
               + urllib.parse.quote(query)
               + "&hl=fr&gl=FR&ceid=FR:fr")
        r = await client.get(url)
        r.raise_for_status()
        items = []
        for it in ET.fromstring(r.content).iter("item"):
            title = it.findtext("title") or ""
            try:
                ts = parsedate_to_datetime(it.findtext("pubDate")).timestamp()
            except Exception:
                ts = time.time()
            items.append({
                "title": title,
                "link": it.findtext("link") or "#",
                "date": int(ts * 1000),
                "src": it.findtext("source") or "",
            })
        return items

    results = await asyncio.gather(*(one(q) for q in NEWS_QUERIES),
                                   return_exceptions=True)
    seen, merged = set(), []
    for res in results:
        if isinstance(res, Exception):
            continue
        for it in res:
            k = re.sub(r"\W+", " ", it["title"].lower())[:60]
            if k in seen:
                continue
            seen.add(k)
            merged.append(it)
    if not merged:
        raise RuntimeError("aucun flux disponible")
    merged.sort(key=lambda x: -x["date"])
    return merged[:60]


@app.get("/api/news")
async def news(request: Request):
    try:
        val, st = await cached("news", TTL_NEWS, _fetch_news, make_allow(request))
    except LookupError:
        return Response(status_code=503)
    return JSONResponse(val, headers={"X-Cache": st,
                                      "Cache-Control": "public, max-age=120"})


# ---------------- FIRMS ----------------
@app.get("/api/firms")
async def firms(request: Request):
    key = firms_key()
    if not key:
        return Response("clé FIRMS absente (env FIRMS_KEY)", status_code=503)

    async def fetch():
        url = (f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
               f"{key}/VIIRS_SNPP_NRT/{FR_BBOX}/2")
        r = await client.get(url)
        r.raise_for_status()
        return r.text

    try:
        val, st = await cached("firms", TTL_FIRMS, fetch, make_allow(request))
    except LookupError:
        return Response(status_code=503)
    return Response(val, media_type="text/csv",
                    headers={"X-Cache": st,
                             "Cache-Control": "public, max-age=300"})


def _coords(lats: str, lons: str, cap: int):
    """Valide et normalise les coordonnées : nombre borné, zone France élargie.
    Empêche de servir de proxy météo mondial, de gonfler le cache mémoire avec
    des clés arbitraires, et les 500 sur entrée non numérique."""
    try:
        la = [float(x) for x in lats.split(",")[:cap]]
        lo = [float(x) for x in lons.split(",")[:cap]]
    except ValueError:
        return None
    if not la or len(la) != len(lo):
        return None
    if not all(35 <= a <= 55 and -10 <= o <= 15 for a, o in zip(la, lo)):
        return None
    return (",".join(f"{a:.2f}" for a in la), ",".join(f"{o:.2f}" for o in lo))


# ---------------- météo ----------------
@app.get("/api/meteo")
async def meteo(request: Request, lats: str, lons: str):
    c = _coords(lats, lons, 24)
    if not c:
        return Response("coordonnées invalides", status_code=400)
    lats, lons = c

    async def fetch():
        url = ("https://api.open-meteo.com/v1/forecast"
               f"?latitude={lats}&longitude={lons}"
               "&current=temperature_2m,relative_humidity_2m,"
               "wind_speed_10m,wind_gusts_10m"
               "&hourly=temperature_2m,relative_humidity_2m,wind_gusts_10m"
               "&daily=precipitation_sum&forecast_days=5&timezone=Europe%2FParis")
        r = await client.get(url)
        r.raise_for_status()
        return r.text

    try:
        val, st = await cached(f"meteo:{lats}:{lons}", TTL_METEO, fetch,
                               make_allow(request))
    except LookupError:
        return Response(status_code=503)
    return Response(val, media_type="application/json",
                    headers={"X-Cache": st,
                             "Cache-Control": "public, max-age=600"})


# ---------------- qualité de l'air : grille fine ~25 km ----------------
# Le modèle CAMS derrière Open-Meteo a ~11 km de résolution : on échantillonne
# une grille limitée à la France métropolitaine (masque terre calculé une fois
# par point-dans-polygone sur le GeoJSON des départements).
# 0,25° ≈ 25 km : ~900 points. Open-Meteo limite à 600 localisations/min, donc
# les requêtes amont sont espacées (~100 s au total) et le endpoint sert la
# dernière grille connue pendant le rafraîchissement (stale-while-revalidate).
GRID_STEP = 0.25
TTL_AIRGRID = 3600
AIR_DATA_F = ROOT / "cache" / "airgrid_data.json"
_grid_pts = None
_grid_lock = asyncio.Lock()


async def france_grid():
    global _grid_pts
    if _grid_pts is not None:
        return _grid_pts
    async with _grid_lock:
        if _grid_pts is not None:
            return _grid_pts
        cache_f = ROOT / "cache" / f"airgrid_pts_{GRID_STEP}_v2.json"
        if cache_f.exists():
            _grid_pts = json.loads(cache_f.read_text())
            return _grid_pts
        r = await client.get(
            "https://raw.githubusercontent.com/gregoiredavid/france-geojson/"
            "master/departements-version-simplifiee.geojson")
        r.raise_for_status()
        polys = []
        for feat in r.json()["features"]:
            g = feat["geometry"]
            rings = [g["coordinates"]] if g["type"] == "Polygon" else g["coordinates"]
            for p in rings:
                outer = p[0]
                xs = [c[0] for c in outer]
                ys = [c[1] for c in outer]
                polys.append((outer, min(xs), min(ys), max(xs), max(ys)))

        def inside(lon, lat):
            for outer, minx, miny, maxx, maxy in polys:
                if lon < minx or lon > maxx or lat < miny or lat > maxy:
                    continue
                ok = False
                j = len(outer) - 1
                for i in range(len(outer)):
                    xi, yi = outer[i]
                    xj, yj = outer[j]
                    if (yi > lat) != (yj > lat) and \
                       lon < (xj - xi) * (lat - yi) / (yj - yi) + xi:
                        ok = not ok
                    j = i
                if ok:
                    return True
            return False

        # une maille est retenue si son centre OU un coin touche la terre :
        # sans les coins, les mailles côtières (centre en mer) laissaient des
        # trous le long du littoral
        half = GRID_STEP / 2
        probes = ((0, 0), (half, half), (half, -half), (-half, half), (-half, -half))
        pts = []
        lat = 41.2 + GRID_STEP / 2
        while lat < 51.3:
            lon = -5.8 + GRID_STEP / 2
            while lon < 9.9:
                if any(inside(lon + dx, lat + dy) for dx, dy in probes):
                    pts.append([round(lat, 2), round(lon, 2)])
                lon += GRID_STEP
            lat += GRID_STEP
        cache_f.write_text(json.dumps(pts))
        _grid_pts = pts
        return pts


async def _fetch_airgrid():
    pts = await france_grid()

    async def one(chunk):
        las = ",".join(f"{p[0]:.2f}" for p in chunk)
        los = ",".join(f"{p[1]:.2f}" for p in chunk)
        url = ("https://air-quality-api.open-meteo.com/v1/air-quality"
               f"?latitude={las}&longitude={los}"
               "&current=european_aqi,pm2_5,pm10,ozone")
        for attempt in (1, 2):
            r = await client.get(url)
            if r.status_code == 429 and attempt == 1:
                await asyncio.sleep(30)    # rate limit Open-Meteo : on retente
                continue
            r.raise_for_status()
            break
        j = r.json()
        return j if isinstance(j, list) else [j]

    # Open-Meteo compte chaque localisation comme un appel (limite 600/min) :
    # chunks de 100 espacés de 12 s -> ~500/min, rafraîchissement total ~100 s
    data = []
    for i in range(0, len(pts), 100):
        data.extend(await one(pts[i:i + 100]))
        if i + 100 < len(pts):
            await asyncio.sleep(12)
    cells = []
    for p, d in zip(pts, data):
        c = d.get("current") or {}
        if c.get("european_aqi") is None:
            continue
        cells.append({"lat": p[0], "lon": p[1],
                      "aqi": c["european_aqi"],
                      "pm25": c.get("pm2_5"), "pm10": c.get("pm10"),
                      "o3": c.get("ozone")})
    out = json.dumps({"step": GRID_STEP, "cells": cells})
    try:
        AIR_DATA_F.write_text(out)     # survit aux restarts du service
    except OSError:
        pass
    return out


async def _warm(key, ttl, fetch_fn):
    """Rafraîchit une entrée du cache en arrière-plan (un seul refresh à la fois)."""
    lock = _locks.setdefault(key, asyncio.Lock())
    if lock.locked():
        return
    async with lock:
        try:
            val = await fetch_fn()
            _mem[key] = (time.time() + ttl, val)
            _last[key] = val
        except Exception:
            pass


async def _swr(key, ttl, fetch_fn, disk_f):
    """Stale-while-revalidate : réponse immédiate avec la dernière donnée
    connue (mémoire ou disque), rafraîchissement en arrière-plan si périmée.
    Seul le tout premier chargement à froid est bloquant."""
    hdr = {"Cache-Control": "public, max-age=300"}
    hit = _mem.get(key)
    if hit and hit[0] > time.time():
        return Response(hit[1], media_type="application/json",
                        headers={"X-Cache": "HIT", **hdr})
    if key not in _last and disk_f.exists():
        try:
            _last[key] = disk_f.read_text()
        except OSError:
            pass
    if key in _last:
        asyncio.create_task(_warm(key, ttl, fetch_fn))
        return Response(_last[key], media_type="application/json",
                        headers={"X-Cache": "STALE", **hdr})
    val, st = await cached(key, ttl, fetch_fn)
    return Response(val, media_type="application/json",
                    headers={"X-Cache": st, **hdr})


@app.get("/api/airgrid")
async def airgrid():
    return await _swr("airgrid", TTL_AIRGRID, _fetch_airgrid, AIR_DATA_F)


# ---------------- prévision risque feu sur la même grille fine ----------------
TTL_PREVGRID = 2 * 3600
PREV_DATA_F = ROOT / "cache" / "prevgrid_data.json"


def _chandler(t, rh):
    return (((110 - 1.373 * rh) - 0.54 * (10.20 - t)) *
            (124 * 10 ** (-0.0142 * rh))) / 60


async def _fetch_prevgrid():
    pts = await france_grid()

    async def one(chunk):
        las = ",".join(f"{p[0]:.2f}" for p in chunk)
        los = ",".join(f"{p[1]:.2f}" for p in chunk)
        url = ("https://api.open-meteo.com/v1/forecast"
               f"?latitude={las}&longitude={los}"
               "&daily=temperature_2m_max,wind_gusts_10m_max,precipitation_sum"
               "&hourly=relative_humidity_2m"
               "&forecast_days=5&timezone=Europe%2FParis")
        for attempt in (1, 2):
            r = await client.get(url)
            if r.status_code == 429 and attempt == 1:
                await asyncio.sleep(30)
                continue
            r.raise_for_status()
            break
        j = r.json()
        return j if isinstance(j, list) else [j]

    # requêtes plus lourdes que l'air (5 jours de données) : pacing plus large
    data = []
    for i in range(0, len(pts), 100):
        data.extend(await one(pts[i:i + 100]))
        if i + 100 < len(pts):
            await asyncio.sleep(20)

    days, cells = [], []
    for p, d in zip(pts, data):
        dl, h = d.get("daily"), d.get("hourly") or {}
        if not dl or not dl.get("time"):
            continue
        days = days or dl["time"]
        rh_by_day = {}
        for t, v in zip(h.get("time", []), h.get("relative_humidity_2m", [])):
            if v is None:
                continue
            day = t[:10]
            if day not in rh_by_day or v < rh_by_day[day]:
                rh_by_day[day] = v
        per_day = []
        for di, day in enumerate(dl["time"]):
            t_max = dl["temperature_2m_max"][di]
            gust = dl["wind_gusts_10m_max"][di]
            rain = dl["precipitation_sum"][di]
            t_max = 15 if t_max is None else t_max
            gust = 0 if gust is None else gust
            rain = 0 if rain is None else rain
            rh_min = rh_by_day.get(day, 50)
            v = _chandler(t_max, rh_min) + max(0, (gust - 30) * 0.4)
            if rain >= 15:
                v -= 30
            elif rain >= 5:
                v -= 15
            per_day.append([round(v), round(t_max), round(rh_min),
                            round(gust), round(rain, 1)])
        cells.append({"lat": p[0], "lon": p[1], "d": per_day})
    out = json.dumps({"step": GRID_STEP, "days": days, "cells": cells})
    try:
        PREV_DATA_F.write_text(out)
    except OSError:
        pass
    return out


@app.get("/api/prevgrid")
async def prevgrid():
    return await _swr("prevgrid", TTL_PREVGRID, _fetch_prevgrid, PREV_DATA_F)


# ---------------- tuiles EFFIS (cache disque) ----------------
# disjoncteur : quand EFFIS est en panne, inutile de le marteler (et de brûler
# le budget amont) — après 5 échecs consécutifs, pause de 60 s
_effis_down = {"fails": 0, "until": 0.0}


def _effis_ok() -> bool:
    return time.time() >= _effis_down["until"]

def _effis_fail():
    _effis_down["fails"] += 1
    if _effis_down["fails"] >= 5:
        _effis_down["until"] = time.time() + 60
        _effis_down["fails"] = 0


@app.get("/wms/effis")
async def effis(request: Request):
    qs = str(request.url.query)
    # validation stricte : sans elle, n'importe qui peut générer des tuiles
    # uniques à volonté (remplissage disque + martèlement d'EFFIS)
    p = {k.lower(): v for k, v in request.query_params.items()}
    try:
        w, hh = int(p.get("width", "512")), int(p.get("height", "512"))
    except ValueError:
        return Response(status_code=400)
    if (len(qs) > 2000 or p.get("request", "").lower() != "getmap"
            or not 0 < w <= 1024 or not 0 < hh <= 1024):
        return Response(status_code=400)

    h = hashlib.sha1(qs.encode()).hexdigest()
    f = TILE_DIR / f"{h}.png"
    if f.exists() and time.time() - f.stat().st_mtime < TTL_TILES:
        return FileResponse(f, media_type="image/png",
                            headers={"X-Cache": "HIT"})

    def stale_or(status: int):
        if f.exists():
            return FileResponse(f, media_type="image/png",
                                headers={"X-Cache": "STALE"})
        return Response(status_code=status)

    if not _effis_ok():
        return stale_or(503)
    if not make_allow(request, "tiles")():
        return stale_or(503)
    try:
        r = await client.get(f"{EFFIS_UPSTREAM}?{qs}")
        r.raise_for_status()
        ctype = r.headers.get("content-type", "")
        if "image" not in ctype:   # erreur XML du serveur : ne pas cacher
            _effis_fail()
            return Response(r.content, media_type=ctype, status_code=502)
        _effis_down["fails"] = 0
        if len(r.content) <= 5_000_000:   # garde-fou disque
            f.write_bytes(r.content)
        return Response(r.content, media_type="image/png",
                        headers={"X-Cache": "MISS"})
    except httpx.HTTPError:
        _effis_fail()
        return stale_or(502)


# ---------------- entretien du cache disque ----------------
@app.on_event("startup")
async def _tile_gc():
    async def gc():
        while True:
            cutoff = time.time() - 24 * 3600
            for p in TILE_DIR.glob("*.png"):
                try:
                    if p.stat().st_mtime < cutoff:
                        p.unlink()
                except OSError:
                    pass
            await asyncio.sleep(3600)
    asyncio.create_task(gc())


# ---------------- frontend ----------------
@app.get("/")
async def index():
    return FileResponse(ROOT / "index.html")


@app.get("/favicon.png")
async def favicon():
    return FileResponse(ROOT / "static" / "favicon.png",
                        headers={"Cache-Control": "public, max-age=86400"})


@app.get("/apple-touch-icon.png")
async def touch_icon():
    return FileResponse(ROOT / "static" / "apple-touch-icon.png",
                        headers={"Cache-Control": "public, max-age=86400"})
