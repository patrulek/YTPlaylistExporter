#!/usr/bin/env python3
"""
yt_playlist_exporter.py
Export multiple YouTube playlists to a SINGLE CSV/TXT/MD file using yt-dlp.
Supports:
  - Multiple playlist URLs/IDs as positional arguments
  - Or: --list FILE with one playlist URL/ID per line
  - Modes: flat (fast), hybrid (parallel enrichment), full (rich but slow)
  - Optional deduplication by video ID

Requires: yt-dlp  (pip install yt-dlp)

Examples:
  python yt_playlist_exporter.py URL1 URL2 --mode flat -o out.md --format md
  python yt_playlist_exporter.py --list playlists.txt --mode hybrid --workers 8 -o all.csv --format csv --dedupe
  python yt_playlist_exporter.py URL --mode full --playlist-items 1-200 -o part.csv

Note:
- TXT/MD outputs are grouped with subheaders per playlist.
- CSV output includes extra columns: playlist_title, playlist_id.
"""
import argparse
import csv
import os
from urllib.parse import urlparse, parse_qs
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

try:
    import yt_dlp
except ImportError:
    raise SystemExit("Error: yt-dlp is not installed. Run: pip install yt-dlp")

def seconds_to_hms(seconds):
    if seconds in (None, ''):
        return ""
    try:
        return str(timedelta(seconds=int(seconds)))
    except Exception:
        return ""

def parse_playlist_id_from_url(u):
    try:
        q = parse_qs(urlparse(u).query)
        return (q.get("list") or [None])[0]
    except Exception:
        return None

def save_csv(path, rows):
    fieldnames = ["playlist_title", "playlist_id", "index", "title", "channel", "duration", "url", "id"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "playlist_title": r.get("playlist_title",""),
                "playlist_id": r.get("playlist_id",""),
                "index": r.get("index",""),
                "title": r.get("title",""),
                "channel": r.get("channel",""),
                "duration": r.get("duration",""),
                "url": r.get("url",""),
                "id": r.get("id",""),
            })

def save_txt(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        # group by playlist
        groups = defaultdict(list)
        for r in rows:
            groups[(r.get("playlist_title",""), r.get("playlist_id",""))].append(r)
        for (ptitle, pid), items in groups.items():
            header = ptitle or "Playlist"
            f.write(f"=== {header} ===\n\n")
            for r in items:
                line = f'{r["index"]}. {r["title"]} — {r.get("channel","")} {("(" + r["duration"] + ")") if r["duration"] else ""}\n{r["url"]}\n'
                f.write(line + ("\n" if not line.endswith("\n") else ""))

def save_md(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# YouTube Playlists Export\n\n")
        # group by playlist
        groups = defaultdict(list)
        for r in rows:
            groups[(r.get("playlist_title",""), r.get("playlist_id",""))].append(r)
        for (ptitle, pid), items in groups.items():
            header = ptitle or "Playlist"
            f.write(f"## {header}\n\n")
            for r in items:
                display = r["title"] or "Untitled"
                url = r["url"] or ""
                channel = r.get("channel", "")
                duration = f' ({r["duration"]})' if r["duration"] else ""
                if url:
                    f.write(f'{r["index"]}. [{display}]({url}) — {channel}{duration}\n')
                else:
                    f.write(f'{r["index"]}. {display} — {channel}{duration}\n')
            f.write("\n")

def write_output(fmt, out_path, rows):
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    if fmt == "csv":
        save_csv(out_path, rows)
    elif fmt == "txt":
        save_txt(out_path, rows)
    else:
        save_md(out_path, rows)
    print(f"✅ Saved {len(rows)} records in a file: {out_path}")

def extract_playlist_flat(playlist_url, cookies=None, playlist_items=None):
    """Returns (title, pid, rows) minimal fields, without expanding every video."""
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "ignoreerrors": True,
        "no_warnings": True,
        "extract_flat": True,
        "cachedir": True,
    }
    if cookies:
        if not os.path.exists(cookies):
            raise FileNotFoundError(f"Cookies file not found: {cookies}")
        ydl_opts["cookiefile"] = cookies
    if playlist_items:
        ydl_opts["playlist_items"] = playlist_items

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(playlist_url, download=False)
        if info is None:
            raise RuntimeError("Failed to fetch playlist info (flat).")
        title = info.get("title") or "Playlist"
        pid = info.get("id") or parse_playlist_id_from_url(playlist_url) or ""
        entries = info.get("entries") or []
        rows = []
        for i, e in enumerate(entries, start=1):
            if not e:
                rows.append({"index": i, "title": "[Unavailable]", "channel": "", "duration": "", "url": "", "id": ""})
                continue
            vid_id = e.get("id") or ""
            url = e.get("url") or e.get("webpage_url") or (f"https://www.youtube.com/watch?v={vid_id}" if vid_id else "")
            rows.append({
                "index": e.get("playlist_index") or i,
                "title": e.get("title") or "",
                "channel": e.get("uploader") or e.get("channel") or "",  # may be empty in flat mode
                "duration": seconds_to_hms(e.get("duration")) if e.get("duration") else "",
                "url": url,
                "id": vid_id,
            })
        return title, pid, rows

def enrich_one(url, cookies=None):
    """Fetch details for a single video (channel, duration)."""
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "ignoreerrors": True,
        "no_warnings": True,
        "extract_flat": False,
        "cachedir": True,
        "extractor_args": {
            "youtube": {
                "player_skip": ["configs", "webpage", "js"],
                "skip": ["translated_subs", "hls", "dash", "live_chat"],
            }
        },
    }
    if cookies:
        ydl_opts["cookiefile"] = cookies
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        try:
            v = ydl.extract_info(url, download=False)
        except Exception:
            return {"channel": "", "duration": ""}
        return {
            "channel": v.get("uploader") or v.get("channel") or "",
            "duration": seconds_to_hms(v.get("duration")) if v.get("duration") else "",
        }

def extract_playlist_full(playlist_url, cookies=None, playlist_items=None):
    """Full expansion (most accurate, slower). Returns (title, pid, rows)."""
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "ignoreerrors": True,
        "no_warnings": True,
        "extract_flat": False,
        "cachedir": True,
    }
    if cookies:
        ydl_opts["cookiefile"] = cookies
    if playlist_items:
        ydl_opts["playlist_items"] = playlist_items

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(playlist_url, download=False)
        if info is None:
            raise RuntimeError("Failed to fetch playlist info.")
        if "entries" not in info:
            entries = [info]
            title = info.get("title") or "Single video"
            pid = info.get("id") or parse_playlist_id_from_url(playlist_url) or ""
        else:
            entries = info.get("entries") or []
            title = info.get("title") or "Playlist"
            pid = info.get("id") or parse_playlist_id_from_url(playlist_url) or ""
        rows = []
        for i, entry in enumerate(entries, start=1):
            if not entry:
                rows.append({"index": i, "title": "[Unavailable]", "channel": "", "duration": "", "url": "", "id": ""})
                continue
            vid_id = entry.get("id") or ""
            url = entry.get("webpage_url") or (f"https://www.youtube.com/watch?v={vid_id}" if vid_id else "")
            rows.append({
                "index": entry.get("playlist_index") or i,
                "title": entry.get("title") or "",
                "channel": entry.get("uploader") or entry.get("channel") or "",
                "duration": seconds_to_hms(entry.get("duration")) if entry.get("duration") else "",
                "url": url,
                "id": vid_id
            })
        return title, pid, rows

def hybrid_enrich(rows, cookies=None, workers=6):
    """Fill missing channel/duration in parallel."""
    to_enrich = [(i, r) for i, r in enumerate(rows) if (not r.get("channel")) or (not r.get("duration"))]
    if not to_enrich:
        return rows
    futures = {}
    with ThreadPoolExecutor(max_workers=max(1, workers)) as ex:
        for idx, r in to_enrich:
            if not r.get("url"):
                continue
            futures[ex.submit(enrich_one, r["url"], cookies)] = idx
        for fut in as_completed(futures):
            idx = futures[fut]
            try:
                extra = fut.result()
            except Exception:
                extra = {"channel": "", "duration": ""}
            if "channel" in extra and extra["channel"] and not rows[idx].get("channel"):
                rows[idx]["channel"] = extra["channel"]
            if "duration" in extra and extra["duration"] and not rows[idx].get("duration"):
                rows[idx]["duration"] = extra["duration"]
    return rows

def read_playlist_inputs(args):
    items = []
    if args.playlists:
        items.extend(args.playlists)
    if args.list:
        if not os.path.exists(args.list):
            raise FileNotFoundError(f"List file not found: {args.list}")
        with open(args.list, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if s and not s.startswith("#"):
                    items.append(s)
    # de-dup inputs
    seen = set()
    uniq = []
    for it in items:
        if it not in seen:
            seen.add(it)
            uniq.append(it)
    return uniq

def main():
    p = argparse.ArgumentParser(description="Export multiple YouTube playlists to a single CSV/TXT/MD using yt-dlp.")
    p.add_argument("playlists", nargs="*", help="Playlist URLs or IDs (0..N). You may also use --list FILE.")
    p.add_argument("--list", help="Path to a text file with one playlist URL/ID per line")
    p.add_argument("-o", "--output", default=None, help="Output file path (defaults to playlists.csv/txt/md based on --format)")
    p.add_argument("--format", choices=["csv", "txt", "md"], default="csv", help="Output format (default: csv)")
    p.add_argument("--cookies", help="Path to a cookies.txt file (for private/unlisted content)")
    p.add_argument("--mode", choices=["flat", "hybrid", "full"], default="full", help="flat=fastest, hybrid=fast+details, full=most detailed")
    p.add_argument("--workers", type=int, default=6, help="Parallel requests in hybrid mode (default 6)")
    p.add_argument("--playlist-items", help="Range per playlist e.g. 1-200 or 1,5,10-20")
    p.add_argument("--dedupe", action="store_true", help="Remove duplicate videos by ID across playlists")
    args = p.parse_args()

    inputs = read_playlist_inputs(args)
    if not inputs:
        raise SystemExit("No playlists provided. Pass URLs/IDs or use --list FILE.")

    if args.output is None:
        base = "playlists"
        args.output = f"{base}.{args.format}"

    all_rows = []
    for src in inputs:
        try:
            if args.mode == "flat":
                title, pid, rows = extract_playlist_flat(src, cookies=args.cookies, playlist_items=args.playlist_items)
            elif args.mode == "hybrid":
                title, pid, rows = extract_playlist_flat(src, cookies=args.cookies, playlist_items=args.playlist_items)
                rows = hybrid_enrich(rows, cookies=args.cookies, workers=args.workers)
            else:
                title, pid, rows = extract_playlist_full(src, cookies=args.cookies, playlist_items=args.playlist_items)
        except Exception as e:
            print(f"⚠️  Skipping '{src}': {e}")
            continue

        # annotate rows with playlist metadata
        for r in rows:
            r["playlist_title"] = title
            r["playlist_id"] = pid
        all_rows.extend(rows)

    if args.dedupe:
        uniq = []
        seen_ids = set()
        for r in all_rows:
            vid = r.get("id", "")
            key = vid or r.get("url", "")  # fallback to url
            if key and key not in seen_ids:
                seen_ids.add(key)
                uniq.append(r)
        all_rows = uniq

    write_output(args.format, args.output, all_rows)

if __name__ == "__main__":
    main()
