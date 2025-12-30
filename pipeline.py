# pipeline.py
# Cloud Run / Cloud Scheduler pipeline:
# - Reads new images from GCS: gs://<BUCKET>/incoming/
# - OCR via Google Vision API (DOCUMENT_TEXT_DETECTION)
# - Appends OCR transcript to gs://<BUCKET>/outputs/ocr_transcript_raw.txt
# - Appends parsed orders to gs://<BUCKET>/outputs/orders_output.csv
# - Rebuilds summary to gs://<BUCKET>/outputs/orders_summary.csv
# - Moves processed images to gs://<BUCKET>/processed/
#
# Env vars expected (Cloud Run):
#   BUCKET_NAME=instad-ocr-bucket
#   INCOMING_PREFIX=incoming/
#   PROCESSED_PREFIX=processed/
#   OUTPUT_PREFIX=outputs/
#   GOOGLE_CLOUD_PROJECT=<project>  (optional)
#
# Auth:
# - Runs on Cloud Run service account with permissions:
#     Storage Object Admin (or at least read incoming + write outputs + move to processed)
#     Vision API User (or roles/visionai.user equivalent; can also use a key but not needed)
#
# Dependencies in requirements.txt:
#   google-cloud-storage
#   google-cloud-vision
#
# Notes:
# - Preserves parse order (no sorting).
# - Customer delimiter: SOCIETY_NAMES + flat OR phone number (+91 95509 13705 etc.)
# - Drops timestamps / whatsapp noise from DESCRIPTION by cutting at first timestamp token inside an item chunk.
# - Uses sequential pairing for quantity+fruit (not nearest-distance pairing):
#     - builds a combined ordered event stream of qty spans and fruit spans
#     - pairs qty->next fruit when qty appears before fruit
#     - also supports fruit->qty when fruit appears before qty (simple, next qty)
# - DESCRIPTION is exact verbatim substring from OCR block corresponding to that item chunk.
#
# You WILL still want to tune aliases / society list as they evolve.

import os
import re
import csv
import io
from dataclasses import dataclass
from datetime import date
from typing import List, Dict, Tuple, Optional
from collections import defaultdict

from google.cloud import storage
from google.cloud import vision

# =========================================================
# CONFIG (GCS)
# =========================================================

BUCKET_NAME = os.environ.get("BUCKET_NAME", "").strip()
INCOMING_PREFIX = os.environ.get("INCOMING_PREFIX", "incoming/").strip()
PROCESSED_PREFIX = os.environ.get("PROCESSED_PREFIX", "processed/").strip()
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX", "outputs/").strip()

RAW_TRANSCRIPT_BLOB = f"{OUTPUT_PREFIX.rstrip('/')}/ocr_transcript_raw.txt"
ORDERS_CSV_BLOB = f"{OUTPUT_PREFIX.rstrip('/')}/orders_output.csv"
SUMMARY_CSV_BLOB = f"{OUTPUT_PREFIX.rstrip('/')}/orders_summary.csv"

# =========================================================
# SOCIETIES (EXHAUSTIVE LIST)
# =========================================================

SOCIETY_NAMES = {
    "ascentia","meda","krishvigavakshi","assetz","espana","t5","t6","t7","lotus",
    "t3","t2","t1","t4","villa","sunnyside","vars","vajram","iksha","ivy","jade",
    "lakefront","sls","dhavala","uberphase2","uberphase1","uberverdant","uv2",
    "t4c003"
}

# =========================================================
# ITEMS + ALIASES (trim/add as you like)
# =========================================================

ITEM_CONFIG = [
    {"name": "Nati guava", "unit": "kg"},
    {"name": "Guava white", "unit": "kg"},
    {"name": "Guava pink", "unit": "kg"},
    {"name": "Nagpur orange", "unit": "kg"},
    {"name": "Kinnow orange", "unit": "kg"},
    {"name": "Gala apple", "unit": "kg"},
    {"name": "Pomegranate medium", "unit": "kg"},
    {"name": "Pomegranate large", "unit": "kg"},
    {"name": "AppleBer", "unit": "kg"},
    {"name": "Strawberry", "unit": "qty"},
    {"name": "Blueberry", "unit": "qty"},
    {"name": "cherry", "unit": "qty"},
    {"name": "Longan", "unit": "kg"},
    {"name": "Pear", "unit": "kg"},
    {"name": "Sweet corn", "unit": "kg"},
    {"name": "Papaya", "unit": "kg"},
    {"name": "Dragon red", "unit": "kg"},
    {"name": "White dragon", "unit": "qty"},
    {"name": "Raspberry", "unit": "qty"},
    {"name": "Persimmon", "unit": "kg"},
    {"name": "Mulberry", "unit": "box"},
    {"name": "Yelakki banana", "unit": "kg"},
]

ITEM_UNIT_RAW: Dict[str, str] = {c["name"]: (c.get("unit") or "") for c in ITEM_CONFIG}

ALIASES = [
    # Apple
    {"alias": "apple", "canonical": "Gala apple"},
    {"alias": "gala", "canonical": "Gala apple"},
    {"alias": "gala apple", "canonical": "Gala apple"},

    # Guava
    {"alias": "guava", "canonical": "Guava white"},
    {"alias": "white guava", "canonical": "Guava white"},
    {"alias": "guava white", "canonical": "Guava white"},
    {"alias": "pink guava", "canonical": "Guava pink"},
    {"alias": "guava pink", "canonical": "Guava pink"},

    # Requested: desi guava variants -> Nati guava
    {"alias": "desi guava", "canonical": "Nati guava"},
    {"alias": "guava desi", "canonical": "Nati guava"},
    {"alias": "desi pink guava", "canonical": "Nati guava"},
    {"alias": "guava desi pink", "canonical": "Nati guava"},
    {"alias": "nati guava", "canonical": "Nati guava"},

    # Orange / Kinnow
    {"alias": "orange", "canonical": "Nagpur orange"},
    {"alias": "nagpur", "canonical": "Nagpur orange"},
    {"alias": "nagpur orange", "canonical": "Nagpur orange"},
    {"alias": "kinnow", "canonical": "Kinnow orange"},
    {"alias": "kino", "canonical": "Kinnow orange"},
    {"alias": "kinnow orange", "canonical": "Kinnow orange"},

    # Pomegranate
    {"alias": "pomegranate", "canonical": "Pomegranate medium"},
    {"alias": "pomegranate medium", "canonical": "Pomegranate medium"},
    {"alias": "medium pomegranate", "canonical": "Pomegranate medium"},
    {"alias": "pomegranate large", "canonical": "Pomegranate large"},
    {"alias": "large pomegranate", "canonical": "Pomegranate large"},
    {"alias": "big pomegranate", "canonical": "Pomegranate large"},

    # Ber
    {"alias": "ber", "canonical": "AppleBer"},
    {"alias": "apple ber", "canonical": "AppleBer"},
    {"alias": "appleber", "canonical": "AppleBer"},

    # Others
    {"alias": "strawberry", "canonical": "Strawberry"},
    {"alias": "strawberries", "canonical": "Strawberry"},
    {"alias": "blueberry", "canonical": "Blueberry"},
    {"alias": "blueberries", "canonical": "Blueberry"},
    {"alias": "cherry", "canonical": "cherry"},
    {"alias": "cherries", "canonical": "cherry"},
    {"alias": "sweet corn", "canonical": "Sweet corn"},
    {"alias": "corn sweet", "canonical": "Sweet corn"},
    {"alias": "raspberry", "canonical": "Raspberry"},
    {"alias": "raspberries", "canonical": "Raspberry"},
    {"alias": "red dragon", "canonical": "Dragon red"},
    {"alias": "dragon red", "canonical": "Dragon red"},
    {"alias": "white dragon", "canonical": "White dragon"},
    {"alias": "dragon white", "canonical": "White dragon"},
    {"alias": "longan", "canonical": "Longan"},
    {"alias": "pear", "canonical": "Pear"},
    {"alias": "papaya", "canonical": "Papaya"},
    {"alias": "persimmon", "canonical": "Persimmon"},
    {"alias": "mulberry", "canonical": "Mulberry"},
    {"alias": "yelakki", "canonical": "Yelakki banana"},
    {"alias": "yelakki banana", "canonical": "Yelakki banana"},
    {"alias": "banana", "canonical": "Yelakki banana"},
]

# =========================================================
# QUANTITY PARSING
# =========================================================

NUM_WORDS = {
    "one": 1.0, "two": 2.0, "three": 3.0, "four": 4.0, "five": 5.0,
    "half": 0.5, "quarter": 0.25,
}

WEIGHT_UNITS = {"kg","kgs","kilo","kilos","kilogram","kilograms","g","gm","gms","gr","gram","grams"}
PIECE_UNITS = {"pc","pcs","piece","pieces","no","nos","box","boxes","pack","packs","packet","packets","qty","q"}

BOX_QTY_ITEMS = {"Strawberry","Blueberry","Mulberry","White dragon","Papaya"}

PIECE_TO_KG: Dict[str, float] = {
    "Pomegranate medium": 0.25,
    "Pomegranate large": 0.31,
    "Pear": 0.20,
    "Gala apple": 0.175,
    "Guava pink": 0.40,
    "Guava white": 0.35,
    "AppleBer": 0.25,
    "Persimmon": 0.30,
}

# Timestamp hard-stop inside item description (remove WhatsApp times from description)
# Covers "11:39 AM", "9:06 AM", "7:20 PM", "10:02", etc.
TS_RE = re.compile(r"\b\d{1,2}:\d{2}\s*(?:am|pm)?\b", re.IGNORECASE)

# =========================================================
# BUILD ITEM INDEX
# =========================================================

def build_item_index(config_list, alias_list):
    index: Dict[str, Dict] = {}
    for item in config_list:
        key = item["name"].strip().lower()
        unit_raw = (item.get("unit") or "").strip().lower()
        unit = "kg" if unit_raw == "kg" else ("qty" if unit_raw in {"qty","box"} else "")
        index[key] = {"name": item["name"], "unit": unit}

    for a in alias_list:
        alias = a["alias"].strip().lower()
        canon = a["canonical"].strip()
        canon_key = canon.lower()
        if canon_key not in index:
            continue
        index[alias] = {"name": index[canon_key]["name"], "unit": index[canon_key]["unit"]}
    return index

ITEMS = build_item_index(ITEM_CONFIG, ALIASES)

ALL_ITEM_KEYS = sorted(list(ITEMS.keys()), key=len, reverse=True)

def phrase_pat(phrase: str) -> str:
    words = [re.escape(w) for w in phrase.split() if w]
    if not words:
        return ""
    return r"\b" + r"\s+".join(words) + r"\b"

FRUIT_NORM_RE = re.compile("|".join(phrase_pat(k) for k in ALL_ITEM_KEYS if k.strip()), re.IGNORECASE)

QTY_NORM_RE = re.compile(
    r"\b(?P<num>(?:one|two|three|four|five|half|quarter|\d+(?:\.\d+)?))\b"
    r"(?:\s+(?P<unit>kg|kgs|kilo|kilos|kilogram|kilograms|g|gm|gms|gr|gram|grams|"
    r"pc|pcs|piece|pieces|no|nos|box|boxes|pack|packs|packet|packets|qty|q))?\b",
    re.IGNORECASE
)

# =========================================================
# NORMALIZATION WITH RAW INDEX MAP
# (splits digit<->alpha transitions for "2piece", "1kg", etc.)
# plus special handling to avoid breaking society+flat tokens into descriptions.
# =========================================================

def normalize_with_map(raw: str) -> Tuple[str, List[int]]:
    out_chars: List[str] = []
    idx_map: List[int] = []

    def cls(ch: str) -> Optional[str]:
        if ch.isdigit():
            return "d"
        if ch.isalpha():
            return "a"
        return None

    prev_class = None

    for ri, ch in enumerate(raw):
        c = cls(ch)
        if c is None:
            if out_chars and out_chars[-1] != " ":
                out_chars.append(" ")
                idx_map.append(ri)
            prev_class = None
            continue

        # digit<->alpha transition -> insert space
        if prev_class and c != prev_class:
            if out_chars and out_chars[-1] != " ":
                out_chars.append(" ")
                idx_map.append(ri)

        out_chars.append(ch.lower())
        idx_map.append(ri)
        prev_class = c

    norm = "".join(out_chars)

    # compress spaces
    compressed, comp_map = [], []
    i = 0
    while i < len(norm):
        if norm[i] != " ":
            compressed.append(norm[i])
            comp_map.append(idx_map[i])
            i += 1
        else:
            j = i
            while j < len(norm) and norm[j] == " ":
                j += 1
            compressed.append(" ")
            comp_map.append(idx_map[i])
            i = j

    return "".join(compressed).strip(), comp_map

def raw_span_from_norm_span(comp_map: List[int], raw: str, ns: int, ne: int) -> Tuple[int, int]:
    if ns < 0: ns = 0
    if ne <= ns: ne = ns + 1
    if ns >= len(comp_map): return (0, 0)
    if ne > len(comp_map): ne = len(comp_map)
    raw_s = comp_map[ns]
    raw_e = comp_map[ne - 1] + 1
    raw_s = max(0, min(raw_s, len(raw)))
    raw_e = max(0, min(raw_e, len(raw)))
    return raw_s, raw_e

# =========================================================
# CUSTOMER SPLIT (society+flat OR phone)
# =========================================================

PHONE_NORM_RE = re.compile(r"(?:\+?91\s*)?\d{5}\s*\d{5}\b", re.IGNORECASE)

SOC_LIST = sorted(list(SOCIETY_NAMES), key=len, reverse=True)
SOC_ALT = "|".join(re.escape(s) for s in SOC_LIST)

# Allow:
# - "iksha g1402", "iksha g 1402", "iksha g-1402"
# - "uv2 c 204", "uberphase1 294"
# - "t4c003" as society itself (flat may be missing in some cases; handled below)
SOCIETY_FLAT_NORM_RE = re.compile(
    r"\b(?P<soc>(" + SOC_ALT + r"))\b\s+(?P<flat>[a-z]{0,4}\s*[-]?\s*\d{1,6})\b",
    re.IGNORECASE
)

def clean_flat(flat_norm: str) -> str:
    return re.sub(r"[\s\-]+", "", (flat_norm or "").upper())

def split_customer_blocks(raw_text: str) -> List[Tuple[str, str]]:
    norm, comp_map = normalize_with_map(raw_text)

    markers: List[Tuple[int, str]] = []

    for m in SOCIETY_FLAT_NORM_RE.finditer(norm):
        soc = m.group("soc").lower()
        flat = clean_flat(m.group("flat"))
        rs, _ = raw_span_from_norm_span(comp_map, raw_text, m.start(), m.start() + 1)
        markers.append((rs, f"{soc} {flat}"))

    for m in PHONE_NORM_RE.finditer(norm):
        digits = re.sub(r"\D", "", m.group(0))
        if digits.startswith("91") and len(digits) > 10:
            digits = digits[-10:]
        if len(digits) == 10:
            rs, _ = raw_span_from_norm_span(comp_map, raw_text, m.start(), m.start() + 1)
            markers.append((rs, f"phone {digits}"))

    if not markers:
        return []

    markers.sort(key=lambda x: x[0])

    blocks: List[Tuple[str, str]] = []
    for i, (rs, cid) in enumerate(markers):
        re_ = markers[i + 1][0] if i + 1 < len(markers) else len(raw_text)
        blk = raw_text[rs:re_].strip()
        if len(blk) < 3:
            continue
        blocks.append((cid, blk))

    return blocks

# =========================================================
# STEP 5/6 CORE: sequential qty<->fruit pairing + boundary
# =========================================================

@dataclass
class FruitSpan:
    ns: int
    ne: int
    matched: str
    canon: str
    base_unit: str

@dataclass
class QtySpan:
    ns: int
    ne: int
    matched: str
    val: float
    unit: str  # normalized

def parse_num(s: str) -> float:
    t = (s or "").strip().lower()
    if t in NUM_WORDS:
        return float(NUM_WORDS[t])
    try:
        return float(t)
    except Exception:
        return 0.0

def norm_unit(u: Optional[str]) -> str:
    x = (u or "").strip().lower()
    if x in {"kgs","kilo","kilos","kilogram","kilograms"}: return "kg"
    if x in {"gm","gms","gr","gram","grams"}: return "g"
    if x in {"pc","pcs","piece","pieces","no","nos"}: return "piece"
    if x in {"pack","packs","packet","packets"}: return "packet"
    if x in {"box","boxes"}: return "box"
    if x in {"qty","q"}: return "qty"
    if x in {"kg","g"}: return x
    return x

def extract_fruit_spans(norm: str) -> List[FruitSpan]:
    out: List[FruitSpan] = []
    for m in FRUIT_NORM_RE.finditer(norm):
        key = m.group(0).strip().lower()
        if key not in ITEMS:
            continue
        out.append(FruitSpan(
            ns=m.start(),
            ne=m.end(),
            matched=m.group(0).strip(),
            canon=ITEMS[key]["name"],
            base_unit=ITEMS[key]["unit"],
        ))
    return out

def extract_qty_spans(norm: str) -> List[QtySpan]:
    out: List[QtySpan] = []
    for m in QTY_NORM_RE.finditer(norm):
        num_s = m.group("num")
        unit_s = m.group("unit")
        out.append(QtySpan(
            ns=m.start(),
            ne=m.end(),
            matched=m.group(0).strip(),
            val=parse_num(num_s),
            unit=norm_unit(unit_s),
        ))
    return out

def qty_to_string_for_item(q: QtySpan, canon_item: str) -> str:
    # Infer missing unit
    unit = q.unit
    if not unit:
        unit = "qty" if canon_item in BOX_QTY_ITEMS else ""

    if unit == "g":
        # grams -> kg for kg items
        if ITEM_UNIT_RAW.get(canon_item, "").lower() == "kg" or canon_item not in BOX_QTY_ITEMS:
            return f"{(q.val/1000.0):.3f}".rstrip("0").rstrip(".")
        return str(int(q.val)) if float(q.val).is_integer() else str(q.val)

    if unit == "kg":
        return f"{q.val:.3f}".rstrip("0").rstrip(".")

    if unit == "piece":
        if canon_item in PIECE_TO_KG:
            return f"{(q.val * PIECE_TO_KG[canon_item]):.3f}".rstrip("0").rstrip(".")
        return str(int(q.val)) if float(q.val).is_integer() else str(q.val)

    if unit in {"box","packet","qty"}:
        return str(int(q.val)) if float(q.val).is_integer() else str(q.val)

    # unknown -> numeric only
    return str(int(q.val)) if float(q.val).is_integer() else str(q.val)

def cut_desc_at_timestamp(desc: str) -> str:
    d = desc.strip()
    m = TS_RE.search(d)
    if m:
        return d[:m.start()].strip()
    # also cut at common whatsapp noise words if they appear as standalone tokens
    for noise in ["unread messages", "edited", "joined using a group link"]:
        idx = d.lower().find(noise)
        if idx != -1:
            return d[:idx].strip()
    return d

def extract_items_from_customer_block(raw_block: str, customer_id: str, order_date: str) -> List[Dict[str, str]]:
    """
    Sequential pairing logic:
    - Build ordered events: qty spans and fruit spans by ns
    - Pair:
        qty -> next fruit (if qty occurs before fruit)
        fruit -> next qty (if fruit occurs before qty and qty is reasonably close)
      This supports mixed patterns: "1 box strawberries, pomegranate 2 piece, nagpur orange 1 kg"
    - Item chunk boundary:
        start = min(qty.ns if qty before fruit else fruit.ns)
        end   = next item start OR end of block
      Then we cut description at timestamp within the chunk.
    """
    norm, comp_map = normalize_with_map(raw_block)

    fruits = extract_fruit_spans(norm)
    qtys = extract_qty_spans(norm)
    if not fruits:
        return []

    events = []
    for i, f in enumerate(fruits):
        events.append(("fruit", i, f.ns))
    for j, q in enumerate(qtys):
        events.append(("qty", j, q.ns))
    events.sort(key=lambda x: x[2])

    used_fruits = set()
    used_qtys = set()
    pairs: List[Tuple[int, Optional[int]]] = []  # (fruit_idx, qty_idx)

    # Pass 1: qty -> next fruit
    k = 0
    while k < len(events):
        typ, idx, pos = events[k]
        if typ == "qty" and idx not in used_qtys:
            # find next fruit after this qty
            m = k + 1
            while m < len(events):
                typ2, idx2, _ = events[m]
                if typ2 == "fruit" and idx2 not in used_fruits:
                    used_qtys.add(idx)
                    used_fruits.add(idx2)
                    pairs.append((idx2, idx))
                    break
                m += 1
        k += 1

    # Pass 2: remaining fruits -> next qty (fruit-qty pattern)
    for fi, f in enumerate(fruits):
        if fi in used_fruits:
            continue
        # find next qty after fruit
        best_q = None
        for qj, q in enumerate(qtys):
            if qj in used_qtys:
                continue
            if q.ns >= f.ne:
                # simple: take first unused qty after fruit
                best_q = qj
                break
        used_fruits.add(fi)
        if best_q is not None:
            used_qtys.add(best_q)
        pairs.append((fi, best_q))

    # Compute item starts in normalized space
    item_starts = []
    for (fi, qj) in pairs:
        f = fruits[fi]
        start_ns = f.ns
        if qj is not None:
            q = qtys[qj]
            # if qty happens before fruit, start at qty
            if q.ne <= f.ns:
                start_ns = q.ns
        item_starts.append((start_ns, fi, qj))

    item_starts.sort(key=lambda x: x[0])

    rows: List[Dict[str, str]] = []

    for idx, (start_ns, fi, qj) in enumerate(item_starts):
        end_ns = item_starts[idx + 1][0] if idx + 1 < len(item_starts) else len(norm)

        # map to raw span
        start_raw, _ = raw_span_from_norm_span(comp_map, raw_block, start_ns, start_ns + 1)
        _, end_raw = raw_span_from_norm_span(comp_map, raw_block, max(start_ns + 1, end_ns - 1), end_ns)

        if end_raw <= start_raw:
            continue

        desc_verbatim = raw_block[start_raw:end_raw].strip()
        desc_verbatim = cut_desc_at_timestamp(desc_verbatim)
        if not desc_verbatim:
            continue

        canon_item = fruits[fi].canon

        qty_str = ""
        if qj is not None:
            qty_str = qty_to_string_for_item(qtys[qj], canon_item)

        # Keep only rows that have the fruit word in chunk (debug-safe)
        rows.append({
            "order_date": order_date,
            "customer_name": customer_id,
            "item_name": canon_item,
            "quantity": qty_str,
            "description": desc_verbatim,   # exact verbatim (trimmed at timestamp)
        })

    return rows

# =========================================================
# OCR (Vision) + GCS IO
# =========================================================

def ocr_image_bytes(img_bytes: bytes) -> str:
    client = vision.ImageAnnotatorClient()
    image = vision.Image(content=img_bytes)
    resp = client.document_text_detection(image=image)
    if resp.error.message:
        raise RuntimeError(resp.error.message)
    return resp.full_text_annotation.text or ""

def gcs_read_text(bucket: storage.Bucket, blob_name: str) -> str:
    b = bucket.blob(blob_name)
    if not b.exists():
        return ""
    return b.download_as_text(encoding="utf-8")

def gcs_append_text(bucket: storage.Bucket, blob_name: str, text: str):
    existing = gcs_read_text(bucket, blob_name)
    new = existing + ("" if existing.endswith("\n") or not existing else "\n") + text
    bucket.blob(blob_name).upload_from_string(new, content_type="text/plain; charset=utf-8")

def gcs_read_csv_rows(bucket: storage.Bucket, blob_name: str) -> List[Dict[str, str]]:
    b = bucket.blob(blob_name)
    if not b.exists():
        return []
    data = b.download_as_text(encoding="utf-8")
    r = csv.DictReader(io.StringIO(data))
    return list(r)

def gcs_append_orders_csv(bucket: storage.Bucket, blob_name: str, rows: List[Dict[str, str]]):
    if not rows:
        return

    b = bucket.blob(blob_name)
    fieldnames = ["order_date","customer_name","item_name","quantity","description"]

    if b.exists():
        existing = b.download_as_text(encoding="utf-8")
        out = io.StringIO()
        out.write(existing)
        # ensure newline
        if not existing.endswith("\n"):
            out.write("\n")
        w = csv.DictWriter(out, fieldnames=fieldnames)
        for rrow in rows:
            w.writerow({k: rrow.get(k, "") for k in fieldnames})
        b.upload_from_string(out.getvalue(), content_type="text/csv; charset=utf-8")
    else:
        out = io.StringIO()
        w = csv.DictWriter(out, fieldnames=fieldnames)
        w.writeheader()
        for rrow in rows:
            w.writerow({k: rrow.get(k, "") for k in fieldnames})
        b.upload_from_string(out.getvalue(), content_type="text/csv; charset=utf-8")

def gcs_write_summary(bucket: storage.Bucket, orders_blob: str, summary_blob: str):
    rows = gcs_read_csv_rows(bucket, orders_blob)
    totals = defaultdict(float)
    for r in rows:
        item = (r.get("item_name") or "").strip()
        qty = (r.get("quantity") or "").strip()
        if not item or not qty:
            continue
        try:
            totals[item] += float(qty)
        except Exception:
            continue

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["item_name","unit","total_quantity"])
    for item_name in sorted(totals.keys()):
        w.writerow([item_name, ITEM_UNIT_RAW.get(item_name, ""), f"{totals[item_name]:.3f}".rstrip("0").rstrip(".")])
    bucket.blob(summary_blob).upload_from_string(out.getvalue(), content_type="text/csv; charset=utf-8")

def move_blob(bucket: storage.Bucket, src_name: str, dst_name: str):
    src = bucket.blob(src_name)
    bucket.copy_blob(src, bucket, new_name=dst_name)
    src.delete()

# =========================================================
# MAIN PIPELINE
# =========================================================

def run_pipeline():
    if not BUCKET_NAME:
        raise RuntimeError("BUCKET_NAME env var is required.")

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)

    # list incoming images
    blobs = list(storage_client.list_blobs(BUCKET_NAME, prefix=INCOMING_PREFIX))
    images = [b for b in blobs if b.name.lower().endswith((".png",".jpg",".jpeg"))]

    # stable order: name (you can change to updated time if you want)
    images.sort(key=lambda b: b.name)

    order_date = date.today().isoformat()
    all_new_rows: List[Dict[str, str]] = []

    for b in images:
        img_name = b.name
        print("Processing:", img_name)

        img_bytes = b.download_as_bytes()
        ocr_text = ocr_image_bytes(img_bytes).strip()

        header = f"\n\n===== {os.path.basename(img_name)} =====\n"
        gcs_append_text(bucket, RAW_TRANSCRIPT_BLOB, header + (ocr_text + "\n" if ocr_text else "(no text)\n"))

        if ocr_text:
            blocks = split_customer_blocks(ocr_text)
            rows = []
            for customer_id, raw_block in blocks:
                rows.extend(extract_items_from_customer_block(raw_block, customer_id, order_date))

            if rows:
                gcs_append_orders_csv(bucket, ORDERS_CSV_BLOB, rows)
                all_new_rows.extend(rows)

        # move to processed/
        dst = PROCESSED_PREFIX.rstrip("/") + "/" + os.path.basename(img_name)
        move_blob(bucket, img_name, dst)

    # rebuild summary
    gcs_write_summary(bucket, ORDERS_CSV_BLOB, SUMMARY_CSV_BLOB)

    return {
        "new_rows": len(all_new_rows),
        "processed_images": len(images),
        "orders_csv": f"gs://{BUCKET_NAME}/{ORDERS_CSV_BLOB}",
        "summary_csv": f"gs://{BUCKET_NAME}/{SUMMARY_CSV_BLOB}",
        "raw_transcript": f"gs://{BUCKET_NAME}/{RAW_TRANSCRIPT_BLOB}",
    }

if __name__ == "__main__":
    result = run_pipeline()
    print(result)
