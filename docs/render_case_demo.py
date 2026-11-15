"""Render the README case from the checked release-restore demo result."""

import json
from html import escape
from pathlib import Path

HERE = Path(__file__).parent
RESULT = json.loads((HERE / "benchmarks" / "release-restore-20261109.json").read_text())
OLD = {row["category"]: row for row in RESULT["earlier_report"]}
NEW = {row["category"]: row for row in RESULT["current_report"]}
assert set(OLD) == set(NEW) == {"Accessories", "Displays"}
assert all(row["guidance_documents"] == 2 for row in [*OLD.values(), *NEW.values()])
OUT = HERE / "assets" / "case-demo.svg"
parts: list[str] = []


def rect(x, y, w, h, *, fill="#fff", stroke="#dbe4ef", radius=12):
    parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" '
                 f'fill="{fill}" stroke="{stroke}"/>')


def text(x, y, value, *, size=14, color="#24364f", weight=400, anchor="start"):
    parts.append(f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" '
                 f'font-weight="{weight}" text-anchor="{anchor}">{escape(str(value))}</text>')


def line(x1, y1, x2, y2, *, color="#93a8c2", marker=False, width=1.8):
    suffix = ' marker-end="url(#arrow)"' if marker else ""
    parts.append(f'<path d="M{x1} {y1} L{x2} {y2}" fill="none" stroke="{color}" '
                 f'stroke-width="{width}"{suffix}/>')


parts.append('<svg xmlns="http://www.w3.org/2000/svg" width="1220" height="562" '
             'viewBox="0 0 1220 562" role="img" aria-labelledby="title desc">')
parts.append('<title id="title">FluxPlane multi-source demo</title>')
parts.append('<desc id="desc">Orders from PostgreSQL, products from Parquet, refunds from HTTP, '
             'and policy documents feed a versioned business report. After an order and refund '
             'update, Accessories net amount changes from 23000 to 27000 cents. An incompatible '
             'source schema is rejected while the published result remains readable.</desc>')
parts.append('<defs><marker id="arrow" markerWidth="7" markerHeight="7" refX="6" '
             'refY="3.5" orient="auto"><path d="M0 0 L7 3.5 L0 7" fill="#93a8c2"/>'
             '</marker></defs>')
parts.append('<g font-family="DejaVu Sans, Arial, sans-serif">')
rect(0, 0, 1220, 562, fill="#fff", stroke="#fff", radius=0)
rect(24, 24, 8, 43, fill="#2563eb", stroke="#2563eb", radius=3)
text(48, 54, "One report, four source types", size=29, color="#162a44", weight=700)
text(48, 80, "A live report from databases, files, APIs, and documents", size=15, color="#64768e")

rect(24, 113, 274, 330, fill="#f8fafc")
text(44, 142, "INPUTS", size=11, color="#5d7290", weight=700)
for y, title, detail in [
    (160, "PostgreSQL", "Orders and customers"),
    (225, "Parquet", "Product categories"),
    (290, "HTTP JSON", "Refund records"),
    (355, "Markdown / PDF", "Return policy and FAQ"),
]:
    rect(44, y, 234, 53, stroke="#e0e7f0", radius=8)
    text(58, y + 23, title, size=14, weight=650)
    text(58, y + 42, detail, size=11, color="#64768e")
line(299, 277, 326, 277, marker=True)

rect(330, 113, 448, 330, fill="#edf4ff", stroke="#b6cef3")
text(350, 142, "FLUXPLANE", size=11, color="#42689e", weight=700)
for y, num, title, detail in [
    (163, "1", "Create versioned snapshots", "Each source enters a common data catalog"),
    (242, "2", "Refresh affected features", "Dependencies select the work to run"),
    (321, "3", "Publish a consistent release", "Queries bind to one set of input versions"),
]:
    rect(350, y, 408, 65, stroke="#c7d9f6", radius=8)
    rect(365, y + 15, 31, 31, fill="#dceafe", stroke="#c7d9f6", radius=8)
    text(380.5, y + 36, num, size=14, color="#2457a8", weight=700, anchor="middle")
    text(410, y + 27, title, size=14, weight=650)
    text(410, y + 47, detail, size=11, color="#64768e")
line(779, 277, 806, 277, marker=True)

rect(810, 113, 386, 330, fill="#f8fafc")
text(830, 142, "VISIBLE RESULT", size=11, color="#5d7290", weight=700)
text(830, 174, "Revenue after refunds", size=19, color="#162a44", weight=700)
text(830, 201, "Category", size=11, color="#64768e", weight=600)
text(1040, 201, "Before", size=11, color="#64768e", weight=600, anchor="end")
text(1172, 201, "After", size=11, color="#64768e", weight=600, anchor="end")
line(830, 212, 1176, 212, color="#d6e0ed", width=1)
for y, category in [(246, "Accessories"), (300, "Displays")]:
    old, new = OLD[category], NEW[category]
    text(830, y, category, size=14, weight=600)
    text(1040, y, f'{int(old["net_cents"]):,}', size=14, weight=600, anchor="end")
    text(1172, y, f'{int(new["net_cents"]):,}', size=14,
         color="#1a8b70" if old["net_cents"] != new["net_cents"] else "#24364f",
         weight=700, anchor="end")
    line(830, y + 13, 1176, y + 13, color="#e5ebf3", width=1)
text(830, 354, "Net amount in cents", size=11, color="#64768e")
rect(830, 375, 346, 43, fill="#eff9f7", stroke="#c5e6dc", radius=8)
text(845, 402, "2 policy references linked to the report", size=12,
     color="#277565", weight=600)

rect(24, 467, 1172, 78, fill="#f4f7fb")
text(44, 494, "THE ONLINE CHANGE", size=11, color="#5d7290", weight=700)
text(44, 520, "Seed release", size=13, weight=600)
line(156, 515, 185, 515, marker=True)
text(197, 520, "Orders + refunds update", size=13, weight=600)
line(389, 515, 418, 515, marker=True)
text(430, 520, "New release and refreshed report", size=13, weight=600)
line(704, 515, 733, 515, marker=True)
text(745, 520, "Bad schema rejected; report stays readable", size=13, weight=600)
parts.append('</g></svg>')
OUT.write_text("\n".join(parts) + "\n", encoding="utf-8")
print(OUT)
