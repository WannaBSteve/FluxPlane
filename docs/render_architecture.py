"""Render the public system architecture as a self-contained SVG."""

from html import escape
from pathlib import Path

OUT = Path(__file__).parent / "assets" / "architecture.svg"
parts: list[str] = []


def rect(x, y, w, h, *, fill="#fff", stroke="#dbe4ef", radius=12):
    parts.append(
        f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" '
        f'fill="{fill}" stroke="{stroke}"/>'
    )


def label(x, y, value, *, size=14, color="#23354e", weight=400, anchor="start"):
    parts.append(
        f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" '
        f'font-weight="{weight}" text-anchor="{anchor}">{escape(value)}</text>'
    )


def path(d, *, color="#8da2bb", width=1.7, marker=None):
    suffix = f' marker-end="url(#{marker})"' if marker else ""
    parts.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}"{suffix}/>')


def pill(x, y, w, value, *, fill="#fff", stroke="#dbe4ef", color="#23354e"):
    rect(x, y, w, 38, fill=fill, stroke=stroke, radius=8)
    label(x + w / 2, y + 24, value, size=13, color=color, weight=550, anchor="middle")


def layer(y, h, index, title, detail, chips, *, fill="#f8fafc", stroke="#dbe4ef"):
    rect(250, y, 720, h, fill=fill, stroke=stroke)
    label(271, y + 29, index, size=11, color="#6b7f99", weight=700)
    label(271, y + 54, title, size=18, color="#162a44", weight=700)
    label(271, y + 76, detail, size=12, color="#64768e")
    path(f"M520 {y + 18} V{y + h - 18}", color=stroke, width=1)
    start, gap, total = 538, 8, 412
    width = (total - gap * (len(chips) - 1)) / len(chips)
    top = y + (h - 38) / 2
    for i, value in enumerate(chips):
        pill(start + i * (width + gap), top, width, value, stroke=stroke)


parts.append('<svg xmlns="http://www.w3.org/2000/svg" width="1220" height="875" '
             'viewBox="0 0 1220 875" role="img" aria-labelledby="title desc">')
parts.append('<title id="title">FluxPlane system architecture</title>')
parts.append('<desc id="desc">User requests and external sources enter the service and adapter layers. '
             'A durable control plane schedules query and background workers. The system returns query '
             'results and status, and publishes versioned datasets and features.</desc>')
parts.append('<defs><marker id="arrow" markerWidth="7" markerHeight="7" refX="6" '
             'refY="3.5" orient="auto"><path d="M0 0 L7 3.5 L0 7" fill="#8da2bb"/>'
             '</marker></defs>')
parts.append('<g font-family="DejaVu Sans, Arial, sans-serif">')
rect(0, 0, 1220, 875, fill="#fff", stroke="#fff", radius=0)
rect(24, 24, 8, 42, fill="#2563eb", stroke="#2563eb", radius=3)
label(48, 55, "FluxPlane", size=32, color="#162a44", weight=700)
label(48, 81, "Multi-source analytics with durable scheduling and versioned updates",
      size=15, color="#65788f")

layer(122, 96, "01 / INTERACT", "Console & API", "Submit work and inspect results",
      ["Web console", "REST API", "SSE / CLI"])
layer(234, 116, "02 / DECIDE", "Control plane", "Plan, schedule and publish",
      ["Dependencies", "Scheduler", "Publisher"], fill="#edf4ff", stroke="#b6cef3")
layer(366, 116, "03 / EXECUTE", "Worker runtime", "Query and background pools",
      ["SQL & features", "Documents", "Maintenance"], fill="#eff9f7", stroke="#b5ddd3")
layer(498, 96, "04 / CONNECT", "Source adapters", "Read source changes into snapshots",
      ["Databases", "Files", "HTTP", "Documents"])
layer(610, 116, "05 / PERSIST", "Durable state", "Tasks, versions and artifacts",
      ["Catalog", "Artifacts", "Release pointers"], fill="#f7f9fd")
for top, bottom in [(218, 234), (350, 366), (482, 498), (594, 610)]:
    path(f"M610 {top + 1} V{bottom - 3}", marker="arrow")

rect(24, 122, 202, 96, fill="#f8fafc")
label(43, 149, "REQUESTS", size=11, color="#5d7290", weight=700)
label(43, 176, "SQL · sync · refresh", size=14, weight=600)
label(43, 198, "from users or applications", size=11, color="#65788f")
path("M227 170 H247", marker="arrow")

rect(24, 498, 202, 228, fill="#f8fafc")
label(43, 525, "DATA SOURCES", size=11, color="#5d7290", weight=700)
for i, value in enumerate(["PostgreSQL / SQLite", "CSV / Parquet / JSON", "HTTP JSON APIs", "Text / Markdown / PDF"]):
    label(43, 559 + i * 37, value, size=13, weight=550)
path("M227 546 H247", marker="arrow")

rect(994, 122, 202, 96, fill="#f8fafc")
label(1013, 149, "RESPONSES", size=11, color="#5d7290", weight=700)
label(1013, 176, "Results & task status", size=13, weight=600)
label(1013, 198, "available in the console / API", size=11, color="#65788f")
path("M971 170 H991", marker="arrow")

rect(994, 366, 202, 116, fill="#f8fafc")
label(1013, 393, "RUNNING SIGNALS", size=11, color="#5d7290", weight=700)
label(1013, 421, "Queue & task duration", size=13, weight=600)
label(1013, 446, "CPU time & memory", size=13, weight=600)
path("M971 424 H991", marker="arrow")

rect(994, 610, 202, 116, fill="#f8fafc")
label(1013, 637, "PUBLISHED OUTPUT", size=11, color="#5d7290", weight=700)
label(1013, 665, "Dataset snapshots", size=13, weight=600)
label(1013, 690, "Features & releases", size=13, weight=600)
path("M971 668 H991", marker="arrow")

rect(250, 748, 720, 68, fill="#f3f6fb", stroke="#dbe4ef")
label(270, 775, "SHARED SERVICES", size=11, color="#5d7290", weight=700)
label(270, 798, "Access control     ·     Durable events     ·     Metrics and logs",
      size=14, color="#304862", weight=550)
label(24, 853, "Layers show responsibilities; requests, data and feedback move through them in both directions.",
      size=11, color="#687b91")
parts.append('</g></svg>')
OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text("\n".join(parts) + "\n", encoding="utf-8")
print(OUT)
