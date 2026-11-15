"""Render the README blueprint, release walkthrough, and matching line icons."""

from html import escape
from pathlib import Path

ASSETS = Path(__file__).parent / "assets"
INK, MUTED, BLUE, TEAL = "#193454", "#59718e", "#3266da", "#178675"
ICONS = {
    "sources": '<ellipse cx="12" cy="5" rx="8" ry="3"/>'
    '<path d="M4 5v14c0 4 16 4 16 0V5M4 12c0 4 16 4 16 0"/>',
    "features": '<path d="M6 5v14m0-7h12V5m0 7v7"/>'
    '<rect x="3" y="2" width="6" height="6" rx="2"/>'
    '<rect x="15" y="2" width="6" height="6" rx="2"/>'
    '<rect x="3" y="16" width="6" height="6" rx="2"/>'
    '<rect x="15" y="16" width="6" height="6" rx="2"/>',
    "releases": '<path d="m3 7 9-5 9 5-9 5Zm0 5 9 5 9-5M3 17l9 5 9-5"/>',
    "query": '<path d="m8 5-6 7 6 7m8-14 6 7-6 7m-3-16-2 18"/>',
    "clock": '<circle cx="12" cy="12" r="9"/><path d="M12 6v6l4 2"/>',
    "file": '<path d="M5 2h9l5 5v15H5Zm9 0v6h5M8 12h8M8 16h8"/>',
    "shield": '<path d="m12 2 9 4v6c0 5-5 8-9 10-4-2-9-5-9-10V6Z"/>'
    '<path d="m7 12 3 3 7-7"/>',
    "pulse": '<path d="M1 13h5l3-9 5 17 3-8h6"/>',
}


class Canvas:
    def __init__(self, width, height, title, description):
        self.parts = [
            (f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
             f'viewBox="0 0 {width} {height}" role="img" aria-labelledby="title desc">'),
            (f'<title id="title">{escape(title)}</title><desc id="desc">'
             f'{escape(description)}</desc>'),
            ('<defs><pattern id="dots" width="24" height="24" patternUnits="userSpaceOnUse">'
            '<circle cx="1" cy="1" r="0.9" fill="#9bb3ce" opacity="0.22"/></pattern>'
            '<filter id="shadow" x="-20%" y="-30%" width="140%" height="180%">'
            '<feDropShadow dx="0" dy="3" stdDeviation="5" flood-color="#28476f" '
             'flood-opacity="0.07"/></filter></defs>'),
            '<g font-family="Trebuchet MS, DejaVu Sans, sans-serif">',
        ]
        self.box(1, 1, width - 2, height - 2, fill="#f8faff", radius=20)
        self.parts.append(f'<rect x="20" y="100" width="{width - 40}" '
                          f'height="{height - 120}" fill="url(#dots)"/>')

    def box(self, x, y, w, h, fill="#fff", stroke="#d7e2ef", radius=12, shadow=False):
        extra = ' filter="url(#shadow)"' if shadow else ""
        self.parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" '
                          f'rx="{radius}" fill="{fill}" stroke="{stroke}"{extra}/>')

    def text(self, x, y, value, size=14, color=INK, weight=400, anchor="start"):
        self.parts.append(f'<text x="{x}" y="{y}" font-size="{size}" fill="{color}" '
                          f'font-weight="{weight}" text-anchor="{anchor}">'
                          f'{escape(value)}</text>')

    def icon(self, x, y, kind, color=BLUE, scale=1):
        self.parts.append(f'<g transform="translate({x} {y}) scale({scale})" fill="none" '
                          f'stroke="{color}" stroke-width="1.7" stroke-linecap="round" '
                          f'stroke-linejoin="round">{ICONS[kind]}</g>')

    def line(self, d, color=BLUE, dashed=False, width=2):
        dash = ' stroke-dasharray="5 5"' if dashed else ""
        self.parts.append(f'<path d="{d}" fill="none" stroke="{color}" '
                          f'stroke-width="{width}" stroke-linejoin="round"{dash}/>')

    def arrow(self, x, y, direction="right", color=BLUE):
        d = f"M{x - 6} {y - 4}L{x} {y}L{x - 6} {y + 4}"
        if direction == "down":
            d = f"M{x - 4} {y - 6}L{x} {y}L{x + 4} {y - 6}"
        self.line(d, color)

    def title(self, eyebrow, title, subtitle):
        self.text(32, 31, eyebrow, 11, BLUE, 700)
        self.text(32, 65, title, 28, INK, 700)
        self.text(32, 89, subtitle, 13, MUTED)

    def save(self, name):
        target = ASSETS / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("\n".join([*self.parts, "</g></svg>"]) + "\n", encoding="utf-8")
        print(target)


def blueprint():
    c = Canvas(1280, 824, "FluxPlane system blueprint",
               "Structured data and document text feed background workers. A scheduler manages two "
               "pools. Queries read published snapshots; metadata and artifacts persist separately.")
    c.title("FLUXPLANE / SYSTEM BLUEPRINT", "Multimodal features, continuously maintained",
            "Structured records and document text, connected through features and versioned analytics.")
    c.box(304, 116, 656, 64, shadow=True)
    c.icon(324, 135, "query")
    c.text(364, 143, "Console / REST API / CLI", 17, weight=700)
    c.text(364, 164, "Submit SQL, synchronize data, refresh features, inspect runs", 12, MUTED)

    c.box(32, 202, 232, 369, shadow=True)
    c.text(52, 231, "01 / DATA SOURCES", 12, BLUE, 700)
    for y, kind, title, sub in [
        (252, "sources", "Databases", "PostgreSQL / SQLite"),
        (321, "file", "Files", "CSV / Parquet / JSON"),
        (390, "query", "HTTP APIs", "JSON + cursor / pagination"),
        (459, "file", "Documents", "Markdown / text-layer PDF"),
    ]:
        c.icon(52, y + 5, kind)
        c.text(88, y + 15, title, 15, weight=700)
        c.text(88, y + 35, sub, 10, MUTED)
    c.text(52, 546, "Adapters produce source snapshots", 11, MUTED)

    c.box(304, 202, 656, 154, fill="#edf3ff", stroke="#bdcfee")
    c.text(324, 230, "02 / ORCHESTRATION", 12, BLUE, 700)
    for x, kind, title, sub in [
        (324, "features", "Dependencies", "Persistent task graph"),
        (532, "clock", "Scheduler", "Source + time triggers"),
        (740, "shield", "Admission", "CPU / memory budgets"),
    ]:
        c.box(x, 247, 200, 88)
        c.icon(x + 14, 262, kind)
        c.text(x + 47, 279, title, 15, weight=700)
        c.text(x + 14, 315, sub, 12, MUTED)
    c.line("M632 180V202")
    c.arrow(632, 200, "down")
    for x in (466, 798):
        c.line(f"M{x} 356V408")
        c.arrow(x, 408, "down")
    c.text(632, 382, "dispatch / lease / retry", 11, MUTED, anchor="middle")

    c.box(304, 408, 312, 156, fill="#eef8f5", stroke="#b7dcd2", shadow=True)
    c.box(648, 408, 312, 156, fill="#edf3ff", stroke="#bdcfee", shadow=True)
    c.icon(324, 429, "features", TEAL)
    c.text(360, 448, "Background pool", 18, weight=700)
    c.text(324, 484, "Sync / feature refresh / text operators", 12, MUTED)
    c.text(324, 508, "Materialization + layout maintenance", 12, MUTED)
    c.icon(668, 429, "query")
    c.text(704, 448, "Query pool", 18, weight=700)
    c.text(668, 484, "Read-only SQL over pinned snapshots", 12, MUTED)
    c.text(668, 508, "DuckDB execution + paginated results", 12, MUTED)
    c.text(324, 543, "Arrow / DuckDB / registered operators", 11, TEAL)
    c.text(668, 543, "Input versions fixed at submission", 11, BLUE)
    c.line("M264 529H304", TEAL)
    c.arrow(302, 529, color=TEAL)
    c.line("M460 564V591H804V566", MUTED, dashed=True)
    c.text(632, 614, "Idle background workers may help query execution", 11, MUTED,
           anchor="middle")

    c.box(1000, 202, 248, 362, shadow=True)
    c.text(1020, 231, "03 / VISIBLE OUTPUT", 12, BLUE, 700)
    for y, kind, title, sub in [
        (259, "releases", "Published releases", "Dataset + feature versions"),
        (345, "pulse", "Run observability", "Task state / CPU / RSS"),
        (431, "query", "Query results", "Rows / pages / input versions"),
    ]:
        c.icon(1020, y, kind, TEAL if kind == "releases" else BLUE)
        c.text(1056, y + 17, title, 14, weight=700)
        c.text(1020, y + 46, sub, 12, MUTED)
    c.line("M960 475H1000")
    c.arrow(997, 475)
    c.line("M960 279H1000", TEAL)
    c.arrow(997, 279, color=TEAL)
    c.text(966, 267, "publish", 9, TEAL)
    c.text(1020, 540, "Console + authenticated API", 12, MUTED)

    c.box(304, 650, 656, 115, fill="#fff", shadow=True)
    c.text(324, 677, "04 / DURABLE STATE", 12, BLUE, 700)
    c.icon(324, 701, "sources")
    c.text(360, 713, "Metadata catalog", 15, weight=700)
    c.text(360, 737, "PostgreSQL: tasks, leases, releases", 11, MUTED)
    c.icon(661, 701, "releases", TEAL)
    c.text(697, 713, "Immutable artifacts", 15, weight=700)
    c.text(697, 737, "Parquet: data, features, results", 11, MUTED)
    c.line("M460 564V575H284V708H304", TEAL)
    c.arrow(302, 708, color=TEAL)
    c.line("M804 650V634H980V524H960", TEAL)
    c.arrow(804, 650, "down", TEAL)
    c.text(989, 628, "read / write", 10, TEAL)
    c.icon(37, 708, "shield", MUTED)
    c.text(73, 720, "Schema + version checks", 11, MUTED)
    c.text(32, 748, "before publication", 11, MUTED)
    c.text(1020, 708, "Instance API roles", 12, weight=700)
    c.text(1020, 731, "Admin / operator / viewer", 11, MUTED)
    c.line("M32 783H1248", "#d7e2ef", width=1)
    c.text(32, 806, "Blue: requests and scheduling", 11, BLUE)
    c.text(316, 806, "Teal: data and publication", 11, TEAL)
    c.text(1248, 806, "Single-host deployment / SQLite catalog available for development",
           11, MUTED, anchor="end")
    c.save("system-blueprint.svg")


def lifecycle():
    c = Canvas(1280, 536, "FluxPlane release lifecycle",
               "An illustrative single-dataset update invalidates dependent features in the new "
               "release. Refresh publishes a matching feature snapshot. Existing queries keep "
               "their pinned release; new queries cannot silently mix stale features with new data.")
    c.title("FLUXPLANE / VERSIONED EXECUTION", "Data changes. Query inputs stay explicit.",
            "Illustrative version labels for one dataset and one dependent feature.")
    cards = [
        (32, "01 / QUERY SUBMITTED", "Release R0", "D1 + F1", "Q-old pins this version", BLUE),
        (348, "02 / SOURCE UPDATED", "Release R1", "D2 / feature pending",
         "Dependent refresh queued", "#a87324"),
        (664, "03 / FEATURE REFRESHED", "Release R2", "D2 + F2",
         "Inputs checked at publication", TEAL),
        (980, "04 / NEW QUERY", "Q-new reads R2", "D2 + F2",
         "Matching published inputs", BLUE),
    ]
    for x, label, title, payload, detail, color in cards:
        c.box(x, 128, 268, 178, shadow=True)
        c.text(x + 18, 155, label, 10, color, 700)
        c.icon(x + 18, 172, "query" if x == 980 else "releases", color)
        c.text(x + 55, 193, title, 19, weight=700)
        c.box(x + 18, 211, 232, 36, fill="#f3f6fb", stroke="#e1e8f1", radius=7)
        c.text(x + 30, 235, payload, 14, color, 700)
        c.text(x + 18, 281, detail, 11, MUTED)
        if x < 980:
            c.line(f"M{x + 268} 216H{x + 311}", MUTED)
            c.arrow(x + 311, 216, color=MUTED)
    c.box(348, 326, 584, 52, fill="#fff8ed", stroke="#ead7b4")
    c.icon(365, 340, "clock", "#a87324")
    c.text(401, 348, "A pending feature is not substituted with its old snapshot.", 12,
           "#866026", 700)
    c.text(401, 367, "New queries requiring it receive an explicit stale-feature error.", 11,
           "#866026")
    c.line("M482 306V326", "#a87324", dashed=True)
    c.box(32, 405, 1216, 86, fill="#edf3ff", stroke="#c2d3f1")
    c.icon(51, 429, "query")
    c.text(88, 439, "Q-old continues on R0", 16, BLUE, 700)
    c.text(88, 466, "Its submitted input versions remain D1 + F1.", 12, MUTED)
    c.line("M453 448H955", BLUE)
    c.arrow(955, 448)
    c.box(979, 425, 248, 45, fill="#fff", stroke="#c2d3f1", radius=9)
    c.text(1103, 454, "Result from the pinned release", 11, BLUE, 700, anchor="middle")
    c.line("M164 306V405", BLUE, dashed=True)
    c.text(32, 519, "D = dataset snapshot    F = feature snapshot    R = published release",
           11, MUTED)
    c.save("release-lifecycle.svg")


def icons():
    for kind in ("sources", "features", "releases", "query", "shield", "pulse"):
        target = ASSETS / "icons" / f"{kind}.svg"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            '<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" '
            f'viewBox="0 0 32 32" role="img" aria-label="{kind}">'
            '<rect width="32" height="32" rx="8" fill="#edf3ff"/>'
            f'<g transform="translate(4 4)" fill="none" stroke="{BLUE}" '
            'stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">'
            f'{ICONS[kind]}</g></svg>\n', encoding="utf-8",
        )
        print(target)


if __name__ == "__main__":
    blueprint()
    lifecycle()
    icons()
