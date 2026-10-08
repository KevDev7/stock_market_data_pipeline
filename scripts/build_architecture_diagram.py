"""Build an editable diagrams.net architecture and matching SVG preview.

The drawing follows the reference mentor's notation: dashed zones, an enclosing
platform, colored layer headers, labeled dataset boxes, policy text blocks,
shared capability bands, document-shaped category labels, and orientation arrows.
After rebuilding, export the editable source with
scripts/export_architecture_diagram.sh (see docs/architecture.md).
"""

from dataclasses import dataclass
from html import escape
from pathlib import Path
import xml.etree.ElementTree as ET


ROOT = Path(__file__).resolve().parents[1]
WIDTH, HEIGHT = 2500, 1160
INK = "#252525"
BLUE = "#dcecff"
BLUE_EDGE = "#7399bf"
DATA = "#dbe8f8"
YELLOW = "#fff4c8"
PALE_BORDER = "#bed0df"


ICON_CONTENT = {
    "database": '<ellipse cx="16" cy="6" rx="12" ry="4"/><path d="M4 6v20c0 5 24 5 24 0V6M4 13c0 5 24 5 24 0M4 20c0 5 24 5 24 0"/>',
    "table": '<rect x="3" y="4" width="26" height="24"/><path d="M3 11h26M12 4v24M21 4v24M3 19h26"/>',
    "view": '<path d="M2 16Q16 0 30 16Q16 32 2 16Z"/><circle cx="16" cy="16" r="5"/>',
    "file": '<path d="M7 2h13l7 7v21H7ZM20 2v8h7M11 15h12M11 20h12M11 25h9"/>',
    "api": '<circle cx="16" cy="16" r="13"/><ellipse cx="16" cy="16" rx="6" ry="13"/><path d="M3 16h26M5 9h22M5 23h22"/>',
    "bucket": '<ellipse cx="16" cy="6" rx="12" ry="4"/><path d="M4 6l3 22c2 3 16 3 18 0l3-22M8 16h16"/>',
    "code": '<path d="M10 7L2 16l8 9M22 7l8 9-8 9M19 4l-6 24"/>',
    "chart": '<path d="M3 3v26h27M8 24V15h5v9M17 24V8h5v16M26 24V3h4v21"/>',
    "snowflake": '<path d="M16 2v28M4 9l24 14M4 23L28 9M12 5l4 4 4-4M12 27l4-4 4 4M4 13l6-2-1-6M28 19l-6 2 1 6M9 27l1-6-6-2M23 5l-1 6 6 2"/>',
    "cloud": '<path d="M7 27h19a6 6 0 0 0 0-12 10 10 0 0 0-19-3 8 8 0 0 0 0 15Z"/>',
    "airflow": '<path d="M16 16L5 2 2 13ZM16 16L30 5 19 2ZM16 16L27 30 30 19ZM16 16L2 27 13 30Z"/>',
}


@dataclass
class Box:
    id: str
    x: float
    y: float
    w: float
    h: float
    lines: tuple = ()
    fill: str = "#ffffff"
    stroke: str = INK
    dashed: bool = False
    kind: str = "rectangle"
    font: int = 18
    bold: bool = False
    underline: bool = False
    icon: str = ""
    icon_color: str = INK
    icon_right: bool = False


class Diagram:
    def __init__(self):
        self.boxes = []
        self.edges = []

    def box(self, id, x, y, w, h, lines=(), **kwargs):
        box = Box(id, x, y, w, h, tuple(lines), **kwargs)
        self.boxes.append(box)
        return box

    def edge(self, id, points, source=None, target=None, label="", label_xy=None,
             dashed=False, color=INK, arrow=True, jump=False):
        self.edges.append(dict(id=id, points=points, source=source, target=target,
                               label=label, label_xy=label_xy, dashed=dashed,
                               color=color, arrow=arrow, jump=jump))

    @staticmethod
    def shape_path(box):
        x, y, w, h = box.x, box.y, box.w, box.h
        if box.kind == "document":
            return f"M{x},{y}H{x+w}V{y+h-12}C{x+w*.67},{y+h-34} {x+w*.33},{y+h+9} {x},{y+h-12}Z"
        if box.kind == "right-arrow":
            return f"M{x},{y+12}H{x+w-25}V{y}L{x+w},{y+h/2}L{x+w-25},{y+h}V{y+h-12}H{x}Z"
        if box.kind == "left-arrow":
            return f"M{x+w},{y+12}H{x+25}V{y}L{x},{y+h/2}L{x+25},{y+h}V{y+h-12}H{x+w}Z"
        return f"M{x},{y}H{x+w}V{y+h}H{x}Z"

    def svg(self):
        defs = "".join(f'<symbol id="icon-{name}" viewBox="0 0 32 32">{body}</symbol>'
                       for name, body in ICON_CONTENT.items())
        parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}" role="img" aria-labelledby="title desc">',
                 '<title id="title">Stock Market Pipeline — Five-Layer Data Warehouse Architecture</title>',
                 '<desc id="desc">Three Massive REST API datasets are pulled through shared extraction, archived in S3, and loaded into Snowflake RAW. STAGING, INTERMEDIATE, and MARTS prepare data for one Stock Market Dashboard BI Project.</desc>',
                 '<rect width="100%" height="100%" fill="#ffffff"/>',
                 f'<defs>{defs}<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10L3 5Z" fill="{INK}"/></marker></defs>']
        # Containers go behind all edges; components and labels cover edge runs.
        for box in self.boxes:
            if box.kind == "container":
                parts.append(self.svg_box(box))
        for edge in self.edges:
            coordinates = " ".join(f"{x},{y}" for x, y in edge["points"])
            dash = ' stroke-dasharray="6 5"' if edge["dashed"] else ""
            marker = ' marker-end="url(#arrow)"' if edge["arrow"] else ""
            parts.append(f'<polyline points="{coordinates}" fill="none" stroke="{edge["color"]}" stroke-width="1.5"{dash}{marker}/>')
        for box in self.boxes:
            if box.kind != "container":
                parts.append(self.svg_box(box))
        for edge in self.edges:
            if edge["label"]:
                x, y = edge["label_xy"]
                width = max(45, len(edge["label"]) * 8.3)
                parts.append(f'<rect x="{x-width/2}" y="{y-15}" width="{width}" height="20" fill="#ffffff"/>')
                parts.append(f'<text x="{x}" y="{y}" font-family="Arial, Helvetica, sans-serif" font-size="16" text-anchor="middle" fill="{INK}">{escape(edge["label"])}</text>')
        parts.append("</svg>")
        return "\n".join(parts)

    def svg_box(self, box):
        parts = []
        if box.kind not in ("text", "icon"):
            dash = ' stroke-dasharray="8 7"' if box.dashed else ""
            parts.append(f'<path d="{self.shape_path(box)}" fill="{box.fill}" stroke="{box.stroke}" stroke-width="1.2"{dash}/>')
        if box.icon:
            size = 28 if box.kind != "icon" else min(box.w, box.h)
            ix = box.x + box.w - size - 10 if box.icon_right else box.x + 10
            if box.kind == "icon":
                ix = box.x
            iy = box.y + (box.h - size) / 2
            parts.append(f'<use href="#icon-{box.icon}" x="{ix}" y="{iy}" width="{size}" height="{size}" fill="none" stroke="{box.icon_color}" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/>')
        if box.lines:
            line_height = box.font * 1.35
            cx = box.x + box.w / 2
            if box.icon and box.kind != "icon":
                cx += -18 if box.icon_right else 18
            top = box.y + box.h / 2 - (len(box.lines) - 1) * line_height / 2
            if box.kind == "document":
                top -= 5
            for i, line in enumerate(box.lines):
                weight = "600" if box.bold else "400"
                underline = ' text-decoration="underline"' if box.underline else ""
                parts.append(f'<text x="{cx}" y="{top+i*line_height}" dominant-baseline="middle" text-anchor="middle" font-family="Arial, Helvetica, sans-serif" font-size="{box.font}" font-weight="{weight}" fill="{INK}"{underline}>{escape(line)}</text>')
        return "\n".join(parts)

    def drawio(self):
        mxfile = ET.Element("mxfile", host="app.diagrams.net", type="device")
        page = ET.SubElement(mxfile, "diagram", id="stock-market-architecture", name="Four-layer architecture")
        model = ET.SubElement(page, "mxGraphModel", dx=str(WIDTH), dy=str(HEIGHT), grid="1", gridSize="10", guides="1", tooltips="1", connect="1", arrows="1", fold="1", page="1", pageScale="1", pageWidth=str(WIDTH), pageHeight=str(HEIGHT), background="#ffffff")
        root = ET.SubElement(model, "root")
        ET.SubElement(root, "mxCell", id="0")
        ET.SubElement(root, "mxCell", id="1", parent="0")
        for box in self.boxes:
            shape = {"document": "shape=document;", "right-arrow": "shape=singleArrow;", "left-arrow": "shape=singleArrow;direction=west;"}.get(box.kind, "shape=rectangle;")
            if box.kind in ("right-arrow", "left-arrow"):
                # Draw.io's default head scales with the entire arrow width.
                # Keep a compact 25-unit triangle, matching the SVG shape path.
                shape += f"arrowSize={25 / box.w};"
            if box.kind in ("text", "icon"):
                shape += "strokeColor=none;fillColor=none;"
            else:
                shape += f"fillColor={box.fill};strokeColor={box.stroke};"
            style = shape + f"rounded=0;whiteSpace=wrap;html=1;align=center;verticalAlign=middle;fontFamily=Arial;fontSize={box.font};fontColor={INK};strokeWidth=1.2;"
            if box.dashed:
                style += "dashed=1;dashPattern=8 7;"
            if box.bold or box.underline:
                style += f"fontStyle={(1 if box.bold else 0) + (4 if box.underline else 0)};"
            value = "<br>".join(escape(line) for line in box.lines)
            if box.icon and box.kind != "icon":
                # Reserve the same inset as the SVG so the text never hits the icon.
                style += "spacingRight=36;" if box.icon_right else "spacingLeft=36;"
            cell = ET.SubElement(root, "mxCell", id=box.id, value=value, style=style, vertex="1", parent="1")
            ET.SubElement(cell, "mxGeometry", x=str(box.x), y=str(box.y), width=str(box.w), height=str(box.h), attrib={"as": "geometry"})
            if box.icon:
                import urllib.parse
                size = 28 if box.kind != "icon" else min(box.w, box.h)
                ix = box.x + box.w - size - 10 if box.icon_right else box.x + 10
                if box.kind == "icon":
                    ix = box.x
                body = ICON_CONTENT[box.icon]
                icon_svg = f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><g fill="none" stroke="{box.icon_color}" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">{body}</g></svg>'
                image_uri = "data:image/svg+xml," + urllib.parse.quote(icon_svg)
                icon_cell = ET.SubElement(root, "mxCell", id=f"{box.id}-icon", style=f"shape=image;image={image_uri};imageAspect=0;", vertex="1", parent="1")
                ET.SubElement(icon_cell, "mxGeometry", x=str(ix), y=str(box.y + (box.h-size)/2), width=str(size), height=str(size), attrib={"as": "geometry"})
        for edge in self.edges:
            end_arrow = "classic" if edge["arrow"] else "none"
            attrs = dict(id=edge["id"], edge="1", parent="1", value="", style=f"edgeStyle=none;rounded=0;html=1;endArrow={end_arrow};endFill=1;strokeColor={edge['color']};strokeWidth=1.5;fontSize=16;fontFamily=Arial;")
            if edge["dashed"]:
                attrs["style"] += "dashed=1;dashPattern=6 5;"
            if edge["jump"]:
                # A bridge marks a crossing, not a data-flow junction.
                attrs["style"] += "jumpStyle=arc;jumpSize=7;"
            points = edge["points"]
            for endpoint, point in (("source", points[0]), ("target", points[-1])):
                if edge[endpoint]:
                    attrs[endpoint] = edge[endpoint]
                    box = next(b for b in self.boxes if b.id == edge[endpoint])
                    side = "exit" if endpoint == "source" else "entry"
                    attrs["style"] += f"{side}X={(point[0]-box.x)/box.w};{side}Y={(point[1]-box.y)/box.h};{side}Perimeter=0;"
            cell = ET.SubElement(root, "mxCell", **attrs)
            geometry = ET.SubElement(cell, "mxGeometry", relative="1", attrib={"as": "geometry"})
            ET.SubElement(geometry, "mxPoint", x=str(points[0][0]), y=str(points[0][1]), attrib={"as": "sourcePoint"})
            ET.SubElement(geometry, "mxPoint", x=str(points[-1][0]), y=str(points[-1][1]), attrib={"as": "targetPoint"})
            waypoints = ET.SubElement(geometry, "Array", attrib={"as": "points"})
            for x, y in points[1:-1]:
                ET.SubElement(waypoints, "mxPoint", x=str(x), y=str(y))
            if edge["label"]:
                x, y = edge["label_xy"]
                width = max(45, len(edge["label"]) * 8.3)
                label = ET.SubElement(root, "mxCell", id=f"{edge['id']}-label",
                                      value=edge["label"], vertex="1", parent="1",
                                      style="shape=rectangle;strokeColor=none;fillColor=#ffffff;align=center;verticalAlign=middle;fontFamily=Arial;fontSize=16;")
                ET.SubElement(label, "mxGeometry", x=str(x-width/2), y=str(y-15),
                              width=str(width), height="20", attrib={"as": "geometry"})
        # Match the SVG's stacking order: boundaries, routes, then components.
        # Arrow annotations are editable text cells at their explicit locations.
        containers = {b.id for b in self.boxes if b.kind == "container"}
        routes = {e["id"] for e in self.edges}
        labels = {f"{e['id']}-label" for e in self.edges if e["label"]}
        cells = list(root)
        def z_order(cell):
            id = cell.get("id")
            return (0 if id in ("0", "1") else 1 if id in containers
                    else 2 if id in routes else 4 if id in labels else 3)
        root[:] = sorted(cells, key=z_order)
        ET.indent(mxfile, space="  ")
        return ET.tostring(mxfile, encoding="unicode", xml_declaration=True)


def build_diagram():
    d = Diagram()
    # This overview groups all three API endpoints into one extraction capability.
    # Operational manifests/checkpoints remain implemented but are omitted here.
    d.box("source-zone", 150, 228, 205, 430, kind="container", fill="#ffffff", stroke=PALE_BORDER, dashed=True)
    d.box("ingest-zone", 400, 228, 265, 430, kind="container", fill="#ffffff", stroke=PALE_BORDER, dashed=True)
    d.box("warehouse", 706, 182, 1276, 830, kind="container", fill="#ffffff", stroke="#777777", dashed=True)
    d.box("consumer-zone", 2030, 228, 270, 365, kind="container", fill="#ffffff", stroke=PALE_BORDER, dashed=True)
    layers = [
        ("RAW", 740, 210, "#fcebd6", "#bf914b", "table"),
        ("STAGING", 980, 210, "#f4f4f4", "#888888", "view"),
        ("INTERMEDIATE", 1220, 210, "#e4eef5", "#7f9eb5", "table"),
        ("MARTS", 1460, 480, "#fff4cc", "#b8a051", "table"),
    ]
    for name, x, width, fill, stroke, icon in layers:
        d.box(f"layer-{name}", x, 250, width, 340, kind="container")
        d.box(f"header-{name}", x, 250, width, 46, [name], fill=fill, stroke=stroke, font=18, icon=icon)
        access = "Access" if name == "MARTS" else "No Access"
        d.box(f"usage-{name}", x, 218, width, 26, [access], kind="text", font=16)

    d.box("input-contract", 150, 35, 800, 72, ["DATA CONTRACT", "Sources → My Platform"], font=19)
    d.box("output-contract", 980, 35, 1320, 72, ["DATA CONTRACT", "My Platform → Consumers"], font=19)
    d.box("source-header", 150, 172, 205, 44, ["Source Layer"], fill=BLUE, stroke=BLUE_EDGE)
    d.box("ingest-header", 400, 172, 265, 44, ["Ingest"], fill="#f7f7f7", stroke="#777777")
    d.box("consumer-header", 2030, 172, 270, 44, ["Consumers"], fill=BLUE, stroke=BLUE_EDGE)
    d.box("snowflake-symbol", 710, 145, 42, 42, kind="icon", icon="snowflake", icon_color="#29a5dc")
    d.box("cloud-symbol", 767, 145, 45, 42, kind="icon", icon="cloud", icon_color="#3986b2")
    d.box("warehouse-title", 940, 181, 900, 29, ["Data Warehouse"], kind="text", font=20)
    d.box("batch-label", 20, 365, 110, 80, ["Batch", "Sources"], kind="document", fill=BLUE, stroke=BLUE_EDGE)

    d.box("massive-provider", 164, 254, 177, 386, kind="container")
    d.box("massive-title", 164, 266, 177, 27, ["Massive"], kind="text", bold=True, font=18)
    d.box("massive-subtitle", 164, 292, 177, 24, ["Market Data Provider"], kind="text", font=15)
    d.box("source-prices", 176, 330, 153, 68, ["Daily Stock", "Prices"], icon="file", font=16)
    d.box("source-catalog", 176, 438, 153, 68, ["Ticker", "Catalog"], icon="file", font=16)
    d.box("source-overview", 176, 546, 153, 68, ["Company", "Overview"], icon="file", font=16)
    d.box("extract-job", 417, 307, 230, 66, ["REST API Extraction"], icon="code", font=17)
    d.box("s3-archive", 417, 398, 230, 70, ["Amazon S3", "Raw NDJSON.gz archive"], icon="bucket", icon_color="#b75b23", font=17)
    d.box("copy-load", 417, 492, 230, 64, ["Snowflake COPY", "S3 stages → RAW"], icon="database", font=17)
    d.box("schedule", 410, 684, 245, 104, ["Daily Stock Prices +", "Ticker Catalog:", "Mon–Fri · 12:00 PM ET"], fill=YELLOW, stroke="#d4b970", font=16)
    d.box("overview-cadence", 410, 808, 245, 84, ["Company Overviews:", "Quarterly + first observed"], fill=YELLOW, stroke="#d4b970", font=16)

    # Boxes name logical datasets, not operations or a required table count.
    # Catalog and overview remain distinct datasets despite shared RAW storage.
    for prefix, x in (("raw", 756), ("stg", 996)):
        for suffix, y, label in (("catalog", 322, "Ticker Catalog"),
                                 ("overview", 394, "Company Overview"),
                                 ("stock", 464, "Stock Prices")):
            d.box(f"{prefix}-{suffix}", x, y, 178, 50, [label],
                  fill=DATA, stroke="#93a9c0", font=17)
    d.box("security-reference", 1236, 328, 178, 50, ["Security Reference"], fill=DATA, stroke="#93a9c0", font=17)
    d.box("price-history", 1236, 464, 178, 50, ["Price History"], fill=DATA, stroke="#93a9c0", font=17)
    d.box("observed-history", 1236, 540, 178, 40, ["Security History"], fill=DATA, stroke="#93a9c0", font=17)
    d.box("analytics-product", 1500, 355, 400, 225, ["Data Product", "Star Schema +", "Fact Constellation", "Stock Market Analytics"], fill=DATA, stroke="#93a9c0", font=17)

    policies = {
        "RAW": ["1:1 Copy", "No Transformations", "No Dimensional Modeling", "Tables", "Partial Overwrite"],
        "STAGING": ["Cleanup Transformations", "Rename", "Casting", "Deduplication", "No Cross-Source Joins", "No Enrichment", "No Dimensional Modeling", "Views"],
        "INTERMEDIATE": ["Cross-Source Joins", "Enrichment", "Shared Business Rules", "No Cleanup", "No Dimensional Modeling", "Tables", "Full/Partial Overwrite"],
        "MARTS": ["Analytical Calculations", "Dimensional Modeling", "Tables", "No Cleanup", "Full/Partial Overwrite"],
    }
    for name, x, width, *_ in layers:
        d.box(f"policy-title-{name}", x, 704, width, 28, ["Layer policies"], kind="text", font=18, bold=True, underline=True)
        lines = policies[name]
        line_height = 22
        # Individual text rows keep both SVG and diagrams.net easy to edit.
        for i, line in enumerate(lines):
            d.box(f"policy-{name}-{i}", x-5, 740+i*line_height, width+10, 22, [line], kind="text", font=16)

    d.box("stock-dashboard", 2038, 392, 254, 86, ["Stock Market Dashboard", "BI Project"], font=17, icon="chart", icon_right=True, icon_color="#ce5353")
    d.box("bi-label", 2340, 397, 137, 81, ["BI / Analytics", "Projects"], kind="document", fill=BLUE, stroke=BLUE_EDGE, font=17)

    # Three source datasets converge on one extractor; no source-to-COPY bypass.
    d.edge("price-pull", [(329, 364), (380, 364), (380, 340), (417, 340)], "source-prices", "extract-job", "Pull", (381, 324))
    d.edge("catalog-pull", [(329, 472), (380, 472), (380, 340), (417, 340)], "source-catalog", "extract-job")
    d.edge("overview-pull", [(329, 580), (380, 580), (380, 340), (417, 340)], "source-overview", "extract-job")
    d.edge("archive", [(532, 373), (532, 398)], "extract-job", "s3-archive")
    d.edge("archive-copy", [(532, 468), (532, 492)], "s3-archive", "copy-load")
    d.edge("raw-load", [(647, 524), (714, 524), (714, 489), (756, 489)], "copy-load", "raw-stock", "Load", (683, 512))
    d.edge("catalog-raw-load", [(647, 524), (714, 524), (714, 347), (756, 347)], "copy-load", "raw-catalog")
    d.edge("overview-raw-load", [(647, 524), (714, 524), (714, 419), (756, 419)], "copy-load", "raw-overview")
    for suffix, y in (("catalog", 347), ("overview", 419), ("stock", 489)):
        d.edge(f"{suffix}-standardization", [(934, y), (996, y)], f"raw-{suffix}", f"stg-{suffix}")
    d.edge("catalog-reference", [(1174, 347), (1206, 347), (1206, 342), (1236, 342)], "stg-catalog", "security-reference")
    d.edge("overview-reference", [(1174, 419), (1198, 419), (1198, 364), (1236, 364)], "stg-overview", "security-reference")
    d.edge("stock-enrichment", [(1174, 489), (1236, 489)], "stg-stock", "price-history")
    d.edge("reference-enrichment", [(1325, 378), (1325, 464)], "security-reference", "price-history")
    d.edge("analytical-modeling", [(1414, 489), (1500, 489)], "price-history", "analytics-product")
    d.edge("reference-attribute-history", [(1414, 353), (1422, 353), (1422, 550), (1414, 550)], "security-reference", "observed-history", jump=True)
    d.edge("accepted-history", [(1325, 514), (1325, 540)], "price-history", "observed-history")
    # MARTS owns calculations and dimensional publication; helper models are internal.
    d.edge("shared-attribute-history", [(1414, 570), (1450, 570), (1450, 610), (1544, 610), (1544, 580)], "observed-history", "analytics-product")
    d.edge("product-consumption", [(1900, 467.5), (1998, 467.5), (1998, 435), (2038, 435)], "analytics-product", "stock-dashboard")

    capabilities = [
        "Metadata & lineage · dbt model descriptions and dependency graph",
        "Data quality · dbt key, relationship, price-history and analytical tests",
        "Pipeline orchestration · Apache Airflow coordinates Python and dbt runs",
    ]
    for i, line in enumerate(capabilities):
        d.box(f"capability-{i}", 780, 958+i*30, 1118, 30, [line], font=17)
    d.box("source-orientation", 150, 1083, 1058, 40, ["SOURCE SYSTEM ORIENTED"], kind="right-arrow", fill=BLUE, stroke=BLUE_EDGE, font=17)
    d.box("business-orientation", 1220, 1083, 1080, 40, ["BUSINESS ORIENTED"], kind="left-arrow", fill="#f8dfe0", stroke="#be7478", font=17)
    return d


def main():
    diagram = build_diagram()
    ids = [b.id for b in diagram.boxes] + [e["id"] for e in diagram.edges]
    if len(ids) != len(set(ids)):
        raise ValueError("Diagram IDs must be unique")
    for box in diagram.boxes:
        if not (0 <= box.x < WIDTH and 0 <= box.y < HEIGHT
                and box.x + box.w <= WIDTH and box.y + box.h <= HEIGHT):
            raise ValueError(f"Box outside canvas: {box.id}")
    for edge in diagram.edges:
        if any(x1 != x2 and y1 != y2 for (x1, y1), (x2, y2)
               in zip(edge["points"], edge["points"][1:])):
            raise ValueError(f"Route must use right-angle segments: {edge['id']}")
    assets = ROOT / "assets"
    assets.mkdir(exist_ok=True)
    for extension, content in (("svg", diagram.svg()), ("drawio", diagram.drawio())):
        path = assets / f"stock-market-architecture.{extension}"
        path.write_text(content, encoding="utf-8")
        ET.parse(path)  # Validate XML before handing it to a renderer/editor.
        print(path)


if __name__ == "__main__":
    main()
