"""
Animated architecture diagram for the GCP MLOps platform, rendered as a
self-contained HTML/SVG string with inline icons (no external icon-font or
CDN dependency - avoids repeating the Streamlit Cloud loading issues we hit
with other dependencies).

Each layer's three services light up one at a time (not all at once), each
with its own specific action line - so the animation shows what's actually
happening at each step, not a single sentence per layer.

Call build_html(stats=None) to get the HTML. Pass a stats dict (see
DEFAULT_STATS below for the expected shape) to have real numbers from the
last live pipeline run show up in the relevant stage's detail lines instead
of generic placeholder text - this is what makes the walkthrough dynamic
rather than static.

    import streamlit.components.v1 as components
    html = animated_architecture.build_html(stats=st.session_state.get("last_pipeline_stats"))
    components.html(html, height=1200, scrolling=True)
"""

import json as _json

ICON_DEFS = """
<symbol id="i-db" viewBox="0 0 24 24"><ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v12a8 3 0 0 0 16 0V6"/></symbol>
<symbol id="i-stream" viewBox="0 0 24 24"><path d="M4 12h14M13 6l6 6-6 6"/></symbol>
<symbol id="i-layers" viewBox="0 0 24 24"><path d="M12 3l9 5-9 5-9-5z"/><path d="M3 13l9 5 9-5"/></symbol>
<symbol id="i-gear" viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M12 2v3M12 19v3M2 12h3M19 12h3M4.9 4.9l2.1 2.1M17 17l2.1 2.1M4.9 19.1L7 17M17 7l2.1-2.1"/></symbol>
<symbol id="i-grid" viewBox="0 0 24 24"><rect x="3" y="3" width="8" height="8" rx="1"/><rect x="13" y="3" width="8" height="8" rx="1"/><rect x="3" y="13" width="8" height="8" rx="1"/><rect x="13" y="13" width="8" height="8" rx="1"/></symbol>
<symbol id="i-ml" viewBox="0 0 24 24"><circle cx="6" cy="6" r="2"/><circle cx="18" cy="6" r="2"/><circle cx="12" cy="18" r="2"/><path d="M6 8l6 8M18 8l-6 8M8 6h8"/></symbol>
<symbol id="i-server" viewBox="0 0 24 24"><rect x="4" y="4" width="16" height="6" rx="1"/><rect x="4" y="14" width="16" height="6" rx="1"/><circle cx="7" cy="7" r="0.6"/><circle cx="7" cy="17" r="0.6"/></symbol>
<symbol id="i-building" viewBox="0 0 24 24"><rect x="6" y="4" width="12" height="16"/><path d="M9 8h2M13 8h2M9 12h2M13 12h2M9 16h2M13 16h2"/></symbol>
<symbol id="i-shield" viewBox="0 0 24 24"><path d="M12 3l7 3v6c0 4.5-3 7.5-7 9-4-1.5-7-4.5-7-9V6z"/></symbol>
"""

LAYERS = [
    ("1. Data sources", [
        ("i-db", "Operational DBs (CDC)", "Reading row-level changes as they happen in transactional databases."),
        ("i-stream", "Event streams", "Capturing real-time events as they're published - clicks, orders, sensor readings."),
        ("i-db", "Files / partner feeds", "Picking up batch files and partner data drops on their scheduled cadence."),
    ]),
    ("2. Ingestion", [
        ("i-stream", "Pub/Sub streaming", "Buffering incoming events so nothing is lost if downstream processing lags."),
        ("i-stream", "Datastream CDC", "Replicating database changes into the pipeline with sub-minute latency."),
        ("i-server", "Cloud Run API ingest", "Accepting pushed API payloads and validating them against the expected schema."),
    ]),
    ("3. Data lake: bronze / silver / gold", [
        ("i-layers", "Cloud Storage raw", "Landing data exactly as received, immutable, schema-on-read. {bronze_rows}"),
        ("i-layers", "BigLake cleansed", "Deduplicating, standardizing types, resolving entities into a canonical form. {silver_stats}"),
        ("i-db", "BigQuery curated", "Producing business-ready, point-in-time-correct tables. {gold_rows}"),
    ]),
    ("4. Processing", [
        ("i-gear", "Dataflow batch", "Running scheduled transformation jobs: joins, enrichment, aggregation."),
        ("i-gear", "Dataflow streaming", "Applying the same transformations continuously as new events arrive."),
        ("i-gear", "Dataproc Spark", "Handling heavier distributed workloads that need full Spark compute."),
    ]),
    ("5. Feature platform", [
        ("i-grid", "Feature engineering", "Computing windowed aggregations and derived features from curated data."),
        ("i-grid", "Feature Store offline", "Storing historical feature values for reproducible model training."),
        ("i-grid", "Feature Store online", "Serving the latest feature values with millisecond latency for live inference."),
    ]),
    ("6. ML platform", [
        ("i-ml", "Training + registry", "Training a new model version and registering it with full lineage to its training data."),
        ("i-ml", "Batch prediction", "Scoring the full dataset on a schedule, writing predictions to the warehouse. {anomalies}"),
        ("i-ml", "Model monitoring", "Comparing live prediction distributions against training data to catch drift early. {forecast}"),
    ]),
    ("7. Serving layer", [
        ("i-db", "BigQuery warehouse", "Making aggregated results queryable for analysts and BI tools. {kpi_summary}"),
        ("i-server", "Vertex AI endpoint", "Serving real-time predictions behind a low-latency inference API."),
        ("i-server", "Memorystore cache", "Caching hot lookups so repeat queries don't hit the warehouse every time."),
    ]),
    ("8. Consumers", [
        ("i-building", "Enterprise clients", "Delivering finished data and predictions through client-facing dashboards and APIs."),
        ("i-building", "Internal apps", "Powering internal tools and operational dashboards off the same curated data."),
        ("i-building", "Analysts / downstream", "Feeding analysts, downstream systems, and further automation."),
    ]),
]

DEFAULT_STATS = {
    "bronze_rows": "",
    "silver_stats": "",
    "gold_rows": "",
    "anomalies": "",
    "forecast": "",
    "kpi_summary": "",
}


def _fill_stats(text, stats):
    try:
        return text.format(**{**DEFAULT_STATS, **(stats or {})})
    except Exception:
        return text.format(**DEFAULT_STATS)


def _build_layers_svg():
    parts = []
    y = 40
    layer_h = 70
    gap = 26
    box_w, box_h = 180, 40
    xs = [50, 250, 450]

    for i, (title, services) in enumerate(LAYERS):
        parts.append('<g id="l' + str(i) + '" class="layer">')
        parts.append('<rect class="container" x="40" y="' + str(y) + '" width="600" height="' + str(layer_h) + '" rx="10"/>')
        parts.append('<circle class="activity-dot" cx="590" cy="' + str(y + 18) + '" r="5"/>')
        parts.append('<text class="th" x="56" y="' + str(y + 18) + '" font-size="14" font-weight="500">' + title + '</text>')
        for j, ((icon, label, _), bx) in enumerate(zip(services, xs)):
            by = y + 26
            parts.append('<rect id="l' + str(i) + 'b' + str(j) + '" class="svcbox" x="' + str(bx) + '" y="' + str(by) + '" width="' + str(box_w) + '" height="' + str(box_h) + '" rx="6"/>')
            parts.append('<use href="#' + icon + '" x="' + str(bx + 10) + '" y="' + str(by + 10) + '" width="20" height="20" class="icon"/>')
            parts.append('<text class="t" x="' + str(bx + 38) + '" y="' + str(by + box_h / 2) + '" dominant-baseline="central" font-size="11">' + label + '</text>')
        parts.append('</g>')

        if i < len(LAYERS) - 1:
            arrow_y1 = y + layer_h
            arrow_y2 = y + layer_h + gap
            parts.append('<line x1="340" y1="' + str(arrow_y1) + '" x2="340" y2="' + str(arrow_y2) + '" stroke="var(--border-strong,#888)" stroke-width="1" marker-end="url(#arrow)"/>')
            parts.append('<circle id="p' + str(i) + '" class="pulse" cx="340" cy="' + str(arrow_y1) + '" r="6"/>')

        y += layer_h + gap

    return "\n".join(parts), y


def build_html(stats=None):
    layers_svg, next_y = _build_layers_svg()
    viewbox_h = next_y + 170

    stages_data = []
    for title, services in LAYERS:
        stages_data.append({
            "title": title,
            "services": [
                {"label": label, "detail": _fill_stats(detail, stats)}
                for _, label, detail in services
            ],
        })
    stages_json = _json.dumps(stages_data)

    top_bar = (
        '<div style="display:flex;justify-content:space-between;align-items:center;'
        'margin-bottom:1rem;font-family:sans-serif;flex-wrap:wrap;gap:8px">'
        '<button id="playBtn" onclick="playFlow()" style="padding:8px 16px;border-radius:8px;'
        'border:0.5px solid #888;background:transparent;cursor:pointer;font-size:14px">'
        '&#9654; Run pipeline flow</button>'
        '<div style="font-size:11px;color:#5F5E5A;display:flex;gap:14px">'
        '<span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
        'background:#185FA5;margin-right:4px"></span>Active now</span>'
        '<span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
        'background:#3B6D11;margin-right:4px"></span>Complete</span>'
        '<span><span style="display:inline-block;width:8px;height:8px;border-radius:50%;'
        'background:#B4B2A9;margin-right:4px"></span>Not yet reached</span>'
        '</div></div>'
    )

    status_panel = (
        '<div id="statusPanel" style="font-family:sans-serif;min-height:60px;margin-bottom:12px;'
        'padding:10px 14px;border-radius:8px;border:0.5px solid #D3D1C7;background:#F1EFE8;'
        'font-size:13px;color:#444441">'
        'Click "Run pipeline flow" to see exactly what happens at every stage, service by service.'
        '</div>'
    )

    svg_style = (
        "@keyframes pulse-dot { 0%,100% { opacity: 1; r: 5; } 50% { opacity: 0.35; r: 6.5; } }"
        ".layer rect.container{fill:#E6F1FB;stroke:#B4B2A9;stroke-width:0.5;transition:stroke .3s,stroke-width .3s}"
        ".layer.active rect.container{stroke:#185FA5;stroke-width:2.5}"
        ".layer.done rect.container{stroke:#3B6D11;stroke-width:1.5}"
        ".svcbox{fill:#FFFFFF;stroke:#D3D1C7;stroke-width:0.5;transition:fill .3s,stroke .3s}"
        ".svcbox.active{fill:#EAF3DE;stroke:#639922;stroke-width:1.5}"
        ".svcbox.done{fill:#F5FAF0}"
        ".pulse{fill:#185FA5;opacity:0;transition:transform .7s linear,opacity .2s}"
        ".icon{color:#5F5E5A;stroke:currentColor;fill:none;stroke-width:1.5;stroke-linecap:round;stroke-linejoin:round}"
        ".activity-dot{fill:#B4B2A9;opacity:0.3}"
        ".layer.active .activity-dot{fill:#185FA5;animation:pulse-dot 1s ease-in-out infinite}"
        ".layer.done .activity-dot{fill:#3B6D11;opacity:1}"
        "text.th{fill:#0C447C}"
        "text.t{fill:#2C2C2A}"
    )

    cross_cutting = (
        '<rect x="40" y="' + str(next_y) + '" width="600" height="140" rx="10" fill="none" '
        'stroke="#B4B2A9" stroke-width="0.5" stroke-dasharray="4 3"/>'
        '<use href="#i-shield" x="56" y="' + str(next_y + 16) + '" width="18" height="18" class="icon"/>'
        '<text class="t" x="80" y="' + str(next_y + 29) + '" font-size="12" font-weight="500">Cross-cutting (always active, not animated)</text>'
        '<text class="t" x="56" y="' + str(next_y + 52) + '" font-size="11">Orchestration: Cloud Composer</text>'
        '<text class="t" x="56" y="' + str(next_y + 74) + '" font-size="11">Observability: Cloud Monitoring, Cloud Logging, Looker Studio</text>'
        '<text class="t" x="56" y="' + str(next_y + 96) + '" font-size="11">Governance and security: Dataplex, Cloud DLP, IAM, KMS, Audit Logs</text>'
        '<text class="t" x="56" y="' + str(next_y + 118) + '" font-size="11">CI/CD: Cloud Build, Artifact Registry, Terraform, Cloud Run/GKE rollout</text>'
    )

    svg = (
        '<svg width="100%" viewBox="0 0 680 ' + str(viewbox_h) + '" role="img" style="font-family:sans-serif">'
        '<title>Animated GCP MLOps platform architecture</title>'
        '<desc>Eight stages from data sources to consumers, each with three services that '
        'activate one at a time, showing the specific action happening at each step.</desc>'
        '<defs><marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" '
        'orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="context-stroke" '
        'stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>'
        + ICON_DEFS + '</defs>'
        '<style>' + svg_style + '</style>'
        + layers_svg + cross_cutting +
        '</svg>'
    )

    log_panel = (
        '<div id="logPanel" style="font-family:sans-serif;margin-top:12px;max-height:320px;'
        'overflow-y:auto;border:0.5px solid #D3D1C7;border-radius:8px;padding:10px 14px;'
        'font-size:12px;color:#444441;display:none"></div>'
    )

    script = (
        "<script>\n"
        "const STAGES = " + stages_json + ";\n"
        "function sleep(ms){return new Promise(r=>setTimeout(r,ms));}\n"
        "async function playFlow(){\n"
        "  const btn=document.getElementById('playBtn');\n"
        "  const statusPanel=document.getElementById('statusPanel');\n"
        "  const logPanel=document.getElementById('logPanel');\n"
        "  btn.disabled=true;\n"
        "  logPanel.style.display='block';\n"
        "  logPanel.innerHTML='';\n"
        "  const n=STAGES.length;\n"
        "  const layers=[...Array(n).keys()].map(i=>document.getElementById('l'+i));\n"
        "  const pulses=[...Array(n-1).keys()].map(i=>document.getElementById('p'+i));\n"
        "  layers.forEach(l=>l.classList.remove('active','done'));\n"
        "  document.querySelectorAll('.svcbox').forEach(b=>b.classList.remove('active','done'));\n"
        "  pulses.forEach(p=>{p.style.opacity=0;p.style.transform='translateY(0px)';});\n"
        "  for(let i=0;i<n;i++){\n"
        "    layers[i].classList.add('active');\n"
        "    const stage=STAGES[i];\n"
        "    const logEntry=document.createElement('div');\n"
        "    logEntry.style.marginBottom='10px';\n"
        "    logEntry.innerHTML='<div style=\"font-weight:500;margin-bottom:2px\">'+stage.title+'</div>';\n"
        "    logPanel.appendChild(logEntry);\n"
        "    for(let j=0;j<stage.services.length;j++){\n"
        "      const svc=stage.services[j];\n"
        "      const box=document.getElementById('l'+i+'b'+j);\n"
        "      box.classList.add('active');\n"
        "      statusPanel.innerHTML='<strong>Stage '+(i+1)+' of '+n+': '+stage.title+'</strong><br>'+\n"
        "        '<span style=\"color:#185FA5\">'+svc.label+'</span> \\u2014 '+svc.detail;\n"
        "      await sleep(1100);\n"
        "      box.classList.remove('active');\n"
        "      box.classList.add('done');\n"
        "      const line=document.createElement('div');\n"
        "      line.style.marginLeft='12px';\n"
        "      line.style.marginBottom='3px';\n"
        "      line.innerHTML='&#10003; <strong>'+svc.label+'</strong> \\u2014 '+svc.detail;\n"
        "      logEntry.appendChild(line);\n"
        "      logPanel.scrollTop=logPanel.scrollHeight;\n"
        "    }\n"
        "    if(i<pulses.length){\n"
        "      pulses[i].style.opacity=1;\n"
        "      pulses[i].style.transform='translateY(26px)';\n"
        "      await sleep(700);\n"
        "      pulses[i].style.opacity=0;\n"
        "      pulses[i].style.transform='translateY(0px)';\n"
        "    }\n"
        "    layers[i].classList.remove('active');\n"
        "    layers[i].classList.add('done');\n"
        "  }\n"
        "  statusPanel.innerHTML='<strong>Pipeline flow complete.</strong> Every stage and service is logged below \\u2014 scroll to review.';\n"
        "  btn.disabled=false;\n"
        "}\n"
        "</script>"
    )

    return top_bar + status_panel + svg + log_panel + script


HTML = build_html()
