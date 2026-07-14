"""
Animated architecture diagram for the GCP MLOps platform, rendered as a
self-contained HTML/SVG string with inline icons (no external icon-font or
CDN dependency - avoids repeating the Streamlit Cloud loading issues we hit
with other dependencies). Embed with:

    import streamlit.components.v1 as components
    components.html(animated_architecture.HTML, height=1050, scrolling=True)
"""

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
        ("i-db", "Operational DBs (CDC)"),
        ("i-stream", "Event streams"),
        ("i-db", "Files / partner feeds"),
    ], "Capturing raw signals: database changes via CDC, real-time events, and batch file drops."),
    ("2. Ingestion", [
        ("i-stream", "Pub/Sub streaming"),
        ("i-stream", "Datastream CDC"),
        ("i-server", "Cloud Run API ingest"),
    ], "Landing data reliably: Pub/Sub buffers streaming events, Datastream captures CDC, Cloud Run accepts API pushes."),
    ("3. Data lake: bronze / silver / gold", [
        ("i-layers", "Cloud Storage raw"),
        ("i-layers", "BigLake cleansed"),
        ("i-db", "BigQuery curated"),
    ], "Medallion storage: raw data lands in bronze, gets cleaned into silver, curated into business-ready gold tables."),
    ("4. Processing", [
        ("i-gear", "Dataflow batch"),
        ("i-gear", "Dataflow streaming"),
        ("i-gear", "Dataproc Spark"),
    ], "Transforming data: batch and streaming jobs join, aggregate, and validate records at scale."),
    ("5. Feature platform", [
        ("i-grid", "Feature engineering"),
        ("i-grid", "Feature Store offline"),
        ("i-grid", "Feature Store online"),
    ], "Building ML-ready features: engineered offline for training, served online with low latency for inference."),
    ("6. ML platform", [
        ("i-ml", "Training + registry"),
        ("i-ml", "Batch prediction"),
        ("i-ml", "Model monitoring"),
    ], "Training models, registering versions, running batch scoring, and watching for drift once deployed."),
    ("7. Serving layer", [
        ("i-db", "BigQuery warehouse"),
        ("i-server", "Vertex AI endpoint"),
        ("i-server", "Memorystore cache"),
    ], "Serving results: warehouse queries, real-time inference endpoints, and a low-latency cache for hot lookups."),
    ("8. Consumers", [
        ("i-building", "Enterprise clients"),
        ("i-building", "Internal apps"),
        ("i-building", "Analysts / downstream"),
    ], "Delivering value: dashboards, APIs, and downstream systems consume the finished data and predictions."),
]


def _build_layers_svg():
    parts = []
    y = 40
    layer_h = 70
    gap = 26
    box_w, box_h = 180, 40
    xs = [50, 250, 450]

    for i, (title, services, desc) in enumerate(LAYERS):
        parts.append(f'<g id="l{i}" class="layer">')
        parts.append(f'<rect class="container" x="40" y="{y}" width="600" height="{layer_h}" rx="10"/>')
        parts.append(f'<circle class="activity-dot" cx="590" cy="{y+18}" r="5"/>')
        parts.append(f'<text class="th" x="56" y="{y+18}" font-size="14" font-weight="500">{title}</text>')
        for (icon, label), bx in zip(services, xs):
            by = y + 26
            parts.append(f'<rect class="svcbox" x="{bx}" y="{by}" width="{box_w}" height="{box_h}" rx="6"/>')
            parts.append(f'<use href="#{icon}" x="{bx+10}" y="{by+10}" width="20" height="20" class="icon"/>')
            parts.append(f'<text class="t" x="{bx+38}" y="{by+box_h/2}" dominant-baseline="central" font-size="11">{label}</text>')
        parts.append('</g>')

        if i < len(LAYERS) - 1:
            arrow_y1 = y + layer_h
            arrow_y2 = y + layer_h + gap
            parts.append(f'<line x1="340" y1="{arrow_y1}" x2="340" y2="{arrow_y2}" stroke="var(--border-strong,#888)" stroke-width="1" marker-end="url(#arrow)"/>')
            parts.append(f'<circle id="p{i}" class="pulse" cx="340" cy="{arrow_y1}" r="6"/>')

        y += layer_h + gap

    return "\n".join(parts), y


_LAYERS_SVG, _NEXT_Y = _build_layers_svg()
_VIEWBOX_H = _NEXT_Y + 170

_DESCRIPTIONS_JSON = "[" + ",".join(
    '{"title": "%s", "desc": "%s"}' % (title.replace('"', "'"), desc.replace('"', "'"))
    for title, _, desc in LAYERS
) + "]"

HTML = f"""
<div style="display:flex;justify-content:center;margin-bottom:1rem;font-family:sans-serif">
<button id="playBtn" onclick="playFlow()" style="padding:8px 16px;border-radius:8px;border:0.5px solid #888;background:transparent;cursor:pointer;font-size:14px">
&#9654; Run pipeline flow</button>
</div>
<div id="statusPanel" style="font-family:sans-serif;min-height:44px;margin-bottom:12px;padding:10px 14px;border-radius:8px;border:0.5px solid #D3D1C7;background:#F1EFE8;font-size:13px;color:#444441">
Click "Run pipeline flow" to see a live, stage-by-stage walkthrough here.
</div>
<svg width="100%" viewBox="0 0 680 {_VIEWBOX_H}" role="img" style="font-family:sans-serif">
<title>Animated GCP MLOps platform architecture</title>
<desc>Eight stages from data sources to consumers, each showing GCP services with icons, with an animated pulse showing data and model flow between stages.</desc>
<defs>
<marker id="arrow" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse"><path d="M2 1L8 5L2 9" fill="none" stroke="context-stroke" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></marker>
{ICON_DEFS}
</defs>
<style>
@keyframes pulse-dot {{ 0%,100% {{ opacity: 1; r: 5; }} 50% {{ opacity: 0.35; r: 6.5; }} }}
.layer rect.container{{fill:#E6F1FB;stroke:#B4B2A9;stroke-width:0.5;transition:stroke .3s,stroke-width .3s}}
.layer.active rect.container{{stroke:#185FA5;stroke-width:2.5}}
.layer.done rect.container{{stroke:#3B6D11;stroke-width:1.5}}
.svcbox{{fill:#FFFFFF;stroke:#D3D1C7;stroke-width:0.5;transition:fill .3s}}
.layer.active .svcbox{{fill:#EAF3DE}}
.pulse{{fill:#185FA5;opacity:0;transition:transform .5s linear,opacity .2s}}
.icon{{color:#5F5E5A;stroke:currentColor;fill:none;stroke-width:1.5;stroke-linecap:round;stroke-linejoin:round}}
.activity-dot{{fill:#B4B2A9;opacity:0.3}}
.layer.active .activity-dot{{fill:#185FA5;animation:pulse-dot 1s ease-in-out infinite}}
.layer.done .activity-dot{{fill:#3B6D11;opacity:1}}
text.th{{fill:#0C447C}}
text.t{{fill:#2C2C2A}}
</style>
{_LAYERS_SVG}
<rect x="40" y="{_NEXT_Y}" width="600" height="140" rx="10" fill="none" stroke="#B4B2A9" stroke-width="0.5" stroke-dasharray="4 3"/>
<use href="#i-shield" x="56" y="{_NEXT_Y+16}" width="18" height="18" class="icon"/>
<text class="t" x="80" y="{_NEXT_Y+29}" font-size="12" font-weight="500">Cross-cutting (always active, not animated)</text>
<text class="t" x="56" y="{_NEXT_Y+52}" font-size="11">Orchestration: Cloud Composer</text>
<text class="t" x="56" y="{_NEXT_Y+74}" font-size="11">Observability: Cloud Monitoring, Cloud Logging, Looker Studio</text>
<text class="t" x="56" y="{_NEXT_Y+96}" font-size="11">Governance and security: Dataplex, Cloud DLP, IAM, KMS, Audit Logs</text>
<text class="t" x="56" y="{_NEXT_Y+118}" font-size="11">CI/CD: Cloud Build, Artifact Registry, Terraform, Cloud Run/GKE rollout</text>
</svg>
<div id="logPanel" style="font-family:sans-serif;margin-top:12px;max-height:220px;overflow-y:auto;border:0.5px solid #D3D1C7;border-radius:8px;padding:10px 14px;font-size:12px;color:#444441;display:none"></div>
<script>
const STAGES = {_DESCRIPTIONS_JSON};
function sleep(ms){{return new Promise(r=>setTimeout(r,ms));}}
async function playFlow(){{
  const btn=document.getElementById('playBtn');
  const statusPanel=document.getElementById('statusPanel');
  const logPanel=document.getElementById('logPanel');
  btn.disabled=true;
  logPanel.style.display='block';
  logPanel.innerHTML='';
  const n={len(LAYERS)};
  const layers=[...Array(n).keys()].map(i=>document.getElementById('l'+i));
  const pulses=[...Array(n-1).keys()].map(i=>document.getElementById('p'+i));
  layers.forEach(l=>l.classList.remove('active','done'));
  pulses.forEach(p=>{{p.style.opacity=0;p.style.transform='translateY(0px)';}});
  for(let i=0;i<layers.length;i++){{
    layers[i].classList.add('active');
    const stage=STAGES[i];
    statusPanel.innerHTML='<strong>Stage '+(i+1)+' of '+n+': '+stage.title+'</strong><br>'+stage.desc;
    await sleep(900);
    if(i<pulses.length){{
      pulses[i].style.opacity=1;
      pulses[i].style.transform='translateY(26px)';
      await sleep(500);
      pulses[i].style.opacity=0;
      pulses[i].style.transform='translateY(0px)';
    }}
    layers[i].classList.remove('active');
    layers[i].classList.add('done');
    const entry=document.createElement('div');
    entry.style.marginBottom='4px';
    entry.innerHTML='&#10003; <strong>'+stage.title+'</strong> — '+stage.desc;
    logPanel.appendChild(entry);
    logPanel.scrollTop=logPanel.scrollHeight;
  }}
  statusPanel.innerHTML='<strong>Pipeline flow complete.</strong> Scroll the log below to review every stage.';
  btn.disabled=false;
}}
</script>
"""
