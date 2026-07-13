"""
Free, no-API-key help assistant for the GCP Data & AI CoE demo.

Answers common questions about the CoE, the architecture, and this app using
simple keyword matching against a curated knowledge base - no external API
call, no API key, no cost, and no dependency that could break the deploy
(everything here is pure Python + what's already in requirements.txt).
"""

import re

KNOWLEDGE_BASE = [
    {
        "keywords": ["why", "coe", "exist", "purpose", "point", "matter"],
        "answer": (
            "Before this CoE, GCP expertise, architecture decisions, and reusable "
            "code were scattered across individual projects — leading to duplicated "
            "work, inconsistent quality, and solutioning that depended on which "
            "person happened to be staffed. This CoE turns GCP capability into an "
            "organizational asset: one reviewed architecture, one library of "
            "reusable code, one certification path, one story to tell clients and Google."
        ),
    },
    {
        "keywords": ["architecture", "layer", "layers", "diagram", "structure"],
        "answer": (
            "The reference architecture has 7 layers: (1) Data sources, "
            "(2) Ingestion — Pub/Sub, Datastream, Dataflow, (3) Lakehouse storage — "
            "Cloud Storage, BigLake, (4) Processing — Dataflow, Dataproc, BigQuery, "
            "(5) ML platform — Vertex AI, (6) Analytics & BI — BigQuery, Looker, "
            "and (7) Application layer — Cloud Run, Streamlit. See the "
            "'Architecture overview' page for full detail on each layer."
        ),
    },
    {
        "keywords": ["real", "simulated", "fake", "actually", "genuine", "mock"],
        "answer": (
            "The data transformation and ML logic (cleaning, aggregation, anomaly "
            "detection, forecasting) is genuinely real — it runs on whatever data "
            "you give it. Ingestion and storage are simulated by default (in-memory) "
            "so the demo works instantly with no GCP setup. Turn on 'Real GCP "
            "infrastructure' on the Live pipeline demo page to have it actually "
            "write to Pub/Sub, Cloud Storage, and BigQuery instead."
        ),
    },
    {
        "keywords": ["pipeline", "stage", "stages", "bronze", "silver", "gold", "medallion"],
        "answer": (
            "The pipeline follows a medallion pattern: Bronze (raw landing, "
            "timestamp added) → Silver (deduplication, null-filling, invalid-row "
            "dropping, revenue calculation) → Gold (aggregated rollups by region/"
            "product/month) → ML platform (anomaly detection + forecast) → "
            "Analytics & BI (the dashboard you see)."
        ),
    },
    {
        "keywords": ["deploy", "deployment", "host", "hosting", "streamlit cloud", "cloud run"],
        "answer": (
            "Two options: Streamlit Community Cloud (fastest, free, good for "
            "simulated-mode demos — push to GitHub, connect at share.streamlit.io) "
            "or Cloud Run (better for real GCP mode since it can use a service "
            "account instead of personal credentials — see the Dockerfile and "
            "GCP_SETUP.md)."
        ),
    },
    {
        "keywords": ["segfault", "crash", "error", "broken", "fails", "failing"],
        "answer": (
            "If the app crashes with 'Segmentation fault' on Streamlit Cloud, it's "
            "almost always a native-extension dependency (grpc, pyarrow) being "
            "unstable on a very new Python version. Fix: keep requirements.txt "
            "lean (only streamlit/pandas/numpy/plotly) and install extras like "
            "google-cloud-* or google-generativeai separately via "
            "requirements-gcp.txt / requirements-ai.txt only when you need them."
        ),
    },
    {
        "keywords": ["catalog", "asset", "reusable", "ip"],
        "answer": (
            "The Reusable asset catalog tracks accelerators, templates, and demo "
            "assets. It's currently persisted to a local JSON file on the server — "
            "survives a page refresh, but isn't durable across multiple Cloud Run "
            "instances. For that, swap it for Firestore or a BigQuery table."
        ),
    },
    {
        "keywords": ["certification", "cert", "training", "skill", "learn"],
        "answer": (
            "The certification roadmap starts with Google Cloud Digital Leader "
            "(all CoE members, first 30 days), then Associate Cloud Engineer, "
            "then track-specific Professional certs (Data Engineer, ML Engineer, "
            "Cloud Architect) depending on your role. See the Certification "
            "Roadmap document for the full path."
        ),
    },
    {
        "keywords": ["gcp", "mode", "pubsub", "pub/sub", "bigquery", "storage", "credentials"],
        "answer": (
            "To enable real GCP mode: run terraform/ (or the gcloud commands in "
            "GCP_SETUP.md) to create a bucket, dataset, and Pub/Sub topic, then "
            "in the app expand 'Real GCP infrastructure', check the box, and enter "
            "your project ID, bucket name, and dataset ID."
        ),
    },
    {
        "keywords": ["test", "testing", "pytest", "unit test"],
        "answer": (
            "Pipeline logic is unit tested independently of the Streamlit UI in "
            "tests/test_pipeline.py — run with `pip install -r requirements-dev.txt "
            "&& pytest tests/`. It covers deduplication, null handling, revenue "
            "calculation, aggregation, and anomaly detection."
        ),
    },
    {
        "keywords": ["ai assistant", "chatgpt", "claude", "gemini", "groq", "api key"],
        "answer": (
            "The AI Assistant page also supports bringing your own API key for "
            "Claude, ChatGPT, Gemini, or Groq for open-ended questions. Gemini "
            "(via aistudio.google.com) and Groq (via console.groq.com) both have "
            "genuinely free tiers with no credit card if you want that instead of "
            "this built-in FAQ helper."
        ),
    },
]


SMALL_TALK = [
    {
        "triggers": ["hi", "hello", "hey", "hii", "hiya", "yo", "greetings", "good morning", "good afternoon", "good evening"],
        "match": "exact_or_starts",
        "answer": (
            "Hi! I'm a built-in FAQ helper for this GCP Data & AI CoE app — "
            "no API key, no cost, works instantly. Ask me about the "
            "architecture layers, pipeline stages, what's real vs simulated, "
            "deployment, the catalog, certifications, or errors like segfaults. "
            "I'm not a full language model though — for open-ended "
            "conversation, switch to 'Bring your own API key' above."
        ),
    },
    {
        "triggers": [
            "what does this app do", "what does the app do", "explain this app",
            "explain the app", "app overview", "walk me through", "walk through",
            "how does this app work", "end to end", "full overview",
            "overview of this app", "what is this app", "what does it do",
            "tell me about this app", "describe this app",
        ],
        "match": "contains",
        "answer": (
            "Here's the full app, end to end — 4 pages:\n\n"
            "**1. Live pipeline demo** — Pick a data source (generated sample "
            "sales data, your own CSV upload, or a live BigQuery query). "
            "Optionally turn on 'Real GCP infrastructure' to have it actually "
            "hit Pub/Sub, Cloud Storage, and BigQuery instead of simulating. "
            "Click 'Run pipeline' and watch it move through 5 stages: "
            "Bronze (raw landing) → Silver (dedup, null-fill, drop invalid "
            "rows, compute revenue) → Gold (aggregate by region/product/"
            "month) → ML platform (anomaly detection + 2-month forecast) → "
            "Analytics & BI (the dashboard: KPI cards, charts, downloadable "
            "tables, and a full data-quality/lineage audit log).\n\n"
            "**2. Architecture overview** — The 7-layer GCP reference "
            "architecture this pipeline mirrors: data sources, ingestion, "
            "lakehouse storage, processing, ML platform, analytics & BI, "
            "and application layer — each mapped to specific GCP services.\n\n"
            "**3. Reusable asset catalog** — A persisted list of this CoE's "
            "reusable IP (this app, the Terraform module, the test suite, "
            "etc.), with a form to add new entries.\n\n"
            "**4. AI Assistant** — This chat. Free built-in FAQ mode (what "
            "you're using now, no API key) or bring-your-own-key mode for "
            "Claude, ChatGPT, Gemini, or Groq.\n\n"
            "Under the hood: pipeline.py holds all the data/ML logic "
            "(independently unit tested), app.py is the Streamlit UI, and "
            "terraform/ can provision the real GCP resources. Runs locally, "
            "on Streamlit Community Cloud, or on Cloud Run."
        ),
    },
    {
        "triggers": ["thanks", "thank you", "thx", "ty", "appreciate it"],
        "match": "contains",
        "answer": "You're welcome! Ask away if you have more questions about the CoE or this app.",
    },
    {
        "triggers": ["what can you do", "who are you", "what are you", "help me", "capabilities", "what do you do"],
        "match": "contains",
        "answer": (
            "I can answer questions about: the CoE's purpose, the 7 "
            "architecture layers, the pipeline's bronze/silver/gold/ML "
            "stages, what's genuinely real vs simulated in this demo, "
            "deployment (Streamlit Cloud vs Cloud Run), enabling real GCP "
            "mode, the reusable asset catalog, the certification roadmap, "
            "and common errors like segfaults. I'm keyword-based, not a "
            "full LLM — for anything else, switch to 'Bring your own API key'."
        ),
    },
    {
        "triggers": ["bye", "goodbye", "see ya", "see you", "later"],
        "match": "exact_or_starts",
        "answer": "Bye! Come back anytime you have questions about the CoE or this app.",
    },
]


def _small_talk(question):
    q_lower = question.strip().lower().rstrip("!.?")
    q_tokens = _tokenize(question)
    for entry in SMALL_TALK:
        if entry["match"] == "exact_or_starts":
            if any(q_lower == t or q_lower.startswith(t + " ") for t in entry["triggers"]):
                return entry["answer"]
        else:  # "contains" - whole-word match for single words, substring for phrases
            for t in entry["triggers"]:
                if " " in t:
                    if t in q_lower:
                        return entry["answer"]
                elif t in q_tokens:
                    return entry["answer"]
    return None


def _tokenize(text):
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def answer(question):
    """Checks small talk first (greetings, thanks, capabilities), then the
    FAQ knowledge base with specificity-weighted keyword scoring. Returns
    None if nothing matches either."""
    small_talk_reply = _small_talk(question)
    if small_talk_reply:
        return small_talk_reply

    q_tokens = _tokenize(question)
    q_lower = question.lower()
    if not q_tokens:
        return None

    best_entry, best_score = None, 0
    for entry in KNOWLEDGE_BASE:
        matched = [kw for kw in entry["keywords"] if kw in q_tokens or kw in q_lower]
        score = sum(len(kw) for kw in matched)
        if score > best_score:
            best_entry, best_score = entry, score

    if best_score == 0:
        return None
    return best_entry["answer"]


FALLBACK = (
    "I don't have a built-in answer for that. Try rephrasing with terms like "
    "'architecture', 'pipeline stages', 'deployment', 'real GCP mode', "
    "'catalog', 'certification', or 'errors' — or switch to 'Bring your own "
    "API key' above for open-ended questions."
)
