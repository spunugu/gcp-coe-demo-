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


def _tokenize(text):
    return set(re.findall(r"[a-z0-9]+", text.lower()))


def answer(question):
    """Returns the best-matching canned answer, or None if nothing scores
    above a minimal relevance threshold. Longer, more specific keywords are
    weighted higher so a generic word like 'why' doesn't out-tie a specific
    one like 'segfault'."""
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
