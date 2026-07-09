# GCP Data & AI CoE — Architecture Explorer & Live Demo

A starter-kit Streamlit app for the Incedo Data Technology CoE (GCP track).
Shows the end-to-end GCP reference architecture and a working live-data demo.

## Run locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Enable the live BigQuery demo (optional)

The app runs fine with sample data out of the box. To pull live results from
a public BigQuery dataset instead:

```bash
gcloud auth application-default login
export GOOGLE_CLOUD_PROJECT=your-billing-project-id
streamlit run app.py
```

Then enter your project ID in the "Live demo" page and click
**Run live BigQuery query**. Public datasets don't cost anything to store —
you're only billed for the (tiny) query bytes scanned.

## Deploy to Cloud Run

```bash
gcloud run deploy gcp-coe-demo \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars GOOGLE_CLOUD_PROJECT=your-billing-project-id
```

Add a `Dockerfile` if you want a custom container, or let Cloud Run's
buildpacks handle the Streamlit app automatically (it will detect
`requirements.txt` and run `streamlit run app.py` if you add a `Procfile`
with `web: streamlit run app.py --server.port=$PORT --server.address=0.0.0.0`).

## Extending this as a CoE asset

- **Architecture layers page**: swap in your team's actual reference
  architecture and reusable pattern docs per layer.
- **Live demo page**: replace the sample query with a real pipeline query
  from a project-specific dataset once one exists.
- **Reusable asset catalog page**: point it at a real source (Google Sheet,
  Firestore, or BigQuery table) instead of the in-memory session list, so
  the catalog persists across users.
