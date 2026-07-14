"""
Connector registry: the plug-and-play system for data sources.

Adding a new source means adding one entry here - the UI in app.py renders
its form fields generically from this spec, so no page code needs to
change. Each entry (other than the special-cased 'sample' and 'csv_upload')
has:
    label:    shown in the source picker
    fields:   list of form field specs rendered generically
    fetch:    function(params_dict) -> pandas.DataFrame
    requires: pip package(s) needed (shown to the user, and used in the
              friendly error message if the import fails)
    help:     optional caption shown above the form

Field spec: {"name": str, "label": str, "type": "text"|"password"|"number"|
             "textarea"|"select", "default": any, "placeholder": str,
             "options": list (for "select"), "help": str}
"""

import pipeline

CONNECTORS = {
    "sample": {
        "label": "Sample data (instant, no setup)",
    },
    "csv_upload": {
        "label": "Upload CSV",
    },
    "file_path": {
        "label": "File: CSV / JSON / Parquet / Excel (path or GCS URI)",
        "help": "One connector for any of these formats - the extension picks the reader. A file on this server, or gs://bucket/path (GCS needs `pip install gcsfs`).",
        "requires": "gcsfs (gs:// only), pyarrow (Parquet), openpyxl (Excel)",
        "fields": [
            {"name": "path_or_uri", "label": "File path or GCS URI", "type": "text", "placeholder": "gs://my-bucket/data/orders.parquet"},
        ],
        "fetch": lambda p: pipeline.read_file_any(p["path_or_uri"]),
        "test": lambda p: pipeline.test_csv_path(p["path_or_uri"]),
    },
    "sftp": {
        "label": "SFTP file drop",
        "help": "Downloads one file from an SFTP server (the classic enterprise batch file-drop pattern) and reads it by extension.",
        "requires": "paramiko",
        "fields": [
            {"name": "host", "label": "Host", "type": "text", "placeholder": "sftp.clientdomain.com"},
            {"name": "port", "label": "Port", "type": "number", "default": 22},
            {"name": "username", "label": "Username", "type": "text"},
            {"name": "password", "label": "Password", "type": "password"},
            {"name": "remote_path", "label": "Remote file path", "type": "text", "placeholder": "/outbound/orders.csv"},
        ],
        "fetch": lambda p: pipeline.read_from_sftp(p["host"], p["username"], p["password"], p["remote_path"], port=int(p["port"])),
        "test": lambda p: pipeline.test_sftp(p["host"], p["username"], p["password"], port=int(p["port"])),
    },
    "kafka": {
        "label": "Kafka topic",
        "help": (
            "The broker must be reachable from wherever this app runs. On Streamlit Cloud, that "
            "means a publicly reachable broker (e.g. Confluent Cloud with SASL_SSL). A broker "
            "inside a private VPC needs this app deployed on Cloud Run in the same network instead."
        ),
        "requires": "kafka-python",
        "fields": [
            {"name": "bootstrap_servers", "label": "Bootstrap servers", "type": "text", "placeholder": "broker1:9092,broker2:9092"},
            {"name": "topic", "label": "Topic name", "type": "text"},
            {"name": "max_messages", "label": "Max messages to read", "type": "number", "default": 50},
            {"name": "security_protocol", "label": "Security protocol", "type": "select", "options": ["PLAINTEXT", "SASL_SSL"], "default": "PLAINTEXT"},
            {"name": "sasl_username", "label": "SASL username / API key", "type": "password"},
            {"name": "sasl_password", "label": "SASL password / API secret", "type": "password"},
        ],
        "fetch": lambda p: pipeline.read_from_kafka(
            p["bootstrap_servers"], p["topic"], max_messages=int(p["max_messages"]),
            security_protocol=p["security_protocol"],
            sasl_username=p.get("sasl_username") or None, sasl_password=p.get("sasl_password") or None,
        ),
        "test": lambda p: pipeline.test_kafka_broker(p["bootstrap_servers"]),
    },
    "bigquery": {
        "label": "BigQuery (live query)",
        "help": "Requires GCP credentials available to this app (ADC locally, or the Cloud Run service account).",
        "requires": "google-cloud-bigquery",
        "fields": [
            {"name": "project_id", "label": "GCP project ID", "type": "text"},
            {"name": "query", "label": "SQL query", "type": "textarea", "default": "SELECT * FROM `project.dataset.table` LIMIT 1000"},
        ],
        "fetch": lambda p: pipeline.fetch_bigquery_data(p["project_id"], p["query"]),
        "test": lambda p: pipeline.test_bigquery(p["project_id"]),
    },
    "sql_database": {
        "label": "SQL database (Postgres / MySQL / Cloud SQL / AlloyDB)",
        "help": "One connector for any SQLAlchemy-supported database - the dialect prefix in the connection string picks the driver.",
        "requires": "sqlalchemy, psycopg2-binary (Postgres) or pymysql (MySQL)",
        "fields": [
            {"name": "connection_string", "label": "Connection string", "type": "password",
             "placeholder": "postgresql+psycopg2://user:password@host:5432/dbname"},
            {"name": "query", "label": "SQL query", "type": "textarea", "default": "SELECT * FROM orders LIMIT 1000"},
        ],
        "fetch": lambda p: pipeline.read_from_sql(p["connection_string"], p["query"]),
        "test": lambda p: pipeline.test_sql(p["connection_string"]),
    },
    "rest_api": {
        "label": "REST API (JSON)",
        "help": "Calls any JSON HTTP API. Use json_path if the row array is nested, e.g. 'data.records'.",
        "requires": "requests",
        "fields": [
            {"name": "url", "label": "URL", "type": "text", "placeholder": "https://api.example.com/orders"},
            {"name": "method", "label": "Method", "type": "select", "options": ["GET", "POST"], "default": "GET"},
            {"name": "headers_json", "label": "Headers (JSON, optional)", "type": "text", "placeholder": '{"Authorization": "Bearer ..."}'},
            {"name": "body_json", "label": "Request body (JSON, optional)", "type": "text"},
            {"name": "json_path", "label": "Path to row array (optional)", "type": "text", "placeholder": "data.records"},
        ],
        "fetch": lambda p: pipeline.read_from_rest_api(
            p["url"], method=p["method"], headers_json=p.get("headers_json", ""),
            body_json=p.get("body_json", ""), json_path=p.get("json_path", ""),
        ),
        "test": lambda p: pipeline.test_rest_api(p["url"], headers_json=p.get("headers_json", "")),
    },
    "google_sheets": {
        "label": "Google Sheets",
        "help": "Paste a service account's JSON key, or leave blank to use default credential discovery.",
        "requires": "gspread, google-auth",
        "fields": [
            {"name": "sheet_url_or_id", "label": "Sheet URL or ID", "type": "text"},
            {"name": "worksheet_name", "label": "Worksheet name", "type": "text", "default": "Sheet1"},
            {"name": "service_account_json", "label": "Service account JSON (optional)", "type": "textarea"},
        ],
        "fetch": lambda p: pipeline.read_from_google_sheets(
            p["sheet_url_or_id"], worksheet_name=p.get("worksheet_name", "Sheet1"),
            service_account_json=p.get("service_account_json", ""),
        ),
        "test": lambda p: pipeline.test_google_sheets(p["sheet_url_or_id"], service_account_json=p.get("service_account_json", "")),
    },
}
