"""
Unit tests for pipeline.py. Run with: pytest tests/
These test pure pandas logic only - no GCP credentials or network needed.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import pytest

import pipeline


@pytest.fixture
def sample_df():
    return pipeline.generate_sample_data(n=200, seed=1)


def test_generate_sample_data_has_required_columns(sample_df):
    missing = pipeline.validate_schema(sample_df)
    assert missing == []


def test_generate_sample_data_contains_intentional_dirtiness(sample_df):
    # duplicates
    assert sample_df["order_id"].duplicated().sum() > 0
    # nulls
    assert sample_df["unit_price"].isna().sum() > 0
    # invalid quantities
    assert (sample_df["quantity"] <= 0).sum() > 0


def test_validate_schema_flags_missing_columns():
    df = pd.DataFrame({"order_id": [1, 2]})
    missing = pipeline.validate_schema(df)
    assert "region" in missing
    assert "quantity" in missing


def test_run_bronze_adds_ingestion_timestamp(sample_df):
    bronze_df, gcs_uri, entry = pipeline.run_bronze(sample_df)
    assert "_ingested_at" in bronze_df.columns
    assert len(bronze_df) == len(sample_df)
    assert gcs_uri is None  # no gcp_cfg passed
    assert entry["stage"] == "Bronze (raw landing)"
    assert entry["rows_before"] == entry["rows_after"] == len(sample_df)


def test_run_silver_removes_duplicates(sample_df):
    bronze_df, _, _ = pipeline.run_bronze(sample_df)
    silver_df, stats, gcs_uri, entry = pipeline.run_silver(bronze_df)
    assert silver_df["order_id"].duplicated().sum() == 0
    assert stats["duplicates_removed"] > 0


def test_run_silver_fills_nulls(sample_df):
    bronze_df, _, _ = pipeline.run_bronze(sample_df)
    silver_df, stats, _, _ = pipeline.run_silver(bronze_df)
    assert silver_df["unit_price"].isna().sum() == 0
    assert stats["nulls_filled"] > 0


def test_run_silver_drops_invalid_quantities(sample_df):
    bronze_df, _, _ = pipeline.run_bronze(sample_df)
    silver_df, stats, _, _ = pipeline.run_silver(bronze_df)
    assert (silver_df["quantity"] <= 0).sum() == 0
    assert stats["invalid_rows_dropped"] > 0


def test_run_silver_computes_revenue_correctly(sample_df):
    bronze_df, _, _ = pipeline.run_bronze(sample_df)
    silver_df, _, _, _ = pipeline.run_silver(bronze_df)
    expected = silver_df["quantity"] * silver_df["unit_price"]
    assert (silver_df["revenue"] == expected).all()


def test_run_gold_aggregates_match_silver_total(sample_df):
    bronze_df, _, _ = pipeline.run_bronze(sample_df)
    silver_df, _, _, _ = pipeline.run_silver(bronze_df)
    by_region, by_product, monthly, kpis, bq_ref, entry = pipeline.run_gold(silver_df)

    assert abs(by_region["total_revenue"].sum() - kpis["total_revenue"]) < 0.01
    assert abs(by_product["total_revenue"].sum() - kpis["total_revenue"]) < 0.01
    assert bq_ref is None  # no gcp_cfg passed
    assert kpis["top_region"] == by_region.iloc[0]["region"]


def test_run_gold_kpis_are_positive(sample_df):
    bronze_df, _, _ = pipeline.run_bronze(sample_df)
    silver_df, _, _, _ = pipeline.run_silver(bronze_df)
    _, _, _, kpis, _, _ = pipeline.run_gold(silver_df)
    assert kpis["total_revenue"] > 0
    assert kpis["total_orders"] > 0
    assert kpis["avg_order_value"] > 0


def test_run_ml_flags_injected_anomalies(sample_df):
    bronze_df, _, _ = pipeline.run_bronze(sample_df)
    silver_df, _, _, _ = pipeline.run_silver(bronze_df)
    _, _, monthly, _, _, _ = pipeline.run_gold(silver_df)
    anomalies, forecast, entry = pipeline.run_ml(silver_df, monthly)
    # generate_sample_data() always injects 6 revenue spikes
    assert len(anomalies) > 0
    assert (anomalies["revenue_zscore"].abs() > 3).all()


def test_run_ml_forecast_shape_when_enough_history(sample_df):
    bronze_df, _, _ = pipeline.run_bronze(sample_df)
    silver_df, _, _, _ = pipeline.run_silver(bronze_df)
    _, _, monthly, _, _, _ = pipeline.run_gold(silver_df)
    _, forecast, _ = pipeline.run_ml(silver_df, monthly)
    if len(monthly) >= 3:
        assert len(forecast) == 2
        assert list(forecast.columns) == ["month", "predicted_revenue"]


def test_full_pipeline_is_deterministic_for_fixed_seed():
    df1 = pipeline.generate_sample_data(n=100, seed=42)
    df2 = pipeline.generate_sample_data(n=100, seed=42)
    pd.testing.assert_frame_equal(df1, df2)


def test_full_pipeline_runs_end_to_end_without_error(sample_df):
    bronze_df, _, _ = pipeline.run_bronze(sample_df)
    silver_df, stats, _, _ = pipeline.run_silver(bronze_df)
    by_region, by_product, monthly, kpis, _, _ = pipeline.run_gold(silver_df)
    anomalies, forecast, _ = pipeline.run_ml(silver_df, monthly)
    assert len(by_region) > 0
    assert len(by_product) > 0
    assert stats["rows_after_cleaning"] <= len(bronze_df)
