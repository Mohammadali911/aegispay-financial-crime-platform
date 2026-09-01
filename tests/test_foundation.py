from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class FoundationTests(unittest.TestCase):
    def test_required_foundation_files_exist(self):
        required = [
            "README.md",
            "pyproject.toml",
            "databricks.yml",
            "docs/business-case.md",
            "docs/threat-model.md",
            "docs/architecture.md",
            "resources/foundation_job.yml",
            "resources/bronze_pipeline.yml",
            "resources/silver_pipeline.yml",
            "src/notebooks/00_validate_foundation.py",
            "src/notebooks/01_generate_synthetic_landing.py",
            "src/pipelines/bronze.py",
            "src/pipelines/silver.py",
        ]
        self.assertTrue(all((ROOT / path).is_file() for path in required))

    def test_no_real_data_directories_are_versioned(self):
        self.assertFalse((ROOT / "data").exists())

    def test_spark_array_indexes_are_explicit_ints(self):
        notebook = (ROOT / "src/notebooks/01_generate_synthetic_landing.py").read_text()
        self.assertEqual(3, notebook.count('.cast("int")'))

    def test_bronze_pipeline_has_quality_and_cdc_controls(self):
        pipeline = (ROOT / "src/pipelines/bronze.py").read_text()
        required_controls = [
            "expect_all_or_drop",
            "quarantine_payment_events",
            "quarantine_customer_changes",
            "create_auto_cdc_flow",
            "stored_as_scd_type=\"1\"",
        ]
        self.assertTrue(all(control in pipeline for control in required_controls))

    def test_silver_pipeline_has_dedup_identity_and_graph_controls(self):
        pipeline = (ROOT / "src/pipelines/silver.py").read_text()
        required_controls = [
            "dropDuplicatesWithinWatermark",
            "resolved_identity_id",
            "silver_transaction_edges",
            "transaction_count",
            "labeled_risk_event_count",
        ]
        self.assertTrue(all(control in pipeline for control in required_controls))

    def test_synthetic_sources_are_append_only(self):
        notebook = (ROOT / "src/notebooks/01_generate_synthetic_landing.py").read_text()
        self.assertEqual(2, notebook.count('.mode("append")'))


if __name__ == "__main__":
    unittest.main()
