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
            "resources/gold_pipeline.yml",
            "src/notebooks/00_validate_foundation.py",
            "src/notebooks/01_generate_synthetic_landing.py",
            "src/notebooks/02_train_fraud_model.py",
            "src/notebooks/03_score_and_explain_fraud_model.py",
            "src/pipelines/bronze.py",
            "src/pipelines/silver.py",
            "src/pipelines/gold.py",
            "contracts/authentication_event.schema.json",
            "contracts/device_intelligence_event.schema.json",
            "contracts/access_event.schema.json",
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
            "bronze_authentication_events",
            "quarantine_authentication_events",
            "bronze_device_intelligence_events",
            "quarantine_device_intelligence_events",
            "bronze_access_events",
            "quarantine_access_events",
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
            "silver_customer_behavioral_features",
            "behavioral_risk_score",
            "REPEATED_LOGIN_FAILURES",
            "IMPOSSIBLE_TRAVEL",
            "silver_access_risk_signals",
            "PRIVILEGED_AFTER_HOURS",
        ]
        self.assertTrue(all(control in pipeline for control in required_controls))

    def test_behavioral_features_do_not_derive_geography_from_training_label(self):
        pipeline = (ROOT / "src/pipelines/silver.py").read_text()
        self.assertNotIn('F.col("scenario_label") == "IMPOSSIBLE_TRAVEL"', pipeline)

    def test_synthetic_sources_are_append_only(self):
        notebook = (ROOT / "src/notebooks/01_generate_synthetic_landing.py").read_text()
        self.assertEqual(5, notebook.count('.mode("append")'))

    def test_gold_pipeline_has_explainable_decision_controls(self):
        pipeline = (ROOT / "src/pipelines/gold.py").read_text()
        required_controls = [
            "gold_device_network_features",
            "gold_risk_decisions",
            "risk_score_bounded",
            "recommended_action",
            "reason_codes",
            "policy_version",
            "gold_investigation_queue",
            "gold_access_alerts",
            "gold_decision_metrics",
        ]
        self.assertTrue(all(control in pipeline for control in required_controls))

    def test_decision_policy_does_not_use_synthetic_labels(self):
        pipeline = (ROOT / "src/pipelines/gold.py").read_text()
        self.assertNotIn("scenario_label", pipeline)

    def test_foundation_job_refreshes_gold_after_silver(self):
        job = (ROOT / "resources/foundation_job.yml").read_text()
        self.assertIn("task_key: refresh_gold_pipeline", job)
        self.assertIn("pipeline_id: ${resources.pipelines.aegispay_gold.id}", job)

    def test_foundation_job_trains_model_after_gold(self):
        job = (ROOT / "resources/foundation_job.yml").read_text()
        self.assertIn("task_key: train_and_register_fraud_model", job)
        self.assertIn("task_key: refresh_gold_pipeline", job)
        self.assertIn("02_train_fraud_model.py", job)

    def test_mlflow_training_has_governance_and_leakage_controls(self):
        notebook = (ROOT / "src/notebooks/02_train_fraud_model.py").read_text()
        required_controls = [
            'mlflow.set_registry_uri("databricks-uc")',
            "registered_model_name=REGISTERED_MODEL_NAME",
            '"data_classification": "synthetic"',
            '"split_strategy": "xxhash64(event_id): 80/20"',
            "ml_model_evaluation_metrics",
            "roc_auc",
            "pr_auc",
            "precision",
            "recall",
            "f1",
            "pd.to_numeric",
            "SERIALIZATION_FORMAT_CLOUDPICKLE",
            "load only from the governed AegisPay registry",
        ]
        self.assertTrue(all(control in notebook for control in required_controls))
        self.assertNotIn('"scenario_label",\n]', notebook)

    def test_model_scoring_has_explanations_policy_comparison_and_feedback(self):
        notebook = (ROOT / "src/notebooks/03_score_and_explain_fraud_model.py").read_text()
        required_controls = [
            "get_model_version_by_alias",
            "predict_proba",
            "ml_scored_transactions",
            "explanation_signals",
            "model_policy_action_agreement",
            "model_policy_risk_gap",
            "ml_analyst_feedback",
            "CONFIRMED_FRAUD, FALSE_POSITIVE, or NEEDS_MORE_REVIEW",
            "delta.enableChangeDataFeed",
            "ml_scoring_metrics",
        ]
        self.assertTrue(all(control in notebook for control in required_controls))
        self.assertNotIn("scenario_label", notebook)

    def test_foundation_job_scores_champion_model_after_training(self):
        job = (ROOT / "resources/foundation_job.yml").read_text()
        self.assertIn("task_key: score_and_explain_fraud_model", job)
        self.assertIn("03_score_and_explain_fraud_model.py", job)
        self.assertIn("model_alias: Champion", job)


if __name__ == "__main__":
    unittest.main()
