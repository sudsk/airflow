import pytest
from unittest import mock

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator


class TestQuotaProjectId:
    @mock.patch("airflow.providers.google.cloud.operators.bigquery.BigQueryHook")
    def test_bigquery_create_empty_dataset_operator(self, mock_hook):
        op = BigQueryCreateEmptyDatasetOperator(
            task_id="test_task",
            dataset_id="test_dataset",
            quota_project_id="test_quota_project",
        )
        assert "quota_project_id" in op.template_fields
        assert op.quota_project_id == "test_quota_project"
        
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once()
        assert mock_hook.call_args.kwargs.get("quota_project_id") == "test_quota_project"

    @mock.patch("airflow.providers.google.cloud.operators.gcs.GCSHook")
    def test_gcs_create_bucket_operator(self, mock_hook):
        op = GCSCreateBucketOperator(
            task_id="test_task",
            bucket_name="test_bucket",
            quota_project_id="test_quota_project",
        )
        assert "quota_project_id" in op.template_fields
        assert op.quota_project_id == "test_quota_project"
        
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once()
        assert mock_hook.call_args.kwargs.get("quota_project_id") == "test_quota_project"

    @mock.patch("airflow.providers.google.cloud.operators.dataflow.DataflowHook")
    def test_dataflow_templated_job_start_operator(self, mock_hook):
        op = DataflowTemplatedJobStartOperator(
            task_id="test_task",
            template="gs://dataflow-templates/wordcount",
            quota_project_id="test_quota_project",
        )
        assert "quota_project_id" in op.template_fields
        assert op.quota_project_id == "test_quota_project"
        
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once()
        assert mock_hook.call_args.kwargs.get("quota_project_id") == "test_quota_project"

    @mock.patch("airflow.providers.google.cloud.operators.dataproc.DataprocHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataproc.Cluster.to_dict")
    def test_dataproc_create_cluster_operator(self, mock_to_dict, mock_hook):
        op = DataprocCreateClusterOperator(
            task_id="test_task",
            project_id="test_project",
            region="test_region",
            cluster_name="test_cluster",
            quota_project_id="test_quota_project",
        )
        assert "quota_project_id" in op.template_fields
        assert op.quota_project_id == "test_quota_project"
        
        op.execute(context=mock.MagicMock())
        mock_hook.assert_called_once()
        assert mock_hook.call_args.kwargs.get("quota_project_id") == "test_quota_project"
