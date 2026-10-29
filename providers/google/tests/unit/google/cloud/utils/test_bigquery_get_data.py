# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

import logging
from unittest import mock

import pytest
from google.cloud.bigquery.table import Row, RowIterator

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.utils.bigquery_get_data import bigquery_get_data


class TestBigQueryGetData:
    def test_bigquery_get_data_batching_and_termination(self, caplog):
        caplog.set_level(logging.INFO)
        logger = logging.getLogger("test_bigquery_get_data")
        hook = mock.create_autospec(BigQueryHook, instance=True)

        batch_1_rows = [mock.MagicMock(spec=Row), mock.MagicMock(spec=Row)]
        batch_1_rows[0].values.return_value = ["val1", "val2"]
        batch_1_rows[1].values.return_value = ["val3", "val4"]

        batch_2_rows = [mock.MagicMock(spec=Row)]
        batch_2_rows[0].values.return_value = ["val5", "val6"]

        hook.list_rows.side_effect = [batch_1_rows, batch_2_rows, []]

        generator = bigquery_get_data(
            logger=logger,
            dataset_id="test_dataset",
            table_id="test_table",
            big_query_hook=hook,
            batch_size=2,
            selected_fields=["col1", "col2"],
        )

        results = list(generator)

        assert results == [
            [["val1", "val2"], ["val3", "val4"]],
            [["val5", "val6"]],
        ]

        assert hook.list_rows.call_count == 3
        hook.list_rows.assert_has_calls(
            [
                mock.call(
                    dataset_id="test_dataset",
                    table_id="test_table",
                    max_results=2,
                    selected_fields=["col1", "col2"],
                    start_index=0,
                ),
                mock.call(
                    dataset_id="test_dataset",
                    table_id="test_table",
                    max_results=2,
                    selected_fields=["col1", "col2"],
                    start_index=2,
                ),
                mock.call(
                    dataset_id="test_dataset",
                    table_id="test_table",
                    max_results=2,
                    selected_fields=["col1", "col2"],
                    start_index=4,
                ),
            ]
        )

        assert "Fetching Data from:" in caplog.text
        assert "Dataset: test_dataset ; Table: test_table" in caplog.text
        assert "Total Extracted rows: 2" in caplog.text
        assert "Total Extracted rows: 3" in caplog.text
        assert "Job Finished" in caplog.text

    def test_bigquery_get_data_pass_through_args(self):
        logger = logging.getLogger("test_bigquery_get_data")
        hook = mock.create_autospec(BigQueryHook, instance=True)
        hook.list_rows.return_value = []

        generator = bigquery_get_data(
            logger=logger,
            dataset_id="my_dataset",
            table_id="my_table",
            big_query_hook=hook,
            batch_size=100,
            selected_fields="field_a,field_b",
        )

        list(generator)

        hook.list_rows.assert_called_once_with(
            dataset_id="my_dataset",
            table_id="my_table",
            max_results=100,
            selected_fields="field_a,field_b",
            start_index=0,
        )

    def test_bigquery_get_data_row_iterator_error(self):
        logger = logging.getLogger("test_bigquery_get_data")
        hook = mock.create_autospec(BigQueryHook, instance=True)
        row_iterator = mock.create_autospec(RowIterator, instance=True)
        hook.list_rows.return_value = row_iterator

        generator = bigquery_get_data(
            logger=logger,
            dataset_id="test_dataset",
            table_id="test_table",
            big_query_hook=hook,
            batch_size=10,
            selected_fields=None,
        )

        with pytest.raises(
            TypeError, match=r"BigQueryHook\.list_rows\(\) returns iterator when return_iterator=False \(default\)"
        ):
            next(generator)
