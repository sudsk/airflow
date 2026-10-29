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

import pytest

from airflow.providers.google.cloud.utils.bigquery import bq_cast, convert_job_id


class TestBigQueryUtils:
    @pytest.mark.parametrize(
        "bq_type",
        [
            "INTEGER",
            "FLOAT",
            "TIMESTAMP",
            "BOOLEAN",
            "STRING",
            "RECORD",
            "UNKNOWN",
        ],
    )
    def test_bq_cast_none(self, bq_type):
        assert bq_cast(None, bq_type) is None

    def test_bq_cast_integer(self):
        result = bq_cast("123", "INTEGER")
        assert result == 123
        assert isinstance(result, int)

    def test_bq_cast_float(self):
        result = bq_cast("123.45", "FLOAT")
        assert result == 123.45
        assert isinstance(result, float)

    def test_bq_cast_timestamp(self):
        result = bq_cast("123.45", "TIMESTAMP")
        assert result == 123.45
        assert isinstance(result, float)

    @pytest.mark.parametrize(
        "val, expected",
        [
            ("true", True),
            ("false", False),
        ],
    )
    def test_bq_cast_boolean(self, val, expected):
        result = bq_cast(val, "BOOLEAN")
        assert result is expected
        assert isinstance(result, bool)

    @pytest.mark.parametrize(
        "invalid_val",
        [
            "True",
            "False",
            "invalid",
        ],
    )
    def test_bq_cast_boolean_invalid(self, invalid_val):
        with pytest.raises(ValueError, match=f"^{invalid_val} must have value 'true' or 'false'$"):
            bq_cast(invalid_val, "BOOLEAN")

    @pytest.mark.parametrize(
        "string_field, bq_type",
        [
            ("test_string", "STRING"),
            ("test_record", "RECORD"),
            ("custom_data", "UNKNOWN"),
        ],
    )
    def test_bq_cast_unrecognised_type(self, string_field, bq_type):
        assert bq_cast(string_field, bq_type) == string_field

    def test_convert_job_id_single_str(self):
        job_id = "job_123"
        project_id = "test_project"
        location = "europe-west1"
        assert convert_job_id(job_id, project_id, location) == "test_project:europe-west1:job_123"

    def test_convert_job_id_list(self):
        job_ids = ["job_1", "job_2", "job_3"]
        project_id = "test_project"
        location = "asia-east1"
        expected = [
            "test_project:asia-east1:job_1",
            "test_project:asia-east1:job_2",
            "test_project:asia-east1:job_3",
        ]
        assert convert_job_id(job_ids, project_id, location) == expected

    @pytest.mark.parametrize("location", [None, ""])
    def test_convert_job_id_default_location_single(self, location):
        job_id = "job_123"
        project_id = "test_project"
        assert convert_job_id(job_id, project_id, location) == "test_project:US:job_123"

    @pytest.mark.parametrize("location", [None, ""])
    def test_convert_job_id_default_location_list(self, location):
        job_ids = ["job_1", "job_2"]
        project_id = "test_project"
        assert convert_job_id(job_ids, project_id, location) == [
            "test_project:US:job_1",
            "test_project:US:job_2",
        ]
