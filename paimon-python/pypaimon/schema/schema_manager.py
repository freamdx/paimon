################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from pathlib import Path
from typing import Optional

from pypaimon import Schema
from pypaimon.common.file_io import FileIO
from pypaimon.schema.table_schema import TableSchema


class SchemaManager:

    def __init__(self, file_io: FileIO, table_path: Path):
        self.schema_prefix = "schema-"
        self.file_io = file_io
        self.table_path = table_path
        self.schema_path = table_path / "schema"

    def latest(self) -> Optional['TableSchema']:
        try:
            versions = self._list_versioned_files()
            if not versions:
                return None

            max_version = max(versions)
            return self._read_schema(max_version)
        except Exception as e:
            raise RuntimeError(f"Failed to load schema from path: {self.schema_path}") from e

    def create_table(self, schema: Schema, external_table: bool = False) -> TableSchema:
        while True:
            latest = self.latest()
            if latest is not None:
                if external_table:
                    self._check_schema_for_external_table(latest.to_schema(), schema)
                    return latest
                else:
                    raise RuntimeError("Schema in filesystem exists, creation is not allowed.")

            table_schema = TableSchema.from_schema(schema_id=0, schema=schema)
            success = self.commit(table_schema)
            if success:
                return table_schema

    def commit(self, new_schema: TableSchema) -> bool:
        schema_path = self._to_schema_path(new_schema.id)
        try:
            return self.file_io.try_to_write_atomic(schema_path, new_schema.to_json())
        except Exception as e:
            raise RuntimeError(f"Failed to commit schema: {e}") from e

    def _to_schema_path(self, schema_id: int) -> Path:
        return self.schema_path / f"{self.schema_prefix}{schema_id}"

    def _read_schema(self, schema_id: int) -> Optional['TableSchema']:
        schema_path = self._to_schema_path(schema_id)
        if not self.file_io.exists(schema_path):
            return None

        return TableSchema.from_path(self.file_io, schema_path)

    def _list_versioned_files(self) -> list[int]:
        if not self.file_io.exists(self.schema_path):
            return []

        statuses = self.file_io.list_status(self.schema_path)
        if statuses is None:
            return []

        versions = []
        for status in statuses:
            name = Path(status.path).name
            if name.startswith(self.schema_prefix):
                try:
                    version = int(name[len(self.schema_prefix):])
                    versions.append(version)
                except ValueError:
                    continue
        return versions

    @staticmethod
    def _check_schema_for_external_table(exists_schema: Schema, new_schema: Schema):
        if ((not new_schema.pa_schema or new_schema.pa_schema.equals(exists_schema.pa_schema))
                and (not new_schema.partition_keys or new_schema.partition_keys == exists_schema.partition_keys)
                and (not new_schema.primary_keys or new_schema.primary_keys == exists_schema.primary_keys)):
            exists_options = exists_schema.options
            new_options = new_schema.options
            for key, value in new_options.items():
                if (key != 'owner' and key != 'path'
                        and (key not in exists_options or exists_options[key] != value)):
                    raise ValueError(
                        f"New schema's options are not equal to the exists schema's, "
                        f"new schema: {new_options}, exists schema: {exists_options}")
        else:
            raise ValueError(
                f"New schema is not equal to the exists schema, "
                f"new schema: {new_schema}, exists schema: {exists_schema}")
