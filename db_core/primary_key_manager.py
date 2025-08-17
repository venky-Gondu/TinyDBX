import json
from pathlib import Path

class PrimaryKeyManager:
    def __init__(self, db_name, table_name):
        self.base_path = Path("data") / db_name / table_name
        self.schema_path = self.base_path / "schema.json"
        self.data_path = self.base_path / "data.json"
        self.pk_cache = None
        self.primary_key_column = None

    def _load_schema(self):
        try:
            with open(self.schema_path, "r") as f:
                schema = json.load(f)
            self.primary_key_column = schema.get("primary_key")
        except (FileNotFoundError, json.JSONDecodeError):
            self.primary_key_column = None

    def _build_cache(self):
        if not self.primary_key_column:
            return

        self.pk_cache = set()
        try:
            with open(self.data_path, "r") as f:
                rows = json.load(f)
            for row in rows:
                if self.primary_key_column in row:
                    self.pk_cache.add(row[self.primary_key_column])
        except (FileNotFoundError, json.JSONDecodeError):
            pass  # No data yet, cache remains empty

    def check_pk_uniqueness(self, pk_value):
        if not self.primary_key_column:
            return True  # No primary key defined

        if self.pk_cache is None:
            self._load_schema()
            self._build_cache()

        return pk_value not in self.pk_cache

    def add_pk_to_cache(self, pk_value):
        if self.primary_key_column and self.pk_cache is not None:
            self.pk_cache.add(pk_value)