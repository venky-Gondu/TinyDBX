import json
from pathlib import Path
from db_core.primary_key_manager import PrimaryKeyManager

class InsertManager:
    def __init__(self, db_name, table_name):
        self.db_name = db_name
        self.table_name = table_name
        self.base_path = Path("data") / db_name / table_name
        self.schema_path = self.base_path / "schema.json"
        self.data_path = self.base_path / "data.json"
        self.wal_path = self.base_path / "log.wal"
        self.pk_manager = PrimaryKeyManager(db_name, table_name)

    def insert_values(self, values: list):
        if not self.base_path.exists() or not self.base_path.is_dir():
            return {"error": f"No Table '{self.table_name}' is present in '{self.db_name}'"}

        try:
            with open(self.schema_path, "r") as f:
                schema = json.load(f)
        except Exception as e:
            return {"error": f"Error reading schema: {e}"}

        columns = schema.get("columns", [])
        primary_key = schema.get("primary_key")

        if len(columns) != len(values):
            return {"error": f"Column count mismatch. Expected {len(columns)} values, got {len(values)}."}

        row_data = {}
        for col, val in zip(columns, values):
            col_name = col["name"]
            col_type = col.get("type", "TEXT")

            # Type validation and data assignment
            if col_type == "INT" and not isinstance(val, int):
                return {"error": f"Column '{col_name}' expects INT but got {type(val).__name__}."}
            elif col_type == "FLOAT" and not isinstance(val, (float, int)):
                return {"error": f"Column '{col_name}' expects FLOAT but got {type(val).__name__}."}
            elif col_type == "TEXT" and not isinstance(val, str):
                return {"error": f"Column '{col_name}' expects TEXT but got {type(val).__name__}."}
            elif col_type == "BOOL" and not isinstance(val, bool):
                return {"error": f"Column '{col_name}' expects BOOL but got {type(val).__name__}."}
            
            row_data[col_name] = val

        # Primary key validation using PrimaryKeyManager
        if primary_key:
            pk_value = row_data.get(primary_key)
            if pk_value is None:
                return {"error": f"Primary key '{primary_key}' cannot be null."}
            
            if not self.pk_manager.check_pk_uniqueness(pk_value):
                return {"error": f"Primary key violation: '{pk_value}' already exists."}

        # Write-Ahead Logging (WAL)
        try:
            log_entry = {"operation": "insert", "data": row_data}
            with open(self.wal_path, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
        except Exception as e:
            return {"error": f"Failed to write to WAL: {e}"}

        # Update PK cache after successful insertion
        if primary_key:
            self.pk_manager.add_pk_to_cache(row_data[primary_key])

        return {"success": f"Insert operation logged for table '{self.table_name}'."}
