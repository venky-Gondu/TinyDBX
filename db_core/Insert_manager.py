import json
from pathlib import Path
import os


class InsertManager:
    def __init__(self, db_name, table_name):
        self.db_name = db_name
        self.table_name = table_name
        self.base_path = Path("data") / db_name / table_name
        self.schema_path = self.base_path / "schema.json"
        self.data_path = self.base_path / "data.json"

    def insert_values(self, values: list):
        if not os.path.exists(self.base_path) or not os.path.isdir(self.base_path):
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

            # Type validation
            if col_type == "INT":
                if not isinstance(val, int):
                    return {"error": f"Column '{col_name}' expects INT but got {type(val).__name__}."}
            elif col_type == "FLOAT":
                if not isinstance(val, (float, int)):
                    return {"error": f"Column '{col_name}' expects FLOAT but got {type(val).__name__}."}
            elif col_type == "TEXT":
                if not isinstance(val, str):
                    return {"error": f"Column '{col_name}' expects TEXT but got {type(val).__name__}."}
            elif col_type == "BOOL":
                if not isinstance(val, bool):
                    return {"error": f"Column '{col_name}' expects BOOL but got {type(val).__name__}."}

            row_data[col_name] = val

        # Load existing rows
        try:
            with open(self.data_path, "r") as f:
                rows = json.load(f)
        except Exception as e:
            return {"error": f"Error reading data.json: {e}"}

        # Check primary key uniqueness
        if primary_key:
            for row in rows:
                if row.get(primary_key) == row_data[primary_key]:
                    return {"error": f"Primary key '{row_data[primary_key]}' already exists."}

        rows.append(row_data)

        try:
            with open(self.data_path, "w") as f:
                json.dump(rows, f, indent=2)
        except Exception as e:
            return {"error": f"Error saving new row: {e}"}

        return {"success": f"Row inserted into '{self.table_name}' successfully."}