from pathlib import Path
import json
from db_core.schema_manager import Schema_Manager 


class TableManager:
    def __init__(self, db_name, table_name, schema):
        self.db_name = db_name
        self.table_name = table_name
        self.schema = schema
        self.base_path = Path("data") / db_name  # Fixed base path to match db_manager
        self.table_path = self.base_path / table_name  # Path to the specific table

    def create_table(self):
        if not self.base_path.exists():
            return {"error": f"Database '{self.db_name}' does not exist."}
        if self.table_path.exists():
            return {"error": f"Table '{self.table_name}' already exists in database '{self.db_name}'."}

        # Validate schema and identify primary key
        primary_key = None
        for col in self.schema.get("columns", []):
            if "PRIMARY" in col.get("constraints", []):
                primary_key = col["name"]
                break  # Assuming only one primary key

        self.schema["primary_key"] = primary_key
        
        validator = Schema_Manager(self.schema)
        validation_result = validator.validate()
        if "error" in validation_result:
            return validation_result

        try:
            self.table_path.mkdir(parents=True, exist_ok=False)

            # Write schema.json
            with open(self.table_path / "schema.json", "w") as f:
                json.dump(self.schema, f, indent=2)

            # Initialize empty rows.json
            with open(self.table_path / "data.json", "w") as f:
                json.dump([], f)

            # Create empty WAL log
            with open(self.table_path / "log.wal", "w") as f:
                f.write("")

            return {"success": f"Table '{self.table_name}' created successfully in database '{self.db_name}'."}

        except Exception as e:
            return {"error": f"Error while creating table '{self.table_name}': {e}"}
    
