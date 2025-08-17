import json
from pathlib import Path

class DeleteManager:
    def __init__(self, db_name, table_name):
        self.db_name = db_name
        self.table_name = table_name
        self.base_path = Path("data") / db_name / table_name
        self.wal_path = self.base_path / "log.wal"

    def delete(self, where_clause):
        log_entry = {
            "operation": "delete",
            "where": where_clause
        }

        try:
            with open(self.wal_path, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
            return {"success": f"Delete operation logged for table '{self.table_name}'."}
        except Exception as e:
            return {"error": f"Failed to write to WAL: {e}"}

