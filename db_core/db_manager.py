from pathlib import Path
import shutil
import os

class DBManager:
    def __init__(self, db_name, base_path="data"):
        self.db_name = db_name
        self.base_path = base_path
        self.target_path = Path(base_path) / db_name

    def database_exists(self) -> bool:
        return self.target_path.exists() and self.target_path.is_dir()

    def create_db(self):
        if self.database_exists():
            return {"error": f"Database '{self.db_name}' already exists."}
        try:
            self.target_path.mkdir(parents=True, exist_ok=True)
            return {"success": f"Database '{self.db_name}' created successfully."}
        except Exception as e:
            return {"error": f"Error while creating database: {e}"}

    def delete_db(self):
        if not self.database_exists():
            return {"error": f"Database '{self.db_name}' does not exist."}
        try:
            shutil.rmtree(self.target_path)
            return {"success": f"Database '{self.db_name}' deleted successfully."}
        except Exception as e:
            return {"error": f"Error while deleting database: {e}"}

    def list_databases(self) -> list[str]:
        try:
            return [f.name for f in Path(self.base_path).iterdir() if f.is_dir()]
        except Exception as e:
            return [f"Error listing databases: {e}"]

        

        










        