import json
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

class SelectManager:
    def __init__(self, db_name, table_name):
        self.db_name = db_name
        self.table_name = table_name
        self.base_path = DATA_DIR / db_name / table_name
        self.data_path = self.base_path / "data.json"
        self.wal_path = self.base_path / "log.wal"

    def select(self, columns, where_clause):
        self._apply_wal()

        try:
            with open(self.data_path, "r") as f:
                rows = json.load(f)
                print(f"Read {len(rows)} rows from {self.data_path}")
        except (FileNotFoundError, json.JSONDecodeError):
            rows = []
            print(f"Could not read rows from {self.data_path}")

        if where_clause:
            rows = self._filter_rows(rows, where_clause)

        if columns != ["*"]:
            rows = self._project_columns(rows, columns)

        return rows

    def _filter_rows(self, rows, where_clause):
        # Naive implementation of a WHERE clause parser
        # This should be replaced with a more robust solution
        try:
            column, value = where_clause.split("=")
            column = column.strip()
            value = value.strip().strip("'\"")
            return [row for row in rows if str(row.get(column)) == value]
        except Exception as e:
            # Handle cases with more complex conditions or invalid syntax
            return []

    def _project_columns(self, rows, columns):
        return [{col: row.get(col) for col in columns} for row in rows]

    def _apply_wal(self):
        try:
            with open(self.data_path, "r") as f:
                rows = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            rows = []

        try:
            with open(self.wal_path, "r") as f:
                for line in f:
                    log_entry = json.loads(line)
                    if log_entry["operation"] == "insert":
                        rows.append(log_entry["data"])
                    elif log_entry["operation"] == "update":
                        rows = self._apply_update(rows, log_entry["set"], log_entry["where"])
                    elif log_entry["operation"] == "delete":
                        rows = self._apply_delete(rows, log_entry["where"])

            with open(self.data_path, "w") as f:
                json.dump(rows, f, indent=2)
            
            with open(self.wal_path, "w") as f:
                f.write("")

        except FileNotFoundError:
            pass

    def _apply_update(self, rows, set_clause, where_clause):
        # This is a naive implementation and should be improved
        try:
            where_col, where_val = where_clause.split("=")
            where_col = where_col.strip()
            where_val = where_val.strip().strip("'")

            set_col, set_val = set_clause.split("=")
            set_col = set_col.strip()
            set_val = set_val.strip().strip("'")

            for row in rows:
                if str(row.get(where_col)) == where_val:
                    row[set_col] = set_val
            return rows
        except Exception as e:
            return rows

    def _apply_delete(self, rows, where_clause):
        # This is a naive implementation and should be improved
        try:
            where_col, where_val = where_clause.split("=")
            where_col = where_col.strip()
            where_val = where_val.strip().strip("'")

            return [row for row in rows if str(row.get(where_col)) != where_val]
        except Exception as e:
            return rows