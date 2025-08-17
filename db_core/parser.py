import os
import re
from pathlib import Path

from db_core.db_manager import DBManager
from db_core.table_manager import TableManager
from db_core.Insert_manager import InsertManager
from db_core.select_manager import SelectManager
from db_core.update_manager import UpdateManager
from db_core.delete_manager import DeleteManager

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

class Parser:
    def __init__(self):
        self.active_db = None

    def route(self, command: str):
        cmd = command.strip().rstrip(";").strip()
        if not cmd:
            return {"error": "Empty command"}

        # Use command
        if re.match(r"^\s*USE\s+(\w+)\s*$", cmd, re.IGNORECASE):
            return self.parse_use(cmd)

        # CREATE DATABASE
        if re.match(r"^\s*CREATE\s+DATABASE\s+(\w+)\s*$", cmd, re.IGNORECASE):
            return self.parse_create_db(cmd)

        # CREATE TABLE
        if "CREATE TABLE" in cmd.upper():
            return self.parse_create_table(cmd)

        # INSERT
        if re.match(r"^\s*INSERT\s+INTO\s+(\w+)\s+VALUES\s*\(.+\)\s*$", cmd, re.IGNORECASE):
            return self.parse_insert(cmd)

        # SELECT
        if re.match(r"^\s*SELECT\s+.+\s+FROM\s+(\w+)(?:\s+WHERE\s+.+)?\s*$", cmd, re.IGNORECASE):
            return self.parse_select(cmd)

        # UPDATE
        if re.match(r"^\s*UPDATE\s+(\w+)\s+SET\s+.+\s+WHERE\s+.+\s*$", cmd, re.IGNORECASE):
            return self.parse_update(cmd)

        # DELETE
        if re.match(r"^\s*DELETE\s+FROM\s+(\w+)\s+WHERE\s+.+\s*$", cmd, re.IGNORECASE):
            return self.parse_delete(cmd)

        return {"error": "Unsupported or invalid command"}

    def parse_use(self, cmd: str):
        m = re.match(r"^\s*USE\s+(\w+)\s*$", cmd, re.IGNORECASE)
        if not m:
            return {"error": "Invalid USE syntax"}
        self.active_db = m.group(1)
        # Check if the database exists
        db_manager = DBManager(self.active_db)
        print(db_manager)
        if not db_manager.database_exists():
            return {"error": f"Database '{self.active_db}' does not exist."}
        return {"success": f"Using database {self.active_db}"}

    def parse_create_db(self, command: str):
        command = command.strip().rstrip(";")
        match = re.match(r"CREATE\s+DATABASE\s+(\w+)", command, re.IGNORECASE)
        if match:
            db_name = match.group(1)
            db = DBManager(db_name)
            return db.create_db()
        return "Invalid CREATE DATABASE syntax"

    def parse_create_table(self, command: str):
        command = command.strip().rstrip(";")

        if not self.active_db:
            return {"error": "No active database. Use 'USE <dbname>;'"}

        match_table = re.search(r"CREATE\s+TABLE\s+(\w+)\s*\((.+)\)", command, re.IGNORECASE)
        if not match_table:
            return "Invalid CREATE TABLE syntax"

        table_name = match_table.group(1)
        columns_str = match_table.group(2)
        columns_list = self._parse_columns(columns_str)

        schema = {"columns": columns_list}

        table = TableManager(self.active_db, table_name, schema)
        return table.create_table()

    def _parse_columns(self, columns_str: str):
        columns = [col.strip() for col in columns_str.split(",")]

        schema = []
        for col in columns:
            parts = col.split()
            col_name = parts[0]
            col_type = parts[1].upper()
            constraints = [p.upper() for p in parts[2:]] if len(parts) > 2 else []

            schema.append({"name": col_name, "type": col_type, "constraints": constraints})
        return schema

    def parse_insert(self, command: str):
        if not self.active_db:
            return {"error": "No active database. Use 'USE <dbname>;'"}

        m = re.match(r"^\s*INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.+)\)\s*$", command, re.IGNORECASE)
        if not m:
            return {"error": "Invalid Insert Syntax"}

        table_name = m.group(1)
        if not self._table_exists(self.active_db, table_name):
            return {"error": f"Table '{table_name}' does not exist in database '{self.active_db}'"}
        
        values_str = m.group(2)
        raw_values = [self._unquote(v.strip()) for v in self._split_commas_respecting_quotes(values_str)]

        norm_values = []
        for v in raw_values:
            if v.isdigit():
                norm_values.append(int(v))
            else:
                try:
                    norm_values.append(float(v))
                except ValueError:
                    if v.lower() == "true":
                        norm_values.append(True)
                    elif v.lower() == "false":
                        norm_values.append(False)
                    else:
                        norm_values.append(v)

        insert = InsertManager(self.active_db, table_name)
        return insert.insert_values(norm_values)

    def parse_select(self, command: str):
        if not self.active_db:
            return {"error": "No active database selected. Use 'USE <dbname>;' before a SELECT statement."}

        m = re.match(r"^\s*SELECT\s+(.+)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?\s*$", command, re.IGNORECASE)
        if not m:
            return {"error": "Invalid SELECT syntax."}

        columns_str, table_name, where_clause = m.groups()
        columns = [c.strip() for c in columns_str.split(",")]

        if not self._table_exists(self.active_db, table_name):
            return {"error": f"Table '{table_name}' does not exist in database '{self.active_db}'."}

        try:
            select_manager = SelectManager(self.active_db, table_name)
            result = select_manager.select(columns, where_clause)
            return {"success": f"Selected data from '{table_name}'", "data": result}
        except Exception as e:
            return {"error": f"An unexpected error occurred during SELECT operation: {e}"}

    def parse_update(self, command: str):
        if not self.active_db:
            return {"error": "No active database selected. Use 'USE <dbname>;' before an UPDATE statement."}

        m = re.match(r"^\s*UPDATE\s+(\w+)\s+SET\s+(.+)\s+WHERE\s+(.+)\s*$", command, re.IGNORECASE)
        if not m:
            return {"error": "Invalid UPDATE syntax."}

        table_name, set_clause, where_clause = m.groups()

        if not self._table_exists(self.active_db, table_name):
            return {"error": f"Table '{table_name}' does not exist in database '{self.active_db}'."}

        update_manager = UpdateManager(self.active_db, table_name)
        return update_manager.update(set_clause, where_clause)

    def parse_delete(self, command: str):
        if not self.active_db:
            return {"error": "No active database selected. Use 'USE <dbname>;' before a DELETE statement."}

        m = re.match(r"^\s*DELETE\s+FROM\s+(\w+)\s+WHERE\s+(.+)\s*$", command, re.IGNORECASE)
        if not m:
            return {"error": "Invalid DELETE syntax."}

        table_name, where_clause = m.groups()

        if not self._table_exists(self.active_db, table_name):
            return {"error": f"Table '{table_name}' does not exist in database '{self.active_db}'."}

        delete_manager = DeleteManager(self.active_db, table_name)
        return delete_manager.delete(where_clause)

    def _split_commas_respecting_quotes(self, s: str):
        items = []
        cur = []
        in_sq = False
        in_dq = False
        i = 0
        while i < len(s):
            ch = s[i]
            if ch == "'" and not in_dq:
                in_sq = not in_sq
                cur.append(ch)
            elif ch == '"' and not in_sq:
                in_dq = not in_dq
                cur.append(ch)
            elif ch == "," and not in_sq and not in_dq:
                items.append("".join(cur).strip())
                cur = []
            else:
                cur.append(ch)
            i += 1
        if cur:
            items.append("".join(cur).strip())
        return items

    def _unquote(self, s: str):
        s = s.strip()
        if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
            return s[1:-1]
        return s

    def _table_exists(self, db_name, table_name):
        db_path = DATA_DIR / db_name / table_name
        return db_path.exists()
