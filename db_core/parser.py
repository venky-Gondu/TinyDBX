import re
from db_core.db_manager import DBManager
from db_core.table_manager import TableManager

class Parser:
    def __init__(self):
        self.active_db = None
    def route(self, command: str):
        command = command.strip().rstrip(";")

        if command.upper().startswith("CREATE DATABASE"):
            return self.parse_create_db(command)

        elif "USE" in command.upper() and "CREATE TABLE" in command.upper():
            return self.parse_create_table(command)

        return "Unsupported command or syntax"



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

        match_use = re.search(r"USE\s+(\w+)", command, re.IGNORECASE)
        if not match_use:
            return "Missing 'USE <db_name>' before CREATE TABLE"
        self.active_db = match_use.group(1)

        match_table = re.search(r"CREATE\s+TABLE\s+(\w+)\s*\((.+)\)", command, re.IGNORECASE)
        if not match_table:
            return "Invalid CREATE TABLE syntax"

        table_name = match_table.group(1)
        columns_str = match_table.group(2)
        columns_list = self._parse_columns(columns_str)

        # wrap inside a dict for TableManager
        schema = {"columns": columns_list}

        table = TableManager(self.active_db, table_name, schema)
        return table.create_table()
    def _parse_columns(self, columns_str: str):
        # Split columns by comma
        columns = [col.strip() for col in columns_str.split(",")]

        schema = []
        for col in columns:
            parts = col.split()
            col_name = parts[0]
            col_type = parts[1].upper()
            constraints = [p.upper() for p in parts[2:]] if len(parts) > 2 else []

            schema.append({
                "name": col_name,
                "type": col_type,
                "constraints": constraints
            })
        return schema
