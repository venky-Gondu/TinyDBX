import re


from db_core.db_manager import DBManager
from db_core.table_manager import TableManager
from db_core.Insert_manager import InsertManager

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

        # CREATE TABLE (allow optional leading USE in same command)
        if "CREATE TABLE" in cmd.upper():
            return self.parse_create_table(cmd)

        # INSERT
        if re.match(r"^\s*INSERT\s+INTO\s+(\w+)\s+VALUES\s*\(.+\)\s*$", cmd, re.IGNORECASE):
            return self.parse_insert(cmd)

        # SELECT
        # if re.match(r"^\s*SELECT\s+", cmd, re.IGNORECASE):
        #     return self.parse_select(cmd)

        return {"error": "Unsupported or invalid command"}


    def parse_use(self, cmd: str):
        m = re.match(r"^\s*USE\s+(\w+)\s*$", cmd, re.IGNORECASE)
        if not m:
            return {"error": "Invalid USE syntax"}
        self.active_db = m.group(1)
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
    def parse_insert(self, command: str):


        m_use = re.search(r"USE\s+(\w+)", command, re.IGNORECASE)
        if m_use:
            self.active_db = m_use.group(1)

        if not self.active_db:
            return {"error": "No active database. Use 'USE <dbname>;'"}

        m = re.match(r"^\s*INSERT\s+INTO\s+(\w+)\s+VALUES\s*\((.+)\)\s*$", command, re.IGNORECASE)
        if not m:
            return {"error": "Invalid Insert Syntax"}

        table_name = m.group(1)
        values_str = m.group(2)

        raw_values = [self._unquote(v.strip()) for v in self._split_commas_respecting_quotes(values_str)]

        # Normalize types
        norm_values = []
        for v in raw_values:
            if v.isdigit():
                norm_values.append(int(v))
            else:
                try:
                    fv = float(v)
                    norm_values.append(fv)
                except ValueError:
                    if v.lower() == "true":
                        norm_values.append(True)
                    elif v.lower() == "false":
                        norm_values.append(False)
                    else:
                        norm_values.append(v)

        insert = InsertManager(self.active_db, table_name)
        return insert.insert_values(norm_values)
            

    


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



