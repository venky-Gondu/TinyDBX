
class Schema_Manager:
    VALID_TYPES = {"INT", "TEXT", "FLOAT", "BOOL"}
    def __init__(self,schema:dict):
        self.schema=schema
    def validate(self):
        if "columns" not in self.schema:
            return {"error": "Schema must contain a 'columns' list"}
        col_names = set()
        primary_keys = []

        for column in self.schema["columns"]:
            name = column.get("name")
            col_type = column.get("type")

            if not name or not col_type:
                return {"error": f"Column definition missing 'name' or 'type': {column}"}

            if name in col_names:
                return {"error": f"Duplicate column name found: {name}"}
            col_names.add(name)

            if col_type not in self.VALID_TYPES:
                return {"error": f"Invalid type '{col_type}' for column '{name}'"}

            # Check for NOT NULL, PRIMARY KEY, DEFAULT
            if column.get("primary_key", False):
                primary_keys.append(name)

        if len(primary_keys) > 1:
            return {"error": "Multiple primary keys defined; only one is allowed."}

        return {"success": "Schema is valid."}
        
        
        