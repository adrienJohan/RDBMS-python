# sgbdr/query_parser.py
import re

class QueryParser:
    def __init__(self, sgbdr):
        self.sgbdr = sgbdr

    def set_sgbdr(self, sgbdr):
        """Définir la référence à l’instance SGBDR"""
        self.sgbdr = sgbdr

    def parse_query(self, query):
        """Parser une requête SQL-like et retourner un dictionnaire avec les composants"""
        query = query.strip()

        if re.match(r"LOGIN JOUEUR\s+\w+\s+MOTDEPASSE\s+'.*'", query, re.IGNORECASE):
            match = re.match(r"LOGIN JOUEUR\s+(\w+)\s+MOTDEPASSE\s+'([^']*)'", query, re.IGNORECASE)
            if not match:
                raise ValueError("Tu cheat, il faut le format : LOGIN JOUEUR login MOTDEPASSE 'pass'")
            login, password = match.groups()
            return {"type": "login_user", "login": login, "password": password}
        
        elif re.match(r"CRAFTER JOUEUR\s+\w+\s+MOTDEPASSE\s+'.*'\s+PERMISSIONS\s+.+", query, re.IGNORECASE):
            match = re.match(r"CRAFTER JOUEUR\s+(\w+)\s+MOTDEPASSE\s+'([^']*)'\s+PERMISSIONS\s+(.+)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Tu cheat, il faut le format : CRAFTER JOUEUR login MOTDEPASSE 'pass' PERMISSIONS read,write,delete")
            login, password, perms = match.groups()
            permissions = [p.strip() for p in perms.split(",")]
            return {"type": "create_user", "login": login, "password": password, "permissions": permissions}
        
        elif re.match(r"EDIT JOUEUR\s+\w+\s+PERMISSIONS\s+.+", query, re.IGNORECASE):
            match = re.match(r"EDIT JOUEUR\s+(\w+)\s+PERMISSIONS\s+(.+)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Tu cheat, il faut le format : EDIT JOUEUR login PERMISSIONS read,write,delete")
            login, perms = match.groups()
            permissions = [p.strip() for p in perms.split(",")]
            return {"type": "edit_user_permissions", "login": login, "permissions": permissions}
        
        elif re.match(r"LISTE JOUEURS", query, re.IGNORECASE):
            return {"type": "list_users"}
        
        elif re.match(r"LISTE PERMISSIONS JOUEUR\s+\w+", query, re.IGNORECASE):
            match = re.match(r"LISTE PERMISSIONS JOUEUR\s+(\w+)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Tu cheat, il faut le format : LISTE PERMISSIONS JOUEUR login")
            login = match.groups()[0]
            return {"type": "list_user_permissions", "login": login}
        
        elif re.match(r"CRAFTER BASE\s+\w+", query, re.IGNORECASE):
            match = re.match(r"CRAFTER BASE\s+(\w+)", query, re.IGNORECASE)
            return {"type": "create_database", "db_name": match.groups()[0]}
        
        elif re.match(r"DEPOP BASE\s+\w+", query, re.IGNORECASE):
            match = re.match(r"DEPOP BASE\s+(\w+)", query, re.IGNORECASE)
            return {"type": "delete_database", "db_name": match.groups()[0]}
        
        elif re.match(r"UTILISER\s+\w+", query, re.IGNORECASE):
            match = re.match(r"UTILISER\s+(\w+)", query, re.IGNORECASE)
            return {"type": "use_database", "db_name": match.groups()[0]}
        
        elif re.match(r"QUITTER BASE", query, re.IGNORECASE):
            return {"type": "deselect_database"}
        
        elif re.match(r"LISTE BASES", query, re.IGNORECASE):
            return {"type": "list_databases"}
        
        elif re.match(r"EXPORTER BASE\s+\w+", query, re.IGNORECASE):
            match = re.match(r"EXPORTER BASE\s+(\w+)", query, re.IGNORECASE)
            return {"type": "export_database", "db_name": match.groups()[0]}
        
        elif re.match(r"IMPORTER BASE\s+\w+\s+FICHIER\s+.+", query, re.IGNORECASE):
            match = re.match(r"IMPORTER BASE\s+(\w+)\s+FICHIER\s+(.+)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Tu cheat, il faut le format : IMPORTER BASE nom FICHIER chemin")
            db_name, file_path = match.groups()
            return {"type": "import_database", "db_name": db_name, "file_path": file_path}
        
        elif re.match(r"CRAFTER TABLEAU\s+\w+\s*\(.+\)", query, re.IGNORECASE):
            match = re.match(r"CRAFTER TABLEAU\s+(\w+)\s*\((.+)\)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Tu cheat, il faut le format : CRAFTER TABLEAU nom (col1 TYPE [constraints], ...)")
            table_name, columns_str = match.groups()
            columns = []
            for col in [c.strip() for c in columns_str.split(",") if c.strip()]:
                parts = col.split()
                col_name = parts[0]
                col_type = parts[1].upper()
                constraints = parts[2:] if len(parts) > 2 else []

                # Validation type
                if col_type not in ("INT", "FLOAT", "TEXT", "DATE", "BOOLEAN"):
                    if not col_type.startswith("VARCHAR("):
                        raise ValueError(f"Type {col_type} non supporté")
                    if not col_type.endswith(")"):
                        raise ValueError("VARCHAR doit être VARCHAR(n)")
                    try:
                        size = int(col_type[8:-1])
                        if size <= 0:
                            raise ValueError()
                    except:
                        raise ValueError("VARCHAR(n) : n doit être un entier positif")

                columns.append(col)
            return {"type": "create_table", "table_name": table_name, "columns": columns}
        
        elif re.match(r"DEPOP TABLEAU\s+\w+", query, re.IGNORECASE):
            match = re.match(r"DEPOP TABLEAU\s+(\w+)", query, re.IGNORECASE)
            return {"type": "delete_table", "table_name": match.groups()[0]}
        
        elif re.match(r"LISTE TABLEAUX", query, re.IGNORECASE):
            return {"type": "list_tables"}
        
        elif re.match(r"POP DANS\s+\w+\s+VALEURS\s*\(.+\)", query, re.IGNORECASE):
            match = re.match(r"POP DANS\s+(\w+)\s+VALEURS\s*\((.+)\)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Tu cheat, il faut le format : POP DANS table VALEURS (val1, val2, ...)")
            table_name, values = match.groups()
            values = [val.strip().strip("'") if val.strip().startswith("'") and val.strip().endswith("'") else val.strip() for val in values.split(",")]
            return {"type": "insert", "table_name": table_name, "values": values}
        
        elif re.match(r"LOOT\s+.+\s+DANS\s+\w+(?:(?:\s*,\s*\w+)?\s*AVEC\s*.+)?(?:\s*TRIER\s+PAR\s*.+)?", query, re.IGNORECASE):
            # Pattern qui supporte LOOT * et LOOT col1, col2
            pattern = r"LOOT\s+(.+?)\s+DANS\s+(\w+)(?:\s*,\s*(\w+))?(?:\s*AVEC\s+(.+?))?(?:\s*TRIER\s+PAR\s+(.+))?$"
            match = re.match(pattern, query, re.IGNORECASE)
            if not match:
                raise ValueError("Syntaxe LOOT invalide")
            
            columns_str, table1, table2, condition, order_by_str = match.groups()
            
            # Parser les colonnes
            if columns_str.strip() == "*":
                columns = "*"
            else:
                columns = [col.strip() for col in columns_str.split(",")]
            
            # Parser ORDER BY
            order_by_cols = []
            if order_by_str:
                parts = re.split(r'\s*,\s*', order_by_str.strip())
                for part in parts:
                    match_order = re.match(r"((?:\w+\.)?\w+)\s*(ASC|DESC)?$", part.strip(), re.IGNORECASE)
                    if not match_order:
                        raise ValueError(f"TRIER PAR invalide : {part}")
                    col, direction = match_order.groups()
                    order_by_cols.append({
                        "column": col.strip(),
                        "direction": direction.upper() if direction else "ASC"
                    })

            if table2:
                return {"type": "join_tables", "table1": table1, "table2": table2, "columns": columns, "join_condition": condition, "order_by": order_by_cols}
            return {"type": "select", "table_name": table1, "columns": columns, "condition": condition, "order_by": order_by_cols}
        
        elif re.match(r"EDIT\s+\w+\s+DEFINIR\s+\w+\s*=\s*'.*'\s*AVEC\s*.+", query, re.IGNORECASE):
            match = re.match(r"EDIT\s+(\w+)\s+DEFINIR\s+(\w+\s*=\s*'[^']*')\s*AVEC\s*(.+)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Syntaxe cheatée ! Format : EDIT table DEFINIR col = 'val' AVEC condition")
            table_name, set_clause, condition = match.groups()
            return {"type": "update", "table_name": table_name, "set_clause": set_clause, "condition": condition}
        
        elif re.match(r"DEPOP DANS\s+\w+\s*AVEC\s*.+", query, re.IGNORECASE):
            match = re.match(r"DEPOP DANS\s+(\w+)\s*AVEC\s*(.+)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Tu cheat, il faut le format : DEPOP DANS table AVEC condition")
            table_name, condition = match.groups()
            return {"type": "delete", "table_name": table_name, "condition": condition}
        
        elif re.match(r"STATS TABLEAU\s+\w+", query, re.IGNORECASE):
            match = re.match(r"STATS TABLEAU\s+(\w+)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Tu cheat, il faut le format : STATS TABLEAU nom")
            return {"type": "table_stats", "table_name": match.groups()[0]}
        
        elif re.match(r"AIDE(?:\s+.+)?", query, re.IGNORECASE):
            match = re.match(r"AIDE\s*(.+)?", query, re.IGNORECASE)
            command = match.groups()[0] if match.groups()[0] else None
            return {"type": "show_help", "command": command}
        
        elif re.match(r"QUITTER", query, re.IGNORECASE):
            return {"type": "quit"}
        

        elif re.match(r"DEBUT TRANSACTION", query, re.IGNORECASE):
            return {"type": "begin_transaction"}
        
        elif re.match(r"VALIDER TRANSACTION", query, re.IGNORECASE):
            return {"type": "commit_transaction"}
        
        elif re.match(r"ANNULER TRANSACTION", query, re.IGNORECASE):
            return {"type": "rollback_transaction"}

        elif re.match(r"STATUS TRANSACTION", query, re.IGNORECASE):
            return {"type": "transaction_status"}

        elif re.match(r"CRAFTER VUE\s+\w+\s+COMME\s+\".*\"", query, re.IGNORECASE):
            match = re.match(r"CRAFTER VUE\s+(\w+)\s+COMME\s+\"(.+)\"", query, re.IGNORECASE)
            if not match:
                raise ValueError("Format : CRAFTER VUE nom COMME \"requête LOOT\"")
            view_name, view_query = match.groups()
            return {"type": "create_view", "view_name": view_name, "query": view_query}

        elif re.match(r"DEPOP VUE\s+\w+", query, re.IGNORECASE):
            match = re.match(r"DEPOP VUE\s+(\w+)", query, re.IGNORECASE)
            return {"type": "delete_view", "view_name": match.groups()[0]}

        elif re.match(r"LISTE VUES", query, re.IGNORECASE):
            return {"type": "list_views"}
        
        elif re.match(r"SNAPSHOT TABLEAU\s+\w+\s+VERSION\s+'.*'", query, re.IGNORECASE):
            match = re.match(r"SNAPSHOT TABLEAU\s+(\w+)\s+VERSION\s+'([^']*)'", query, re.IGNORECASE)
            if not match:
                raise ValueError("Format: SNAPSHOT TABLEAU nom VERSION 'description'")
            table_name, description = match.groups()
            return {"type": "create_snapshot", "table_name": table_name, "description": description}

        elif re.match(r"VOYAGE TABLEAU\s+\w+\s+VERSION\s+\w+", query, re.IGNORECASE):
            match = re.match(r"VOYAGE TABLEAU\s+(\w+)\s+VERSION\s+(\w+)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Format: VOYAGE TABLEAU nom VERSION id_snapshot")
            table_name, snapshot_id = match.groups()
            return {"type": "restore_snapshot", "table_name": table_name, "snapshot_id": snapshot_id}

        elif re.match(r"VOIR SNAPSHOT\s+\w+\s+VERSION\s+\w+", query, re.IGNORECASE):
            match = re.match(r"VOIR SNAPSHOT\s+(\w+)\s+VERSION\s+(\w+)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Format: VOIR SNAPSHOT nom VERSION id_snapshot")
            table_name, snapshot_id = match.groups()
            return {"type": "view_snapshot", "table_name": table_name, "snapshot_id": snapshot_id}

        elif re.match(r"LISTE SNAPSHOTS TABLEAU\s+\w+", query, re.IGNORECASE):
            match = re.match(r"LISTE SNAPSHOTS TABLEAU\s+(\w+)", query, re.IGNORECASE)
            return {"type": "list_snapshots", "table_name": match.groups()[0]}

        elif re.match(r"DEPOP SNAPSHOT TABLEAU\s+\w+\s+VERSION\s+\w+", query, re.IGNORECASE):
            match = re.match(r"DEPOP SNAPSHOT TABLEAU\s+(\w+)\s+VERSION\s+(\w+)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Format: DEPOP SNAPSHOT TABLEAU nom VERSION id_snapshot")
            table_name, snapshot_id = match.groups()
            return {"type": "delete_snapshot", "table_name": table_name, "snapshot_id": snapshot_id}

        elif re.match(r"HISTORIQUE QUETE\s+\w+", query, re.IGNORECASE):
            match = re.match(r"HISTORIQUE QUETE\s+(\w+)", query, re.IGNORECASE)
            return {"type": "quest_history", "quest_name": match.groups()[0]}

        elif re.match(r"RESULTATS QUETE\s+\w+\s+EXECUTION\s+\w+", query, re.IGNORECASE):
            match = re.match(r"RESULTATS QUETE\s+(\w+)\s+EXECUTION\s+(\w+)", query, re.IGNORECASE)
            return {"type": "quest_results", "quest_name": match.groups()[0], "execution_id": match.groups()[1]}

        elif re.match(r"CRAFTER QUETE\s+(\w+)\s+\"([^\"]+)\"\s+CHAQUE\s+(.+)", query, re.IGNORECASE):
            match = re.match(r"CRAFTER QUETE\s+(\w+)\s+\"([^\"]+)\"\s+CHAQUE\s+(.+)", query, re.IGNORECASE)
            if not match:
                raise ValueError("Format: CRAFTER QUETE nom \"requête LOOT\" CHAQUE nombre unité")
            


            quest_name, quest_query, interval = match.groups()
            


            # Valider l'intervalle
            valid_intervals = ["1 JOURS", "1 HEURES", "30 MINUTES", "1 SEMAINE"]

            if interval not in valid_intervals:
                raise ValueError(f"Intervalle invalide. Options: {', '.join(valid_intervals)}")
            

            return {"type": "create_quest", "quest_name": quest_name, "query": quest_query, "interval": interval}
        
        elif re.match(r"EXECUTER QUETE\s+\w+", query, re.IGNORECASE):
            match = re.match(r"EXECUTER QUETE\s+(\w+)", query, re.IGNORECASE)
            return {"type": "execute_quest", "quest_name": match.groups()[0]}

        elif re.match(r"LISTE QUETES", query, re.IGNORECASE):
            return {"type": "list_quests"}

        elif re.match(r"DEPOP QUETE\s+\w+", query, re.IGNORECASE):
            match = re.match(r"DEPOP QUETE\s+(\w+)", query, re.IGNORECASE)
            return {"type": "delete_quest", "quest_name": match.groups()[0]}

        elif re.match(r"DEMARRER QUETES", query, re.IGNORECASE):
            return {"type": "start_quests"}

        else:

            raise ValueError("Sort inconnu ! Check ton grimoire SQL")