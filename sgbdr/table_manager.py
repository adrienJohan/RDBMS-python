import json
import re
from pathlib import Path
from datetime import datetime

class TableManager:
    def __init__(self, db_path, sgbdr):
        self.db_path = db_path
        self.sgbdr = sgbdr

    def set_sgbdr(self, sgbdr):
        """Définir la référence à l’instance SGBDR"""
        self.sgbdr = sgbdr

    def create_table(self, table_name, columns):
        self.sgbdr.user_manager.check_permission("write")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        db_dir = self.db_path / self.sgbdr.current_db

        with open(db_dir / "metadata.json", "r") as f:
            metadata = json.load(f)

        if table_name in metadata["tables"]:
            raise ValueError(f"Table {table_name} existe déjà.")

        parsed_columns = {}
        primary_key = None
        foreign_keys = {}
        unique_cols = set()
        not_null_cols = set()

        for col in columns:
            parts = col.split()
            col_name = parts[0]
            col_type = parts[1].upper()
            constraints =  parts[2:]
            constraints_upper = [c.upper() for c in constraints]

            # Type + taille pour VARCHAR
            if col_type.startswith("VARCHAR("):
                try:
                    size = int(col_type.split("(")[1].split(")")[0])
                    col_type = "VARCHAR"
                except:
                    raise ValueError("VARCHAR(n) invalide")
            else:
                size = None

            if col_type not in ("INT", "FLOAT", "TEXT", "DATE", "BOOLEAN", "VARCHAR"):
                raise ValueError(f"Type {col_type} non supporté")

            # Contrainte
            if "PRIMARY" in constraints_upper and "KEY" in constraints_upper:
                if primary_key:
                    raise ValueError("Une seule PRIMARY KEY")
                primary_key = col_name
                unique_cols.add(col_name)
                not_null_cols.add(col_name)
            if "NOT" in constraints_upper and "NULL" in constraints_upper:
                not_null_cols.add(col_name)
            if "REFERENCES" in constraints_upper:
                ref_index = constraints.index("REFERENCES")
                ref = " ".join(constraints[ref_index:])
                match = re.match(r"REFERENCES\s+(\w+)\((\w+)\)", ref)
                if not match:
                    raise ValueError("REFERENCES table(col)")
                ref_table, ref_col = match.groups()
                foreign_keys[col_name] = {"table": ref_table, "column": ref_col}

            parsed_columns[col_name] = {
                "type": col_type,
                "nullable": "NOT" not in constraints or "NULL" not in constraints,
                "size": size if col_type == "VARCHAR" else None
            }

        if primary_key:
            unique_cols.add(primary_key)

        metadata["tables"][table_name] = {
            "columns": parsed_columns,
            "constraints": {
                "primary_key": primary_key,
                "foreign_keys": foreign_keys,
                "unique": list(unique_cols),
                "not_null": list(not_null_cols)
            }
        }

        with open(db_dir / "metadata.json", "w") as f:
            json.dump(metadata, f, indent=2)

        # Créer fichier vide
        with open(db_dir / f"{table_name}.json", "w") as f:
            json.dump([], f, indent=2)

        print(f"╔════════════════════════════════════")
        print(f"║ Table {table_name} craftée !")
        print(f"║ Colonnes : {parsed_columns}")
        print(f"║ Contraintes : {metadata['tables'][table_name]['constraints']}")
        print(f"╚════════════════════════════════════")

    def delete_table(self, table_name):
        """Supprimer une table"""
        self.sgbdr.user_manager.check_permission("delete")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée. Faut d'abord switcher vers la base")
        db_dir = self.db_path / self.sgbdr.current_db
        
        with open(db_dir / "metadata.json", "r+") as f:
            metadata = json.load(f)
            
            if table_name not in metadata["tables"]:
                raise ValueError(f"Table {table_name} introuvable. T’as raté la map ?")
                
            # Vérifier les références étrangères
            for other_table, table_data in metadata["tables"].items():
                if other_table != table_name:
                    for col, fk in table_data.get("constraints", {}).get("foreign_keys", {}).items():
                        if fk["table"] == table_name:
                            raise ValueError(f"Table {table_name} est référencée par {other_table} ! Supprime les clés étrangères d'abord.")
            
            # CORRECTION : Le fichier est toujours table_name.json
            data_file = db_dir / f"{table_name}.json"
            if data_file.exists():
                data_file.unlink()
                
            del metadata["tables"][table_name]
            f.seek(0)
            json.dump(metadata, f, indent=2)
            f.truncate()
            
        print(f"╔════════════════════════════════════")
        print(f"║ Tableau {table_name} détruit")
        print(f"╚════════════════════════════════════")

    def list_tables(self):
        """Lister toutes les tables de la base actuelle"""
        self.sgbdr.user_manager.check_permission("read")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée. Faut d'abord switcher vers la base")
        db_dir = self.db_path / self.sgbdr.current_db
        with open(db_dir / "metadata.json", "r") as f:
            metadata = json.load(f)
            tables = [{"name": name, "columns": data["columns"], "constraints": data.get("constraints", {})} for name, data in metadata["tables"].items()]
        print(f"╔════════════════════════════════════")
        print(f"║ Tables craftées dans {self.sgbdr.current_db} : {len(tables)} trouvées !")
        print(f"╚════════════════════════════════════")
        return tables
    
    def create_view(self, view_name, query):
        """Créer une vue (table virtuelle)"""
        self.sgbdr.user_manager.check_permission("write")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        
        with open(db_dir / "metadata.json", "r+") as f:
            metadata = json.load(f)
            
            if view_name in metadata["tables"]:
                raise ValueError(f"Une table ou vue nommée {view_name} existe déjà.")
            
            # Valider que la requête est un SELECT valide
            if not query.upper().startswith("LOOT"):
                raise ValueError("Une vue doit être basée sur une requête LOOT valide.")
            
            # Stocker la vue dans les métadonnées
            if "views" not in metadata:
                metadata["views"] = {}
                
            metadata["views"][view_name] = {
                "query": query,
                "created_by": self.sgbdr.current_user,
                "created_at": datetime.now().isoformat()
            }
            
            f.seek(0)
            json.dump(metadata, f, indent=2)
            f.truncate()
        
        print(f"╔════════════════════════════════════")
        print(f"║ Vue {view_name} craftée !")
        print(f"║ Requête : {query}")
        print(f"╚════════════════════════════════════")

    def delete_view(self, view_name):
        """Supprimer une vue"""
        self.sgbdr.user_manager.check_permission("write")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        
        with open(db_dir / "metadata.json", "r+") as f:
            metadata = json.load(f)
            
            if "views" not in metadata or view_name not in metadata["views"]:
                raise ValueError(f"Vue {view_name} introuvable.")
            
            del metadata["views"][view_name]
            f.seek(0)
            json.dump(metadata, f, indent=2)
            f.truncate()
        
        print(f"╔════════════════════════════════════")
        print(f"║ Vue {view_name} supprimée !")
        print(f"╚════════════════════════════════════")

    def list_views(self):
        """Lister toutes les vues de la base"""
        self.sgbdr.user_manager.check_permission("read")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        
        with open(db_dir / "metadata.json", "r") as f:
            metadata = json.load(f)
            
            views = metadata.get("views", {})
            result = [{"name": name, "query": data["query"], "created_by": data.get("created_by", "inconnu")} 
                    for name, data in views.items()]
        
        print(f"╔════════════════════════════════════")
        print(f"║ Vues craftées dans {self.sgbdr.current_db} : {len(result)} trouvées !")
        print(f"╚════════════════════════════════════")
        return result