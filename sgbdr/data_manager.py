# sgbdr/data_manager.py
import json
import re
from datetime import datetime
from pathlib import Path
from .utils import evaluate_condition

class DataManager:
    def __init__(self, db_path, sgbdr):
        self.db_path = db_path
        self.sgbdr = sgbdr

    def set_sgbdr(self, sgbdr):
        """Définir la référence à l’instance SGBDR"""
        self.sgbdr = sgbdr

    def insert(self, table_name, values):
        """Insérer une ligne avec support BOOLEAN et VARCHAR(n)"""
        self.sgbdr.user_manager.check_permission("write")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée. Faut d'abord switcher vers la base")
        
        db_dir = self.db_path / self.sgbdr.current_db
        table_path = db_dir / f"{table_name}.json"
        if not table_path.exists():
            raise ValueError(f"Table {table_name} introuvable. T’as raté la map ?")

        # Charger métadonnées
        with open(db_dir / "metadata.json", "r") as f:
            metadata = json.load(f)
        columns = metadata["tables"][table_name]["columns"]
        constraints = metadata["tables"][table_name]["constraints"]

        if len(values) != len(columns):
            raise ValueError(f"Nombre de valeurs ({len(values)}) ≠ colonnes ({len(columns)})")

        row = {}
        for (col_name, col_info), val in zip(columns.items(), values):
            col_type = col_info["type"]
            nullable = col_info["nullable"]
            size = col_info.get("size")  # pour VARCHAR

            # --- Validation NOT NULL ---
            if val == "null" and not nullable:
                raise ValueError(f"La colonne {col_name} ne peut pas être NULL")

            # --- Validation par type ---
            if val != "null":
                if col_type == "INT":
                    if not (val.lstrip("-").isdigit()):
                        raise ValueError(f"{col_name} doit être un INT")
                    row[col_name] = val
                elif col_type == "FLOAT":
                    try:
                        float(val)
                    except ValueError:
                        raise ValueError(f"{col_name} doit être un FLOAT")
                    row[col_name] = val
                elif col_type in ("TEXT", "VARCHAR"):
                    if col_type == "VARCHAR" and size is not None and len(val) > size:
                        raise ValueError(f"{col_name} trop long (max {size} caractères)")
                    row[col_name] = val
                elif col_type == "BOOLEAN":
                    if val.lower() not in ("true", "false"):
                        raise ValueError(f"{col_name} doit être TRUE ou FALSE")
                    row[col_name] = val.lower()  # stocké en minuscules
                elif col_type == "DATE":
                    if not re.match(r"^\d{4}-\d{2}-\d{2}$", val):
                        raise ValueError(f"{col_name} doit être YYYY-MM-DD")
                    try:
                        datetime.strptime(val, "%Y-%m-%d")
                    except ValueError:
                        raise ValueError(f"Date invalide dans {col_name}")
                    row[col_name] = val
                else:
                    row[col_name] = val
            else:
                row[col_name] = "null"

        # --- Charger données existantes ---
        with open(table_path, "r") as f:
            data = json.load(f)

        # --- Contraintes ---
        # PRIMARY KEY
        if constraints["primary_key"]:
            pk = constraints["primary_key"]
            if any(d.get(pk) == row[pk] for d in data if row[pk] != "null"):
                raise ValueError(f"Valeur {row[pk]} déjà prise pour la clé primaire {pk}")

        # UNIQUE
        for col in constraints["unique"]:
            if row[col] != "null":
                if any(d.get(col) == row[col] for d in data):
                    raise ValueError(f"Valeur {row[col]} déjà prise pour la colonne unique {col}")

        # FOREIGN KEY
        for col, fk in constraints["foreign_keys"].items():
            if row[col] != "null":
                ref_table = fk["table"]
                ref_col = fk["column"]
                ref_path = db_dir / f"{ref_table}.json"
                if not ref_path.exists():
                    raise ValueError(f"Table référencée {ref_table} introuvable")
                with open(ref_path, "r") as f:
                    ref_data = json.load(f)
                if not any(d.get(ref_col) == row[col] for d in ref_data):
                    raise ValueError(f"Valeur {row[col]} dans {col} n'existe pas dans {ref_table}.{ref_col}")


        data.append(row)
        with open(table_path, "w") as f:
            json.dump(data, f, indent=2)

        print(f"╔════════════════════════════════════")
        print(f"║ 1 loot ajouté dans {table_name} !")
        print(f"╚════════════════════════════════════")

    def _get_column_type(self, table_name, column_name):
        """Utilitaire pour récupérer le type d'une colonne depuis metadata"""
        db_dir = self.db_path / self.sgbdr.current_db
        with open(db_dir / "metadata.json", "r") as f:
            metadata = json.load(f)
        return metadata["tables"][table_name]["columns"][column_name]["type"]

    def select(self, table, selected_columns="*", condition=None, order_by=None):
        """Sélectionner des données avec tri robuste"""
        self.sgbdr.user_manager.check_permission("read")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        db_dir = self.db_path / self.sgbdr.current_db
        table_path = db_dir / f"{table}.json"
        if not table_path.exists():
            raise ValueError(f"Table {table} introuvable.")

        with open(db_dir / "metadata.json", "r") as f:
            metadata = json.load(f)
        table_columns = metadata["tables"][table]["columns"]  # CHANGER columns → table_columns

        with open(table_path, "r") as f:
            data = json.load(f)

        # Filtrer
        if condition:
            filtered_data = [row for row in data if evaluate_condition(row, condition, table_columns)]  # CHANGER columns → table_columns
        else:
            filtered_data = data[:]

        # Filtrer les colonnes si spécifiées
        if selected_columns != "*":  # CHANGER columns → selected_columns
            
            result_data = []
            for row in filtered_data:
                filtered_row = {}
                for col in selected_columns:  # CHANGER columns → selected_columns
                    col_name = col.strip()
                    if col_name in row:
                        filtered_row[col_name] = row[col_name]
                    else:
                        # Essayer de trouver la colonne sans préfixe de table
                        simple_col = col_name.split('.')[-1] if '.' in col_name else col_name
                        if simple_col in row:
                            filtered_row[col_name] = row[simple_col]
                        else:
                            # Colonne non trouvée, mettre None
                            filtered_row[col_name] = None

                result_data.append(filtered_row)
            filtered_data = result_data


        # ORDER BY
        if order_by:
            def sort_key(row):
                keys = []
                for order in order_by:
                    col = order["column"]
                    direction = order["direction"]


                    if col not in table_columns and col not in [c.split('.')[-1] for c in table_columns.keys()]:
                        raise ValueError(f"Colonne {col} introuvable dans {table}")

                    value = row.get(col, "null")

                    col_type = table_columns.get(col, {}).get("type", "TEXT")

                    # Gestion NULL
                    if value == "null":
                        sort_val = (0, None)
                    else:
                        if col_type == "INT":
                            sort_val = (1, int(value))
                        elif col_type == "FLOAT":
                            sort_val = (1, float(value))
                        elif col_type == "DATE":
                            sort_val = (1, datetime.strptime(value, "%Y-%m-%d"))
                        elif col_type in ("TEXT", "VARCHAR"):  # VARCHAR ajouté ici
                            sort_val = (1, value.lower())  # insensible à la casse
                        elif col_type == "BOOLEAN":
                            sort_val = (1, value.lower() == "true")
                        else:
                            sort_val = (1, str(value))

                    # Inversion pour DESC
                    if direction == "DESC":
                        if sort_val[1] is None:
                            sort_val = (2, None)  # null en dernier
                        elif isinstance(sort_val[1], (int, float)):
                            sort_val = (sort_val[0], -sort_val[1])
                        elif isinstance(sort_val[1], datetime):
                            sort_val = (sort_val[0], -sort_val[1].timestamp())
                        elif isinstance(sort_val[1], bool):
                            sort_val = (sort_val[0], not sort_val[1])
                        # TEXT et VARCHAR : on garde, on inverse après

                    keys.append(sort_val)
                return tuple(keys)

            # Tri final
            filtered_data = sorted(filtered_data, key=sort_key)

            # Pour DESC sur TEXT/VARCHAR : on inverse si nécessaire
            if any(order["direction"] == "DESC" and table_columns[order["column"]]["type"] in ("TEXT", "VARCHAR") for order in order_by):
                # Si DESC sur TEXT/VARCHAR, on inverse l'ordre
                filtered_data = filtered_data[::-1]

        print(f"╔════════════════════════════════════")
        print(f"║ Loot dans {table} : {len(filtered_data)} lignes trouvées !")
        print(f"╚════════════════════════════════════")
        return filtered_data


    def join_tables(self, table1, table2, columns="*", join_condition=None, order_by=None):
        """Jointure avec ORDER BY — TEXT/DESC corrigé"""
        self.sgbdr.user_manager.check_permission("read")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        db_dir = self.db_path / self.sgbdr.current_db

        if not (db_dir / f"{table1}.json").exists() or not (db_dir / f"{table2}.json").exists():
            raise ValueError("Une des tables est introuvable.")

        match = re.match(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", join_condition.strip())
        if not match:
            raise ValueError("Condition de jointure invalide")
        t1, c1, t2, c2 = match.groups()
        if t1 != table1 or t2 != table2:
            raise ValueError("Les tables dans la condition doivent correspondre.")

        with open(db_dir / f"{table1}.json", "r") as f:
            data1 = json.load(f)
        with open(db_dir / f"{table2}.json", "r") as f:
            data2 = json.load(f)

        with open(db_dir / "metadata.json", "r") as f:
            metadata = json.load(f)

        result = []
        for r1 in data1:
            for r2 in data2:
                if str(r1.get(c1)) == str(r2.get(c2)):
                    row = {f"{table1}.{k}": v for k, v in r1.items()}
                    row.update({f"{table2}.{k}": v for k, v in r2.items()})
                    result.append(row)

        
        # CORRECTION : Extraire les conditions supplémentaires de join_condition
        join_parts = join_condition.split(" ET ")
        base_join_condition = join_parts[0]  # employes.id = projets.responsable_id
        additional_conditions = " ET ".join(join_parts[1:]) if len(join_parts) > 1 else None

        # Appliquer les conditions supplémentaires si elles existent
        if additional_conditions:

            
            # Créer les métadonnées des colonnes pour les conditions
            columns_metadata = {}
            for table in [table1, table2]:
                if table in metadata["tables"]:
                    for col_name, col_info in metadata["tables"][table]["columns"].items():
                        prefixed_col = f"{table}.{col_name}"
                        columns_metadata[prefixed_col] = col_info
                        # Ajouter aussi le nom simple pour compatibilité
                        columns_metadata[col_name] = col_info
            
            # Filtrer les résultats avec les conditions supplémentaires
            from .utils import evaluate_condition
            filtered_result = []
            for row in result:
                try:
                    if evaluate_condition(row, additional_conditions, columns_metadata):
                        filtered_result.append(row)
                except Exception as e:

                    # En cas d'erreur, on garde la ligne pour éviter de tout perdre
                    filtered_result.append(row)
            
            result = filtered_result


        # Filtrer les colonnes si spécifiées
        if columns != "*":
            filtered_result = []
            for row in result:
                filtered_row = {}
                for col in columns:
                    col_name = col.strip()
                    if col_name in row:
                        filtered_row[col_name] = row[col_name]
                    else:
                        # Essayer de trouver la colonne sans préfixe de table
                        simple_col = col_name.split('.')[-1] if '.' in col_name else col_name
                        found = False
                        for key in row.keys():
                            if key == simple_col or key.endswith('.' + simple_col):
                                filtered_row[col_name] = row[key]
                                found = True
                                break
                        if not found:
                            filtered_row[col_name] = None
                filtered_result.append(filtered_row)
            result = filtered_result       

        
        if order_by:
            def sort_key(row):
                keys = []
                for order in order_by:
                    full_col = order["column"]
                    direction = order["direction"]

                    if "." in full_col:
                        table_name, col_name = full_col.split(".", 1)
                        prefixed = f"{table_name}.{col_name}"
                        if table_name not in (table1, table2):
                            raise ValueError(f"Table {table_name} inconnue")
                    else:
                        col_name = full_col
                        prefixed1 = f"{table1}.{col_name}"
                        prefixed2 = f"{table2}.{col_name}"
                        if prefixed1 in row:
                            prefixed = prefixed1
                            table_name = table1
                        elif prefixed2 in row:
                            prefixed = prefixed2
                            table_name = table2
                        else:
                            raise ValueError(f"Colonne {full_col} introuvable")

                    value = row.get(prefixed, "null")
                    col_type = metadata["tables"][table_name]["columns"][col_name]["type"]

                    if value == "null":
                        sort_val = (0, None)
                    else:
                        if col_type == "INT":
                            sort_val = (1, int(value))
                        elif col_type == "FLOAT":
                            sort_val = (1, float(value))
                        elif col_type == "DATE":
                            sort_val = (1, datetime.strptime(value, "%Y-%m-%d"))
                        elif col_type in ("TEXT", "VARCHAR"):
                            sort_val = (1, value.lower())
                        elif col_type == "BOOLEAN":
                            sort_val = (1, value.lower() == "true")
                        else:
                            sort_val = (1, str(value))

                    # DESC : seulement pour numériques
                    if direction == "DESC":
                        if sort_val[1] is None:
                            sort_val = (2, None)
                        elif isinstance(sort_val[1], (int, float)):
                            sort_val = (sort_val[0], -sort_val[1])
                        elif isinstance(sort_val[1], datetime):
                            sort_val = (sort_val[0], -sort_val[1].timestamp())
                        elif isinstance(sort_val[1], bool):
                            sort_val = (sort_val[0], not sort_val[1])
                        # TEXT : on inverse après

                    keys.append(sort_val)
                return tuple(keys)

            result = sorted(result, key=sort_key)

            # Inversion manuelle pour TEXT/VARCHAR en DESC
            has_text_desc = any(
                order["direction"] == "DESC" and 
                "." in order["column"] and
                metadata["tables"][order["column"].split(".", 1)[0]]["columns"][order["column"].split(".", 1)[1]]["type"] in ("TEXT", "VARCHAR")
                for order in order_by
            )
            if has_text_desc:
                result = result[::-1]

        print(f"╔════════════════════════════════════")
        print(f"║ Jointure : {len(result)} lignes trouvées !")
        print(f"╚════════════════════════════════════")
        return result

    def update(self, table_name, set_clause, condition):
        """Mettre à jour des lignes avec BOOLEAN et VARCHAR"""
        self.sgbdr.user_manager.check_permission("write")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        table_path = db_dir / f"{table_name}.json"
        if not table_path.exists():
            raise ValueError(f"Table {table_name} introuvable.")

        with open(db_dir / "metadata.json", "r") as f:
            metadata = json.load(f)
        columns = metadata["tables"][table_name]["columns"]
        constraints = metadata["tables"][table_name]["constraints"]

        # Parser SET col = 'val'
        match = re.match(r"(\w+)\s*=\s*'([^']*)'", set_clause)
        if not match:
            raise ValueError("Syntaxe SET invalide : col = 'val'")
        col_name, new_val = match.groups()

        if col_name not in columns:
            raise ValueError(f"Colonne {col_name} introuvable dans {table_name}")

        col_type = columns[col_name]["type"]
        nullable = columns[col_name]["nullable"]
        size = columns[col_name].get("size")

        # --- Validation de la nouvelle valeur ---
        if new_val == "null" and not nullable:
            raise ValueError(f"{col_name} ne peut pas être NULL")
        
        if new_val != "null":
            if col_type == "INT":
                if not (new_val.lstrip("-").isdigit()):
                    raise ValueError(f"{col_name} doit être un INT")
            elif col_type == "FLOAT":
                try:
                    float(new_val)
                except ValueError:
                    raise ValueError(f"{col_name} doit être un FLOAT")
            elif col_type in ("TEXT", "VARCHAR"):
                if col_type == "VARCHAR" and size is not None and len(new_val) > size:
                    raise ValueError(f"{col_name} trop long (max {size})")
            elif col_type == "BOOLEAN":
                if new_val.lower() not in ("true", "false"):
                    raise ValueError(f"{col_name} doit être TRUE ou FALSE")
                new_val = new_val.lower()
            elif col_type == "DATE":
                if not re.match(r"^\d{4}-\d{2}-\d{2}$", new_val):
                    raise ValueError(f"{col_name} doit être YYYY-MM-DD")
                try:
                    datetime.strptime(new_val, "%Y-%m-%d")
                except ValueError:
                    raise ValueError(f"Date invalide")

        # --- Charger données ---
        with open(table_path, "r+") as f:
            data = json.load(f)
            updated_count = 0

            for row in data:
                if evaluate_condition(row, condition, columns):
                    old_val = row[col_name]

                    # Vérifier contrainte UNIQUE (si changement)
                    if old_val != new_val and new_val != "null":
                        for uniq_col in constraints["unique"]:
                            if uniq_col == col_name:
                                if any(d.get(col_name) == new_val and d is not row for d in data):
                                    raise ValueError(f"Valeur {new_val} déjà prise pour {col_name}")

                    # Vérifier FOREIGN KEY
                    if col_name in constraints["foreign_keys"] and new_val != "null":
                        fk = constraints["foreign_keys"][col_name]
                        ref_table = fk["table"]
                        ref_col = fk["column"]
                        ref_path = db_dir / f"{ref_table}.json"
                        with open(ref_path, "r") as rf:
                            ref_data = json.load(rf)
                        if not any(d.get(ref_col) == new_val for d in ref_data):
                            raise ValueError(f"Valeur {new_val} n'existe pas dans {ref_table}.{ref_col}")

                    row[col_name] = new_val
                    updated_count += 1

            f.seek(0)
            json.dump(data, f, indent=2)
            f.truncate()

        print(f"╔════════════════════════════════════")
        print(f"║ {updated_count} lignes modifiées dans {table_name} !")
        print(f"╚════════════════════════════════════")


    def delete(self, table_name, condition):
        """Supprimer des lignes dans une table"""
        self.sgbdr.user_manager.check_permission("delete")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée. Faut d'abord switcher vers la base")
        db_dir = self.db_path / self.sgbdr.current_db
        if not (db_dir / f"{table_name}.json").exists():
            raise ValueError(f"Table {table_name} introuvable. T’as raté la map ?")
        
        with open(db_dir / "metadata.json", "r") as f:
            metadata = json.load(f)
        columns = metadata["tables"][table_name]["columns"]
        
        with open(db_dir / f"{table_name}.json", "r+") as f:
            data = json.load(f)
            new_data = [row for row in data if not evaluate_condition(row, condition, columns)]
            deleted_count = len(data) - len(new_data)
            f.seek(0)
            json.dump(new_data, f,indent=2)
            f.truncate()
        
        print(f"╔════════════════════════════════════")
        print(f"║ {deleted_count} lignes supprimées dans {table_name} !")
        print(f"╚════════════════════════════════════")

    def table_stats(self, table_name):
        """Afficher des statistiques sur une table"""
        self.sgbdr.user_manager.check_permission("read")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée. Faut d'abord switcher vers la base")
        db_dir = self.db_path / self.sgbdr.current_db
        if not (db_dir / f"{table_name}.json").exists():
            raise ValueError(f"Table {table_name} introuvable. T’as raté la map ?")
        
        with open(db_dir / "metadata.json", "r") as f:
            metadata = json.load(f)
        columns = metadata["tables"][table_name]["columns"]
        
        with open(db_dir / f"{table_name}.json", "r") as f:
            data = json.load(f)
        
        stats = {"row_count": len(data)}
        for col, col_info in columns.items():
            col_type = col_info["type"]
            values = [row[col] for row in data if row[col] != "null"]
            stats[col] = {}
            if col_type in ("INT", "FLOAT"):
                if values:
                    numeric_values = [float(v) for v in values]
                    stats[col]["min"] = min(numeric_values)
                    stats[col]["max"] = max(numeric_values)
                    stats[col]["avg"] = sum(numeric_values) / len(numeric_values)
                else:
                    stats[col]["min"] = "N/A"
                    stats[col]["max"] = "N/A"
                    stats[col]["avg"] = "N/A"
            if col_type in ("TEXT", "DATE"):
                stats[col]["distinct_count"] = len(set(values))
        
        print(f"╔════════════════════════════════════")
        print(f"║ Statistiques pour {table_name}")
        print(f"╠════════════════════════════════════")
        print(f"║ Nombre de lignes : {stats['row_count']}")
        for col, col_stats in stats.items():
            if col != "row_count":
                print(f"║ Colonne {col} ({columns[col]['type']}) :")
                if col_stats.get("min") is not None:
                    print(f"║   Min : {col_stats['min']}")
                    print(f"║   Max : {col_stats['max']}")
                    print(f"║   Moyenne : {col_stats['avg']}")
                if col_stats.get("distinct_count") is not None:
                    print(f"║   Valeurs distinctes : {col_stats['distinct_count']}")
        print(f"╚════════════════════════════════════")
        return stats


    def execute_view(self, view_name, condition=None, order_by=None):
        """Exécuter une vue avec conditions et tri supplémentaires"""
        self.sgbdr.user_manager.check_permission("read")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        
        # Charger la définition de la vue
        with open(db_dir / "metadata.json", "r") as f:
            metadata = json.load(f)
            
            if "views" not in metadata or view_name not in metadata["views"]:
                raise ValueError(f"Vue {view_name} introuvable.")
            
            view_query = metadata["views"][view_name]["query"]
        
        # Exécuter la requête de la vue via le SGBDR
        print(f"╔════════════════════════════════════")
        print(f"║ Exécution de la vue {view_name}...")
        print(f"╚════════════════════════════════════")
        
        # Exécuter la vue de base
        base_result = self.sgbdr.execute_query(view_query)
        
        # Récupérer les métadonnées des colonnes de la vue
        view_columns = self._get_view_columns_metadata(view_name, base_result)
        
        # Appliquer les conditions et tri supplémentaires si présents
        if condition or order_by:
            filtered_result = self._apply_additional_filters(base_result, condition, order_by, view_columns)
            return filtered_result
        
        return base_result

    def _get_view_columns_metadata(self, view_name, sample_data):
        """Déduire les types de colonnes à partir des données de la vue"""
        if not sample_data:
            return {}
        
        columns_metadata = {}
        
        # Analyser le premier élément pour deviner les types
        first_row = sample_data[0]
        
        for col_name, value in first_row.items():
            if value == "null":
                columns_metadata[col_name] = {"type": "TEXT"}  # Type par défaut
            else:
                # Deviner le type basé sur la valeur
                if value.lower() in ("true", "false"):
                    columns_metadata[col_name] = {"type": "BOOLEAN"}
                elif re.match(r"^\d{4}-\d{2}-\d{2}$", value):
                    columns_metadata[col_name] = {"type": "DATE"}
                elif value.replace('.', '').replace('-', '').isdigit():
                    if '.' in value:
                        columns_metadata[col_name] = {"type": "FLOAT"}
                    else:
                        columns_metadata[col_name] = {"type": "INT"}
                else:
                    columns_metadata[col_name] = {"type": "TEXT"}
        
        return columns_metadata

    def _apply_additional_filters(self, data, condition, order_by, columns_metadata):
        """Appliquer des conditions et tri supplémentaires sur les résultats d'une vue"""
        if not data:
            return data
        
        # Filtrer par condition
        if condition:
            filtered_data = [row for row in data if evaluate_condition(row, condition, columns_metadata)]
        else:
            filtered_data = data
        
        # Trier
        if order_by:
            def sort_key(row):
                keys = []
                for order in order_by:
                    col = order["column"]
                    direction = order["direction"]
                    value = row.get(col, "null")
                    col_type = columns_metadata.get(col, {}).get("type", "TEXT")

                    # Gestion NULL
                    if value == "null":
                        sort_val = (0, None)
                    else:
                        # Conversion selon le type
                        if col_type == "INT":
                            sort_val = (1, int(value))
                        elif col_type == "FLOAT":
                            sort_val = (1, float(value))
                        elif col_type == "DATE":
                            sort_val = (1, datetime.strptime(value, "%Y-%m-%d"))
                        elif col_type in ("TEXT", "VARCHAR"):
                            sort_val = (1, value.lower())  # insensible à la casse
                        elif col_type == "BOOLEAN":
                            sort_val = (1, value.lower() == "true")
                        else:
                            sort_val = (1, str(value))

                    # Inversion pour DESC
                    if direction == "DESC":
                        if sort_val[1] is None:
                            sort_val = (2, None)  # null en dernier
                        elif isinstance(sort_val[1], (int, float)):
                            sort_val = (sort_val[0], -sort_val[1])
                        elif isinstance(sort_val[1], datetime):
                            sort_val = (sort_val[0], -sort_val[1].timestamp())
                        elif isinstance(sort_val[1], bool):
                            sort_val = (sort_val[0], not sort_val[1])
                        # TEXT et VARCHAR : on garde, on inverse après

                    keys.append(sort_val)
                return tuple(keys)

            # Tri final
            filtered_data = sorted(filtered_data, key=sort_key)

            # Pour DESC sur TEXT/VARCHAR : on inverse si nécessaire
            if any(order["direction"] == "DESC" and 
                columns_metadata.get(order["column"], {}).get("type") in ("TEXT", "VARCHAR") 
                for order in order_by):
                filtered_data = filtered_data[::-1]
        
        return filtered_data

    def show_help(self, command=None):
        """Afficher l'aide pour une commande ou générale - VERSION MISE À JOUR"""
        help_text = {
            "LOGIN JOUEUR": "LOGIN JOUEUR login MOTDEPASSE 'pass' : Connecte un joueur",
            "CRAFTER JOUEUR": "CRAFTER JOUEUR login MOTDEPASSE 'pass' PERMISSIONS read,write,delete : Crée un joueur",
            "EDIT JOUEUR": "EDIT JOUEUR login PERMISSIONS read,write,delete : Modifie les permissions",
            "LISTE JOUEURS": "LISTE JOUEURS : Liste tous les joueurs",
            "LISTE PERMISSIONS JOUEUR": "LISTE PERMISSIONS JOUEUR login : Liste les permissions d'un joueur",
            
            "CRAFTER BASE": "CRAFTER BASE nom : Crée une base de données",
            "DEPOP BASE": "DEPOP BASE nom : Supprime une base",
            "UTILISER": "UTILISER nom : Sélectionne une base",
            "QUITTER BASE": "QUITTER BASE : Désélectionne la base active",
            "LISTE BASES": "LISTE BASES : Liste toutes les bases",
            "EXPORTER BASE": "EXPORTER BASE nom : Exporte une base en ZIP",
            "IMPORTER BASE": "IMPORTER BASE nom FICHIER chemin : Importe une base depuis un ZIP",
            
            "CRAFTER TABLEAU": "CRAFTER TABLEAU nom (col1 TYPE [constraints], ...) : Crée une table",
            "DEPOP TABLEAU": "DEPOP TABLEAU nom : Supprime une table",
            "LISTE TABLEAUX": "LISTE TABLEAUX : Liste toutes les tables",
            
            "POP DANS": "POP DANS table VALEURS (val1, val2, ...) : Insère une ligne",
            "LOOT": "LOOT * DANS table [AVEC condition] [TRIER PAR col1 [ASC|DESC], ...] : Sélectionne des données",
            "EDIT": "EDIT table DEFINIR col='val' AVEC condition : Met à jour des lignes",
            "DEPOP DANS": "DEPOP DANS table AVEC condition : Supprime des lignes",
            "STATS TABLEAU": "STATS TABLEAU nom : Affiche des statistiques sur une table",
            
            "DEBUT TRANSACTION": "DEBUT TRANSACTION : Démarre une transaction",
            "VALIDER TRANSACTION": "VALIDER TRANSACTION : Valide la transaction",
            "ANNULER TRANSACTION": "ANNULER TRANSACTION : Annule la transaction",
            "STATUS TRANSACTION": "STATUS TRANSACTION : Affiche le statut des transactions",
            
            "CRAFTER VUE": "CRAFTER VUE nom COMME \"requête LOOT\" : Crée une vue",
            "DEPOP VUE": "DEPOP VUE nom : Supprime une vue",
            "LISTE VUES": "LISTE VUES : Liste toutes les vues",
            
            "SNAPSHOT TABLEAU": "SNAPSHOT TABLEAU nom VERSION 'description' : Crée un snapshot",
            "VOIR SNAPSHOT": "VOIR SNAPSHOT nom VERSION id : Visualise un snapshot",
            "VOYAGE TABLEAU": "VOYAGE TABLEAU nom VERSION id : Restaure un snapshot",
            "LISTE SNAPSHOTS": "LISTE SNAPSHOTS TABLEAU nom : Liste les snapshots d'une table",
            "DEPOP SNAPSHOT": "DEPOP SNAPSHOT TABLEAU nom VERSION id : Supprime un snapshot",
            
            "CRAFTER QUETE": "CRAFTER QUETE nom \"requête LOOT\" CHAQUE intervalle : Crée une quête",
            "EXECUTER QUETE": "EXECUTER QUETE nom : Exécute une quête manuellement",
            "LISTE QUETES": "LISTE QUETES : Liste toutes les quêtes",
            "DEPOP QUETE": "DEPOP QUETE nom : Supprime une quête",
            "DEMARRER QUETES": "DEMARRER QUETES : Démarre le scheduler des quêtes",
            
            "AIDE": "AIDE [commande] : Affiche l'aide générale ou d'une commande",
            "QUITTER": "QUITTER : Quitte l'application"
        }
        
        if command:
            command = command.upper()
            if command in help_text:
                print(f"╔════════════════════════════════════")
                print(f"║ Aide pour {command} :")
                print(f"║ {help_text[command]}")
                print(f"╚════════════════════════════════════")
            else:
                raise ValueError(f"Commande {command} inconnue. Tape AIDE pour voir toutes les commandes.")
        else:
            print(f"╔════════════════════════════════════")
            print(f"║ Commandes disponibles ({len(help_text)} commandes) :")
            print(f"╠════════════════════════════════════")
            # Grouper par catégorie pour une meilleure lisibilité
            categories = {
                "Joueurs": ["LOGIN JOUEUR", "CRAFTER JOUEUR", "EDIT JOUEUR", "LISTE JOUEURS", "LISTE PERMISSIONS JOUEUR"],
                "Bases": ["CRAFTER BASE", "DEPOP BASE", "UTILISER", "QUITTER BASE", "LISTE BASES", "EXPORTER BASE", "IMPORTER BASE"],
                "Tables": ["CRAFTER TABLEAU", "DEPOP TABLEAU", "LISTE TABLEAUX"],
                "Données": ["POP DANS", "LOOT", "EDIT", "DEPOP DANS", "STATS TABLEAU"],
                "Transactions": ["DEBUT TRANSACTION", "VALIDER TRANSACTION", "ANNULER TRANSACTION", "STATUS TRANSACTION"],
                "Vues": ["CRAFTER VUE", "DEPOP VUE", "LISTE VUES"],
                "Snapshots": ["SNAPSHOT TABLEAU", "VOIR SNAPSHOT", "VOYAGE TABLEAU", "LISTE SNAPSHOTS", "DEPOP SNAPSHOT"],
                "Quêtes": ["CRAFTER QUETE", "EXECUTER QUETE", "LISTE QUETES", "DEPOP QUETE", "DEMARRER QUETES"],
                "Général": ["AIDE", "QUITTER"]
            }
            
            for category, commands in categories.items():
                print(f"║ --- {category} ---")
                for cmd in commands:
                    if cmd in help_text:
                        print(f"║ {help_text[cmd]}")
                print(f"║")
            
            print(f"╚════════════════════════════════════")