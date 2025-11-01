import re

def evaluate_condition(row, condition, columns):
    """Évaluer une condition WHERE avec priorité AND > OR - Version corrigée"""
    
    def evaluate_single_condition(cond, row, columns):
        """Évaluer une condition simple : colonne opérateur valeur"""
        cond = cond.strip()
        
        # Pattern pour les conditions - maintenant avec support table.colonne
        match = re.match(r"((?:\w+\.)?\w+)\s*(=|!=|>|<)\s*'([^']*)'", cond)
        if not match:
            raise ValueError(f"Condition mal formée : {cond}")
        
        full_col, op, value = match.groups()
        
        # Vérifier si la colonne existe dans les données
        if full_col not in row:
            # Essayer de trouver la colonne sans préfixe
            col_name = full_col.split('.')[-1] if '.' in full_col else full_col
            found = False
            for key in row.keys():
                if key.endswith('.' + col_name) or key == col_name:
                    full_col = key
                    found = True
                    break
            if not found:
                raise ValueError(f"Colonne {full_col} introuvable")
        
        row_value = row.get(full_col)
        
        # Gestion des valeurs NULL
        if row_value == "null" or value == "null":
            return op == "=" and row_value == value or op == "!=" and row_value != value
        
        # Déterminer le type de la colonne
        col_type = "TEXT"  # Par défaut
        if full_col in columns:
            col_type = columns[full_col]["type"]
        else:
            # Deviner le type basé sur le nom de la colonne
            col_name = full_col.split('.')[-1] if '.' in full_col else full_col
            for col_key, col_info in columns.items():
                if col_key.endswith('.' + col_name) or col_key == col_name:
                    col_type = col_info["type"]
                    break
        
        # Conversion des types
        if col_type in ("INT", "FLOAT"):
            try:
                row_value = float(row_value)
                value = float(value)
            except (ValueError, TypeError):
                return False
        elif col_type == "DATE":
            try:
                from datetime import datetime
                row_value = datetime.strptime(row_value, "%Y-%m-%d")
                value = datetime.strptime(value, "%Y-%m-%d")
            except (ValueError, TypeError):
                return False
        elif col_type == "BOOLEAN":
            row_value = row_value.lower() == "true"
            value = value.lower() == "true"
        
        # Application de l'opérateur
        if op == "=":
            return row_value == value
        elif op == "!=":
            return row_value != value
        elif op == ">":
            return row_value > value
        elif op == "<":
            return row_value < value

    def parse_condition(condition_str):
        """Parser les conditions avec priorité ET > OR"""
        condition_str = condition_str.strip()
        
        # D'abord, séparer par OU (le moins prioritaire)
        or_parts = re.split(r"\s+OU\s+", condition_str, flags=re.IGNORECASE)
        
        if len(or_parts) > 1:
            # Au moins un des OU doit être vrai
            for or_part in or_parts:
                if parse_and_conditions(or_part.strip()):
                    return True
            return False
        else:
            # Pas de OU, traiter les ET
            return parse_and_conditions(condition_str)
    
    def parse_and_conditions(condition_str):
        """Parser les conditions ET"""
        and_parts = re.split(r"\s+ET\s+", condition_str, flags=re.IGNORECASE)
        
        if len(and_parts) > 1:
            # Tous les ET doivent être vrais
            for and_part in and_parts:
                if not evaluate_single_condition(and_part.strip(), row, columns):
                    return False
            return True
        else:
            # Condition simple
            return evaluate_single_condition(condition_str.strip(), row, columns)
    
    try:
        return parse_condition(condition)
    except Exception as e:
        raise ValueError(f"Erreur dans la condition '{condition}': {e}")