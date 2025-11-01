import json
import shutil
import zipfile
from pathlib import Path

class DatabaseManager:
    def __init__(self, db_path, sgbdr):
        self.db_path = db_path
        self.sgbdr = sgbdr

    def set_sgbdr(self, sgbdr):
        """Définir la référence à l’instance SGBDR"""
        self.sgbdr = sgbdr

    def create_database(self, db_name):
        """Créer une base de données"""
        self.sgbdr.user_manager.check_permission("write")
        db_dir = self.db_path / db_name
        if db_dir.exists():
            print(f"╔════════════════════════════════════")
            print(f"║ Tu as déjà crafté la base {db_name} ! Choisis un autre nom.")
            print(f"╚════════════════════════════════════")
            return
        db_dir.mkdir(exist_ok=True)
        with open(db_dir / "metadata.json", "w") as f:
            json.dump({"tables": {}}, f, indent=2)
        print(f"╔════════════════════════════════════")
        print(f"║ Base {db_name} craftée avec succès ! GG")
        print(f"╚════════════════════════════════════")

    def delete_database(self, db_name):
        """Supprimer une base de données"""
        self.sgbdr.user_manager.check_permission("delete")
        if self.sgbdr.current_db:
            raise ValueError("Une base est active, faut d’abord quitter la map !")
        db_dir = self.db_path / db_name
        if not db_dir.exists():
            raise ValueError(f"Base {db_name} introuvable. T’as raté la map ?")
        shutil.rmtree(db_dir)
        print(f"╔════════════════════════════════════")
        print(f"║ Base {db_name} pulvérisée !")
        print(f"╚════════════════════════════════════")

    def use_database(self, db_name):
        """Sélectionner une base de données"""
        self.sgbdr.user_manager.check_permission("read")
        if (self.db_path / db_name).exists():
            self.sgbdr.current_db = db_name
            print(f"╔════════════════════════════════════")
            print(f"║ Switch vers la base {db_name}.")
            print(f"╚════════════════════════════════════")
        else:
            raise ValueError(f"Base de données {db_name} introuvable. T’as oublié de la faire spawn ?")

    def deselect_database(self):
        """Quitter la base de données actuelle"""
        if not self.sgbdr.current_db:
            print(f"╔════════════════════════════════════")
            print(f"║ Aucune base active, t’es déjà hors de la map !")
            print(f"╚════════════════════════════════════")
            return
        db_name = self.sgbdr.current_db
        self.sgbdr.current_db = None
        print(f"╔════════════════════════════════════")
        print(f"║ Tu as quitté la map {db_name} !")
        print(f"╚════════════════════════════════════")

    def list_databases(self):
        """Lister toutes les bases de données"""
        self.sgbdr.user_manager.check_permission("read")
        databases = [d.name for d in self.db_path.iterdir() if d.is_dir() and (d / "metadata.json").exists()]
        print(f"╔════════════════════════════════════")
        print(f"║ Voici les maps disponibles : {len(databases)} trouvées !")
        print(f"╚════════════════════════════════════")
        return databases

    def export_database(self, db_name):
        """Exporter une base de données dans un fichier ZIP"""
        self.sgbdr.user_manager.check_permission("write")
        db_dir = self.db_path / db_name
        if not db_dir.exists():
            raise ValueError(f"Base {db_name} introuvable. T’as raté la map ?")
        zip_path = self.db_path / f"{db_name}.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for file_path in db_dir.glob("*.json"):
                zipf.write(file_path, f"{db_name}/{file_path.name}")
        print(f"╔════════════════════════════════════")
        print(f"║ Base {db_name} exportée dans {zip_path} !")
        print(f"╚════════════════════════════════════")

    def import_database(self, db_name, zip_path):
            """Importer une base de données depuis un fichier ZIP"""
            self.sgbdr.user_manager.check_permission("write")
            zip_path = Path(zip_path)
            if not zip_path.exists() or not zip_path.suffix == ".zip":
                raise ValueError("Fichier ZIP introuvable ou invalide")
            db_dir = self.db_path / db_name
            if db_dir.exists():
                raise ValueError(f"Base {db_name} existe déjà. Supprime-la d’abord !")
            
            # Extraire dans un répertoire temporaire
            temp_dir = self.db_path / f"temp_import_{db_name}"
            try:
                with zipfile.ZipFile(zip_path, "r") as zipf:
                    zipf.extractall(temp_dir)
                
                # Trouver le dossier racine dans le ZIP (ex. : testdb/)
                source_db_dir = None
                for item in temp_dir.iterdir():
                    if item.is_dir() and (item / "metadata.json").exists():
                        source_db_dir = item
                        break
                
                if not source_db_dir:
                    raise ValueError("Structure du ZIP invalide : aucun dossier avec metadata.json trouvé")
                
                # Déplacer le dossier vers db_name (ex. : testdb -> testdb_copy)
                shutil.move(source_db_dir, db_dir)
                
                # Vérifier que metadata.json existe dans le répertoire final
                if not (db_dir / "metadata.json").exists():
                    shutil.rmtree(db_dir, ignore_errors=True)
                    raise ValueError("Fichier metadata.json manquant dans le dossier importé")
            
            finally:
                # Nettoyer le répertoire temporaire
                shutil.rmtree(temp_dir, ignore_errors=True)
            
            print(f"╔════════════════════════════════════")
            print(f"║ Base {db_name} importée depuis {zip_path} !")
            print(f"╚════════════════════════════════════")