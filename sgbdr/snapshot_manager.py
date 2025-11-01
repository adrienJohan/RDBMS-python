import json
from pathlib import Path
from datetime import datetime

class SnapshotManager:
    def __init__(self, db_path, sgbdr):
        self.db_path = db_path
        self.sgbdr = sgbdr

    def set_sgbdr(self, sgbdr):
        self.sgbdr = sgbdr

    def create_snapshot(self, table_name, description):
        """Créer un snapshot d'une table"""
        self.sgbdr.user_manager.check_permission("write")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        table_path = db_dir / f"{table_name}.json"
        
        if not table_path.exists():
            raise ValueError(f"Table {table_name} introuvable.")
        
        # Charger les données actuelles
        with open(table_path, "r") as f:
            current_data = json.load(f)
        
        # Créer le répertoire des snapshots
        snapshots_dir = db_dir / "_snapshots" / table_name
        snapshots_dir.mkdir(parents=True, exist_ok=True)
        
        # Générer un ID unique
        snapshot_id = f"snap_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Sauvegarder le snapshot
        snapshot_data = {
            "id": snapshot_id,
            "description": description,
            "created_at": datetime.now().isoformat(),
            "created_by": self.sgbdr.current_user,
            "data": current_data,
            "row_count": len(current_data)
        }
        
        snapshot_file = snapshots_dir / f"{snapshot_id}.json"
        with open(snapshot_file, "w") as f:
            json.dump(snapshot_data, f, indent=2)
        
        print(f"╔════════════════════════════════════")
        print(f"║ Snapshot {snapshot_id} créé !")
        print(f"║ Table: {table_name}")
        print(f"║ Description: {description}")
        print(f"║ Lignes: {len(current_data)}")
        print(f"╚════════════════════════════════════")
        
        # Retourner un objet avec des clés pour l'affichage tabulaire
        return [{
            "snapshot_id": snapshot_id,
            "table": table_name,
            "description": description,
            "rows": len(current_data),
            "status": "CRÉÉ"
        }]

    def list_snapshots(self, table_name):
        """Lister tous les snapshots d'une table"""
        self.sgbdr.user_manager.check_permission("read")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        snapshots_dir = db_dir / "_snapshots" / table_name
        
        if not snapshots_dir.exists():
            return []
        
        snapshots = []
        for snapshot_file in snapshots_dir.glob("*.json"):
            with open(snapshot_file, "r") as f:
                data = json.load(f)
                snapshots.append({
                    "id": data["id"],
                    "description": data["description"],
                    "created_at": data["created_at"],
                    "created_by": data["created_by"],
                    "row_count": data["row_count"]
                })
        
        # Trier par date
        snapshots.sort(key=lambda x: x["created_at"], reverse=True)
        
        print(f"╔════════════════════════════════════")
        print(f"║ Snapshots de {table_name}: {len(snapshots)} trouvés")
        print(f"╚════════════════════════════════════")
        
        return snapshots

    def view_snapshot(self, table_name, snapshot_id):
        """Visualiser un snapshot"""
        self.sgbdr.user_manager.check_permission("read")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        snapshot_file = db_dir / "_snapshots" / table_name / f"{snapshot_id}.json"
        
        if not snapshot_file.exists():
            raise ValueError(f"Snapshot {snapshot_id} introuvable.")
        
        with open(snapshot_file, "r") as f:
            snapshot_data = json.load(f)
        
        print(f"╔════════════════════════════════════")
        print(f"║ Snapshot {snapshot_id}")
        print(f"║ Table: {table_name}")
        print(f"║ Description: {snapshot_data['description']}")
        print(f"║ Date: {snapshot_data['created_at']}")
        print(f"║ Par: {snapshot_data['created_by']}")
        print(f"╚════════════════════════════════════")
        
        # Retourner directement les données pour l'affichage tabulaire
        return snapshot_data["data"]

    def restore_snapshot(self, table_name, snapshot_id):
        """Restaurer un snapshot"""
        self.sgbdr.user_manager.check_permission("write")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        snapshot_file = db_dir / "_snapshots" / table_name / f"{snapshot_id}.json"
        table_path = db_dir / f"{table_name}.json"
        
        if not snapshot_file.exists():
            raise ValueError(f"Snapshot {snapshot_id} introuvable.")
        if not table_path.exists():
            raise ValueError(f"Table {table_name} introuvable.")
        
        # Charger le snapshot
        with open(snapshot_file, "r") as f:
            snapshot_data = json.load(f)
        
        # Restaurer les données
        with open(table_path, "w") as f:
            json.dump(snapshot_data["data"], f,indent=2)
        
        print(f"╔════════════════════════════════════")
        print(f"║ Table {table_name} restaurée !")
        print(f"║ Snapshot: {snapshot_id}")
        print(f"║ Date originale: {snapshot_data['created_at']}")
        print(f"║ Lignes restaurées: {snapshot_data['row_count']}")
        print(f"╚════════════════════════════════════")

    def delete_snapshot(self, table_name, snapshot_id):
        """Supprimer un snapshot"""
        self.sgbdr.user_manager.check_permission("delete")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        snapshot_file = db_dir / "_snapshots" / table_name / f"{snapshot_id}.json"
        
        if not snapshot_file.exists():
            raise ValueError(f"Snapshot {snapshot_id} introuvable.")
        
        # Charger les infos avant suppression
        with open(snapshot_file, "r") as f:
            snapshot_data = json.load(f)
        
        snapshot_file.unlink()
        
        print(f"╔════════════════════════════════════")
        print(f"║ Snapshot {snapshot_id} supprimé !")
        print(f"║ Table: {table_name}")
        print(f"║ Description: {snapshot_data['description']}")
        print(f"╚════════════════════════════════════")