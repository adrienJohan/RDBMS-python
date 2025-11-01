import json
from pathlib import Path
import shutil
import tempfile

class TransactionManager:
    def __init__(self, db_path, sgbdr):
        self.db_path = db_path
        self.sgbdr = sgbdr
        self.transaction_stack = []
        self.in_transaction = False
        self.backup_base = Path(tempfile.gettempdir()) / "sgbdr_transactions"
        self.backup_base.mkdir(exist_ok=True)
        
    def set_sgbdr(self, sgbdr):
        """Définir la référence à l'instance SGBDR"""
        self.sgbdr = sgbdr

    def begin_transaction(self):
        """Démarrer une transaction"""
        self.sgbdr.user_manager.check_permission("write")
        
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée pour la transaction !")
        
        transaction_id = f"tx_{len(self.transaction_stack)}_{id(self)}"
        backup_dir = self.backup_base / transaction_id
        
        # Sauvegarder l'état actuel de la base
        self._backup_current_state(backup_dir)
        
        self.transaction_stack.append({
            "id": transaction_id,
            "backup_dir": backup_dir,
            "database": self.sgbdr.current_db
        })
        
        self.in_transaction = True
        print(f"╔════════════════════════════════════")
        print(f"║ Transaction {transaction_id} commencée !")
        print(f"╚════════════════════════════════════")

    def commit(self):
        """Valider la transaction"""
        if not self.in_transaction:
            raise ValueError("Aucune transaction en cours !")
        
        transaction = self.transaction_stack.pop()
        # Nettoyer la sauvegarde
        shutil.rmtree(transaction["backup_dir"], ignore_errors=True)
        
        self.in_transaction = len(self.transaction_stack) > 0
        print(f"╔════════════════════════════════════")
        print(f"║ Transaction {transaction['id']} validée !")
        print(f"╚════════════════════════════════════")

    def rollback(self):
        """Annuler la transaction"""
        if not self.in_transaction:
            raise ValueError("Aucune transaction en cours !")
        
        transaction = self.transaction_stack.pop()
        
        # Restaurer l'état précédent seulement si c'est la bonne base
        if self.sgbdr.current_db == transaction["database"]:
            self._restore_backup_state(transaction["backup_dir"])
            print(f"╔════════════════════════════════════")
            print(f"║ Transaction {transaction['id']} annulée !")
            print(f"║ Base {self.sgbdr.current_db} restaurée.")
            print(f"╚════════════════════════════════════")
        else:
            # Nettoyer seulement la sauvegarde
            shutil.rmtree(transaction["backup_dir"], ignore_errors=True)
            print(f"╔════════════════════════════════════")
            print(f"║ Transaction {transaction['id']} annulée !")
            print(f"║ Note : Base différente, restauration ignorée.")
            print(f"╚════════════════════════════════════")
        
        self.in_transaction = len(self.transaction_stack) > 0

    def _backup_current_state(self, backup_dir):
        """Sauvegarder l'état actuel de la base"""
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
        backup_dir.mkdir(parents=True)
        
        current_db_dir = self.db_path / self.sgbdr.current_db
        if not current_db_dir.exists():
            return
            
        # Copier tous les fichiers JSON de la base
        for json_file in current_db_dir.glob("*.json"):
            shutil.copy2(json_file, backup_dir / json_file.name)

    def _restore_backup_state(self, backup_dir):
        """Restaurer l'état depuis la sauvegarde"""
        if not backup_dir.exists():
            raise ValueError("Sauvegarde de transaction introuvable !")
        
        current_db_dir = self.db_path / self.sgbdr.current_db
        if not current_db_dir.exists():
            current_db_dir.mkdir(parents=True)
        
        # Restaurer tous les fichiers de sauvegarde
        for backup_file in backup_dir.glob("*.json"):
            shutil.copy2(backup_file, current_db_dir / backup_file.name)

    def get_transaction_status(self):
        """Obtenir le statut des transactions"""
        if not self.in_transaction:
            return {"active": False, "count": 0}
        return {
            "active": True, 
            "count": len(self.transaction_stack),
            "current": self.transaction_stack[-1]["id"] if self.transaction_stack else None
        }