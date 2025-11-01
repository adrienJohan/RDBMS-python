
from .user_manager import UserManager
from .database_manager import DatabaseManager
from .table_manager import TableManager
from .data_manager import DataManager
from .query_parser import QueryParser
from .transaction_manager import TransactionManager
from .snapshot_manager import SnapshotManager
from .quest_manager import QuestManager

from pathlib import Path
import re
import json

class SGBDR:
    def __init__(self, db_path="bases_de_donnees"):
        self.db_path = Path(db_path)
        self.db_path.mkdir(exist_ok=True)
        self.current_db = None
        self.current_user = None
        self.user_manager = UserManager(self.db_path)
        self.database_manager = DatabaseManager(self.db_path, self)
        self.table_manager = TableManager(self.db_path, self)
        self.data_manager = DataManager(self.db_path, self)
        self.query_parser = QueryParser(self)
        self.transaction_manager = TransactionManager(self.db_path, self)
        self.snapshot_manager = SnapshotManager(self.db_path, self)
        self.quest_manager = QuestManager(self.db_path, self)

        # Initialiser les références à l'instance SGBDR
        self.user_manager.set_sgbdr(self)
        self.database_manager.set_sgbdr(self)
        self.table_manager.set_sgbdr(self)
        self.data_manager.set_sgbdr(self)
        self.query_parser.set_sgbdr(self)
        self.transaction_manager.set_sgbdr(self)
        self.snapshot_manager.set_sgbdr(self)
        self.quest_manager.set_sgbdr(self)

    def _is_view(self, name):
        """Vérifier si un nom correspond à une vue"""
        if not self.current_db:
            return False
        
        db_dir = self.db_path / self.current_db
        if not db_dir.exists():
            return False
        
        try:
            with open(db_dir / "metadata.json", "r") as f:
                metadata = json.load(f)
                return "views" in metadata and name in metadata["views"]
        except:
            return False

    def execute_query(self, query):
        """Exécuter une requête SQL-like"""
        query = query.strip()
        parsed = self.query_parser.parse_query(query)
        
        if parsed["type"] == "login_user":
            self.user_manager.login_user(parsed["login"], parsed["password"])
        
        elif parsed["type"] == "create_user":
            self.user_manager.create_user(parsed["login"], parsed["password"], parsed["permissions"])
        
        elif parsed["type"] == "edit_user_permissions":
            self.user_manager.edit_user_permissions(parsed["login"], parsed["permissions"])
        
        elif parsed["type"] == "list_users":
            return self.user_manager.list_users()
        
        elif parsed["type"] == "list_user_permissions":
            return self.user_manager.list_user_permissions(parsed["login"])
        
        elif parsed["type"] == "create_database":
            self.database_manager.create_database(parsed["db_name"])
        
        elif parsed["type"] == "delete_database":
            self.database_manager.delete_database(parsed["db_name"])
        
        elif parsed["type"] == "use_database":
            self.database_manager.use_database(parsed["db_name"])
        
        elif parsed["type"] == "deselect_database":
            self.database_manager.deselect_database()
        
        elif parsed["type"] == "list_databases":
            return self.database_manager.list_databases()
        
        elif parsed["type"] == "export_database":
            self.database_manager.export_database(parsed["db_name"])
        
        elif parsed["type"] == "import_database":
            self.database_manager.import_database(parsed["db_name"], parsed["file_path"])
        
        elif parsed["type"] == "create_table":
            self.table_manager.create_table(parsed["table_name"], parsed["columns"])
        
        elif parsed["type"] == "delete_table":
            self.table_manager.delete_table(parsed["table_name"])
            
        
        elif parsed["type"] == "list_tables":
            return self.table_manager.list_tables()
        
        elif parsed["type"] == "insert":
            self.data_manager.insert(parsed["table_name"], parsed["values"])
        
        elif parsed["type"] == "select":
            if self._is_view(parsed["table_name"]):
                return self.data_manager.execute_view(parsed["table_name"], parsed.get("condition"), parsed.get("order_by"))
            else:
                return self.data_manager.select(parsed["table_name"], parsed.get("columns", "*") ,  parsed.get("condition"), parsed.get("order_by"))    
        
        elif parsed["type"] == "join_tables":
            return self.data_manager.join_tables(parsed["table1"], parsed["table2"], parsed.get("columns", "*") ,  parsed["join_condition"], parsed.get("order_by"))
        
        elif parsed["type"] == "update":
            self.data_manager.update(parsed["table_name"], parsed["set_clause"], parsed["condition"])
        
        elif parsed["type"] == "delete":
            self.data_manager.delete(parsed["table_name"], parsed["condition"])
        
        elif parsed["type"] == "table_stats":
            return self.data_manager.table_stats(parsed["table_name"])
        
        elif parsed["type"] == "show_help":
            self.data_manager.show_help(parsed.get("command"))
        
        elif parsed["type"] == "quit":
            print("À plus, aventurier ! La quête s’arrête ici !")        
        
        elif parsed["type"] == "begin_transaction":
            self.transaction_manager.begin_transaction()
        
        elif parsed["type"] == "commit_transaction":
            self.transaction_manager.commit()
        
        elif parsed["type"] == "rollback_transaction":
            self.transaction_manager.rollback()

        elif parsed["type"] == "transaction_status":
            status = self.transaction_manager.get_transaction_status()
            return status
        
        elif parsed["type"] == "create_view":
            self.table_manager.create_view(parsed["view_name"], parsed["query"])
        
        elif parsed["type"] == "delete_view":
            self.table_manager.delete_view(parsed["view_name"])
        
        elif parsed["type"] == "list_views":
            return self.table_manager.list_views()
        
        elif parsed["type"] == "create_snapshot":
            return self.snapshot_manager.create_snapshot(parsed["table_name"], parsed["description"])
        
        elif parsed["type"] == "restore_snapshot":
            self.snapshot_manager.restore_snapshot(parsed["table_name"], parsed["snapshot_id"])
        
        elif parsed["type"] == "view_snapshot":
            return self.snapshot_manager.view_snapshot(parsed["table_name"], parsed["snapshot_id"])
        
        elif parsed["type"] == "list_snapshots":
            return self.snapshot_manager.list_snapshots(parsed["table_name"])
        
        elif parsed["type"] == "delete_snapshot":
            self.snapshot_manager.delete_snapshot(parsed["table_name"], parsed["snapshot_id"])
        
        elif parsed["type"] == "quest_history":
            return self.quest_manager.view_quest_history(parsed["quest_name"])
        
        elif parsed["type"] == "quest_results":
            return self.quest_manager.view_quest_results(parsed["quest_name"], parsed["execution_id"])

        elif parsed["type"] == "create_quest":
            self.quest_manager.create_quest(parsed["quest_name"], parsed["query"], parsed["interval"])
        
        elif parsed["type"] == "execute_quest":
            return self.quest_manager.execute_quest(parsed["quest_name"])
        
        elif parsed["type"] == "list_quests":
            return self.quest_manager.list_quests()
        
        elif parsed["type"] == "delete_quest":
            self.quest_manager.delete_quest(parsed["quest_name"])
        
        elif parsed["type"] == "start_quests":
            self.quest_manager.start_scheduler()

        else:
            raise ValueError("Sort inconnu ! Check ton grimoire SQL")