# sgbdr/quest_manager.py
import json
import schedule
import time
import threading
from pathlib import Path
from datetime import datetime

class QuestManager:
    def __init__(self, db_path, sgbdr):
        self.db_path = db_path
        self.sgbdr = sgbdr
        self.scheduler_running = False
        self.scheduler_thread = None

    def set_sgbdr(self, sgbdr):
        """Définir la référence à l'instance SGBDR"""
        self.sgbdr = sgbdr
        self._load_quests()

    def create_quest(self, quest_name, query, interval):
        """Créer une quête automatisée"""
        self.sgbdr.user_manager.check_permission("write")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        # Valider l'intervalle
        valid_intervals = ["1 JOURS", "1 HEURES", "30 MINUTES", "1 SEMAINE"]
        if interval not in valid_intervals:
            raise ValueError(f"Intervalle invalide. Options: {', '.join(valid_intervals)}")
        
        # Valider que la requête est un LOOT
        if not query.upper().startswith("LOOT"):
            raise ValueError("Une quête doit être basée sur une requête LOOT.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        quests_file = db_dir / "quests.json"
        quests_logs_dir = db_dir / "_quests_logs"
        quests_logs_dir.mkdir(exist_ok=True)
        
        # Charger les quêtes existantes
        if quests_file.exists():
            with open(quests_file, "r") as f:
                quests_data = json.load(f)
        else:
            quests_data = {"quests": {}}
        
        if quest_name in quests_data["quests"]:
            raise ValueError(f"Quête {quest_name} existe déjà.")
        
        # Sauvegarder la quête
        quest_data = {
            "name": quest_name,
            "query": query,
            "interval": interval,
            "created_by": self.sgbdr.current_user,
            "created_at": datetime.now().isoformat(),
            "last_run": None,
            "last_results_count": 0,
            "is_active": True,
            "total_executions": 0
        }
        
        quests_data["quests"][quest_name] = quest_data
        
        with open(quests_file, "w") as f:
            json.dump(quests_data, f, indent=2)
        
        # Créer le fichier de log pour cette quête
        log_file = quests_logs_dir / f"{quest_name}_logs.json"
        with open(log_file, "w") as f:
            json.dump({"executions": []}, f, indent=2)
        
        # Ajouter au scheduler
        self._add_quest_to_scheduler(quest_name, quest_data)
        
        print(f"╔════════════════════════════════════")
        print(f"║ Quête '{quest_name}' craftée !")
        print(f"║ Requête: {query}")
        print(f"║ Intervalle: {interval}")
        print(f"║ Fichier de log: _quests_logs/{quest_name}_logs.json")
        print(f"╚════════════════════════════════════")

    def execute_quest(self, quest_name):
        """Exécuter une quête et stocker les résultats"""
        self.sgbdr.user_manager.check_permission("read")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        quests_file = db_dir / "quests.json"
        quests_logs_dir = db_dir / "_quests_logs"
        
        if not quests_file.exists():
            raise ValueError("Aucune quête n'a été craftée.")
        
        with open(quests_file, "r") as f:
            quests_data = json.load(f)
        
        if quest_name not in quests_data["quests"]:
            raise ValueError(f"Quête {quest_name} introuvable.")
        
        quest_data = quests_data["quests"][quest_name]
        
        print(f"╔════════════════════════════════════")
        print(f"║ Exécution de la quête '{quest_name}'...")
        print(f"╚════════════════════════════════════")
        
        # Exécuter la requête
        try:
            results = self.sgbdr.execute_query(quest_data["query"])
            results_count = len(results) if isinstance(results, list) else 0
            
            # Stocker les résultats dans le log
            execution_id = f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            execution_data = {
                "id": execution_id,
                "timestamp": datetime.now().isoformat(),
                "results_count": results_count,
                "results": results,  # Stocker les VRAIS résultats
                "trigger": "MANUEL"
            }
            
            # Sauvegarder dans le log de la quête
            log_file = quests_logs_dir / f"{quest_name}_logs.json"
            if log_file.exists():
                with open(log_file, "r") as f:
                    log_data = json.load(f)
            else:
                log_data = {"executions": []}
            
            log_data["executions"].append(execution_data)
            
            # Garder seulement les 50 dernières exécutions
            if len(log_data["executions"]) > 50:
                log_data["executions"] = log_data["executions"][-50:]
            
            with open(log_file, "w") as f:
                json.dump(log_data, f, indent=2)
            
            # Mettre à jour les métadonnées de la quête
            quest_data["last_run"] = datetime.now().isoformat()
            quest_data["last_results_count"] = results_count
            quest_data["total_executions"] = quest_data.get("total_executions", 0) + 1
            
            with open(quests_file, "w") as f:
                json.dump(quests_data, f, indent=2)
            
            print(f"╔════════════════════════════════════")
            print(f"║ Quête '{quest_name}' accomplie !")
            print(f"║ Résultats trouvés: {results_count}")
            print(f"║ Résultats stockés dans: _quests_logs/{quest_name}_logs.json")
            print(f"╚════════════════════════════════════")
            
            return results
            
        except Exception as e:
            print(f"╔════════════════════════════════════")
            print(f"║ Erreur dans la quête '{quest_name}': {e}")
            print(f"╚════════════════════════════════════")
            return []

    def list_quests(self):
        """Lister toutes les quêtes"""
        self.sgbdr.user_manager.check_permission("read")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        quests_file = db_dir / "quests.json"
        
        if not quests_file.exists():
            return []
        
        with open(quests_file, "r") as f:
            quests_data = json.load(f)
        
        quests = []
        for name, data in quests_data["quests"].items():
            quests.append({
                "name": name,
                "query": data["query"],
                "interval": data["interval"],
                "created_by": data["created_by"],
                "last_run": data.get("last_run", "Jamais"),
                "last_results": data.get("last_results_count", 0),
                "total_executions": data.get("total_executions", 0),
                "status": "ACTIVE" if data.get("is_active", True) else "INACTIVE"
            })
        
        print(f"╔════════════════════════════════════")
        print(f"║ Quêtes craftées: {len(quests)} trouvées")
        print(f"╚════════════════════════════════════")
        
        return quests

    def delete_quest(self, quest_name):
        """Supprimer une quête"""
        self.sgbdr.user_manager.check_permission("delete")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        quests_file = db_dir / "quests.json"
        
        if not quests_file.exists():
            raise ValueError("Aucune quête n'a été craftée.")
        
        with open(quests_file, "r") as f:
            quests_data = json.load(f)
        
        if quest_name not in quests_data["quests"]:
            raise ValueError(f"Quête {quest_name} introuvable.")
        
        # Retirer du scheduler
        self._remove_quest_from_scheduler(quest_name)
        
        # Supprimer la quête
        del quests_data["quests"][quest_name]
        
        with open(quests_file, "w") as f:
            json.dump(quests_data, f, indent=2)
        
        # Supprimer les logs (optionnel)
        log_file = db_dir / "_quests_logs" / f"{quest_name}_logs.json"
        if log_file.exists():
            log_file.unlink()
        
        print(f"╔════════════════════════════════════")
        print(f"║ Quête '{quest_name}' supprimée !")
        print(f"╚════════════════════════════════════")

    def view_quest_history(self, quest_name, limit=10):
        """Voir l'historique d'exécution d'une quête"""
        self.sgbdr.user_manager.check_permission("read")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        log_file = db_dir / "_quests_logs" / f"{quest_name}_logs.json"
        
        if not log_file.exists():
            raise ValueError(f"Aucun historique pour la quête {quest_name}.")
        
        with open(log_file, "r") as f:
            log_data = json.load(f)
        
        executions = log_data.get("executions", [])
        executions.reverse()  # Du plus récent au plus ancien
        
        print(f"╔════════════════════════════════════")
        print(f"║ Historique de la quête '{quest_name}'")
        print(f"║ {len(executions)} exécutions enregistrées")
        print(f"╚════════════════════════════════════")
        
        return executions[:limit]

    def view_quest_results(self, quest_name, execution_id):
        """Voir les résultats d'une exécution spécifique"""
        self.sgbdr.user_manager.check_permission("read")
        if not self.sgbdr.current_db:
            raise ValueError("Aucune base sélectionnée.")
        
        db_dir = self.db_path / self.sgbdr.current_db
        log_file = db_dir / "_quests_logs" / f"{quest_name}_logs.json"
        
        if not log_file.exists():
            raise ValueError(f"Aucun historique pour la quête {quest_name}.")
        
        with open(log_file, "r") as f:
            log_data = json.load(f)
        
        for execution in log_data.get("executions", []):
            if execution["id"] == execution_id:
                print(f"╔════════════════════════════════════")
                print(f"║ Résultats de {execution_id}")
                print(f"║ Quête: {quest_name}")
                print(f"║ Date: {execution['timestamp']}")
                print(f"║ Résultats: {execution['results_count']} lignes")
                print(f"╚════════════════════════════════════")
                return execution["results"]
        
        raise ValueError(f"Exécution {execution_id} introuvable.")

    def start_scheduler(self):
        """Démarrer le scheduler des quêtes"""
        if self.scheduler_running:
            return
        
        self.scheduler_running = True
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        print(f"╔════════════════════════════════════")
        print(f"║ Scheduler des quêtes démarré !")
        print(f"║ Les quêtes s'exécuteront automatiquement")
        print(f"╚════════════════════════════════════")

    def _run_scheduler(self):
        """Boucle principale du scheduler"""
        while self.scheduler_running:
            schedule.run_pending()
            time.sleep(60)  # Vérifier toutes les minutes

    def _load_quests(self):
        """Charger les quêtes au démarrage"""
        if not self.sgbdr.current_db:
            return
        
        db_dir = self.db_path / self.sgbdr.current_db
        quests_file = db_dir / "quests.json"
        
        if quests_file.exists():
            with open(quests_file, "r") as f:
                quests_data = json.load(f)
            
            for quest_name, quest_data in quests_data["quests"].items():
                if quest_data.get("is_active", True):
                    self._add_quest_to_scheduler(quest_name, quest_data)

    def _add_quest_to_scheduler(self, quest_name, quest_data):
        """Ajouter une quête au scheduler"""
        interval = quest_data["interval"]
        
        if interval == "1 JOURS":
            schedule.every().day.at("09:00").do(self._execute_scheduled_quest, quest_name).tag(quest_name)
        elif interval == "1 HEURES":
            schedule.every().hour.do(self._execute_scheduled_quest, quest_name).tag(quest_name)
        elif interval == "30 MINUTES":
            schedule.every(30).minutes.do(self._execute_scheduled_quest, quest_name).tag(quest_name)
        elif interval == "1 SEMAINE":
            schedule.every().monday.at("09:00").do(self._execute_scheduled_quest, quest_name).tag(quest_name)

    def _remove_quest_from_scheduler(self, quest_name):
        """Retirer une quête du scheduler"""
        schedule.clear(quest_name)

    def _execute_scheduled_quest(self, quest_name):
        """Exécuter une quête planifiée et logger les résultats"""
        try:
            results = self.execute_quest(quest_name)
            if results and len(results) > 0:
                # Stocker une alerte spéciale
                db_dir = self.db_path / self.sgbdr.current_db
                alerts_file = db_dir / "_quests_alerts.json"
                
                if alerts_file.exists():
                    with open(alerts_file, "r") as f:
                        alerts_data = json.load(f)
                else:
                    alerts_data = {"alerts": []}
                
                alert = {
                    "id": f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    "quest_name": quest_name,
                    "timestamp": datetime.now().isoformat(),
                    "results_count": len(results),
                    "message": f"Quête '{quest_name}' a trouvé {len(results)} résultats"
                }
                
                alerts_data["alerts"].append(alert)
                
                # Garder seulement les 100 dernières alertes
                if len(alerts_data["alerts"]) > 100:
                    alerts_data["alerts"] = alerts_data["alerts"][-100:]
                
                with open(alerts_file, "w") as f:
                    json.dump(alerts_data, f, indent=2)
                
                print(f"╔════════════════════════════════════")
                print(f"║ 🎯 ALERTE QUÊTE '{quest_name}' !")
                print(f"║ {len(results)} résultat(s) trouvé(s)")
                print(f"║ Alerte stockée dans _quests_alerts.json")
                print(f"╚════════════════════════════════════")
                
        except Exception as e:
            print(f"╔════════════════════════════════════")
            print(f"║ Erreur dans la quête planifiée '{quest_name}': {e}")
            print(f"╚════════════════════════════════════")