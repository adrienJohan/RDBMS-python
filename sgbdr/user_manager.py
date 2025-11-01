import json
import hashlib
from pathlib import Path

class UserManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.users_file = self.db_path / "users.json"
        # Initialiser le fichier des utilisateurs avec un admin par défaut
        if not self.users_file.exists():
            admin_password = "admin123"
            hashed_password = hashlib.sha256(admin_password.encode()).hexdigest()
            with open(self.users_file, "w") as f:
                json.dump({
                    "admin": {
                        "password": hashed_password,
                        "permissions": ["read", "write", "delete", "admin"]
                    }
                }, f, indent=2)

    def set_sgbdr(self, sgbdr):
        """Définir la référence à l’instance SGBDR"""
        self.sgbdr = sgbdr

    def create_user(self, login, password, permissions):
        """Créer un utilisateur avec un login, mot de passe et permissions"""
        if not self._check_admin_permission():
            raise ValueError("Seuls les admins peuvent crafter des joueurs !")
        with open(self.users_file, "r+") as f:
            users = json.load(f)
            if login in users:
                print(f"╔════════════════════════════════════")
                print(f"║ Ce joueur {login} est déjà crafté ! Choisis un autre pseudo")
                print(f"╚════════════════════════════════════")
                return
            valid_permissions = ["read", "write", "delete"]
            if not all(p in valid_permissions for p in permissions):
                raise ValueError("Permissions cheatées ! Options : read, write, delete")
            hashed_password = hashlib.sha256(password.encode()).hexdigest()
            users[login] = {"password": hashed_password, "permissions": permissions}
            f.seek(0)
            json.dump(users, f, indent=2)
        print(f"╔════════════════════════════════════")
        print(f"║ Joueur {login} crafté avec pouvoirs {permissions} !")
        print(f"╚════════════════════════════════════")

    def login_user(self, login, password):
        """Connecter un utilisateur"""
        with open(self.users_file, "r") as f:
            users = json.load(f)
        hashed_password = hashlib.sha256(password.strip().encode()).hexdigest()
        if login in users and users[login]["password"] == hashed_password:
            self.sgbdr.current_user = login
            print(f"╔════════════════════════════════════")
            print(f"║ Login réussi, {login} ! T’es dans l’arène !")
            print(f"╚════════════════════════════════════")
        else:
            raise ValueError("Pseudo ou mot de passe incorrect ! Réessaie.")

    def edit_user_permissions(self, login, permissions):
        """Modifier les permissions d’un utilisateur"""
        if not self._check_admin_permission():
            raise ValueError("Seuls les admins peuvent modifier les pouvoirs des joueurs !")
        with open(self.users_file, "r+") as f:
            users = json.load(f)
            if login not in users:
                raise ValueError(f"Joueur {login} introuvable. T’as raté le pseudo ?")
            valid_permissions = ["read", "write", "delete"]
            if not all(p in valid_permissions for p in permissions):
                raise ValueError("Permissions cheatées ! Options : read, write, delete")
            users[login]["permissions"] = permissions
            f.seek(0)
            json.dump(users, f, indent=2)
            f.truncate()
        print(f"╔════════════════════════════════════")
        print(f"║ Pouvoirs de {login} mis à jour : {permissions} !")
        print(f"╚════════════════════════════════════")

    def list_users(self):
        """Lister tous les utilisateurs"""
        with open(self.users_file, "r") as f:
            users = json.load(f)
        result = [{"login": login, "permissions": data["permissions"]} for login, data in users.items()]
        print(f"╔════════════════════════════════════")
        print(f"║ Joueurs dans l’arène : {len(result)} trouvés !")
        print(f"╚════════════════════════════════════")
        return result

    def list_user_permissions(self, login):
        """Lister les permissions d’un utilisateur spécifique"""
        with open(self.users_file, "r") as f:
            users = json.load(f)
        if login not in users:
            raise ValueError(f"Joueur {login} introuvable. T’as raté le pseudo ?")
        result = [{"login": login, "permissions": users[login]["permissions"]}]
        print(f"╔════════════════════════════════════")
        print(f"║ Pouvoirs de {login} : {len(result[0]['permissions'])} trouvés !")
        print(f"╚════════════════════════════════════")
        return result

    def check_permission(self, permission):
        """Vérifier si l’utilisateur actuel a la permission requise"""
        if not self.sgbdr.current_user:
            raise ValueError("Aucun joueur connecté. Faut d’abord lancer LOGIN JOUEUR !")
        permissions = self.get_user_permissions(self.sgbdr.current_user)
        if permission not in permissions and "admin" not in permissions:
            raise ValueError(f"Tu n’as pas le pouvoir {permission} ! Demande à un admin.")

    def get_user_permissions(self, login):
        """Récupérer les permissions d’un utilisateur"""
        with open(self.users_file, "r") as f:
            users = json.load(f)
        return users.get(login, {}).get("permissions", [])

    def _check_admin_permission(self):
        """Vérifier si l’utilisateur actuel est admin"""
        if not self.sgbdr.current_user:
            raise ValueError("Aucun joueur connecté. Faut d’abord lancer LOGIN JOUEUR !")
        return "admin" in self.get_user_permissions(self.sgbdr.current_user)

    def set_sgbdr(self, sgbdr):
        """Définir la référence à l’instance SGBDR"""
        self.sgbdr = sgbdr