from sgbdr.sgbdr import SGBDR
import readline
import os
from pathlib import Path

def format_table(data):
    """Formater les résultats de LOOT en tableau ASCII"""
    if not data:
        return "║ Aucun loot trouvé dans cette quête ! ║"
    
    # Obtenir les colonnes à partir du premier enregistrement
    columns = list(data[0].keys())
    # Calculer la largeur max de chaque colonne
    widths = {col: max(len(col), max((len(str(row.get(col, ''))) for row in data), default=0)) for col in columns}
    
    # Construire le tableau
    table = []
    # Ligne supérieure
    table.append(f"╔{'═' * (sum(widths.values()) + len(columns) * 3 + 1)}╗")
    # En-tête
    header = "│" + "".join(f" {col:<{widths[col]}}  │" for col in columns)
    table.append(header)
    # Séparateur
    table.append(f"╠{'═' * (sum(widths.values()) + len(columns) * 3 + 1)}╣")
    # Lignes de données
    for row in data:
        line = "│" + "".join(f" {str(row.get(col, '')):<{widths[col]}}  │" for col in columns)
        table.append(line)
    # Ligne inférieure
    table.append(f"╚{'═' * (sum(widths.values()) + len(columns) * 3 + 1)}╝")
    
    return "\n".join(table)

def format_databases(databases):
    """Formater la liste des bases en tableau ASCII"""
    if not databases:
        return "║ Aucune map disponible dans cette quête ! ║"
    
    # Construire le tableau avec une seule colonne "Base"
    table = []
    width = max(len("Base"), max((len(db) for db in databases), default=0))
    table.append(f"╔{'═' * (width + 4)}╗")
    table.append(f"│ {'Base':<{width}}   │")
    table.append(f"╠{'═' * (width + 4)}╣")
    for db in databases:
        table.append(f"│ {db:<{width}}   │")
    table.append(f"╚{'═' * (width + 4)}╝")
    
    return "\n".join(table)

def format_tables(tables):
    """Formater la liste des tables en tableau ASCII simplifié"""
    if not tables:
        return "║ Aucune table craftée dans cette map ! ║"
    
    # Construire un tableau simplifié
    table = []
    name_width = max(len("Nom"), max((len(t["name"]) for t in tables), default=0))
    cols_width = 30  # Largeur fixe pour les colonnes
    
    table.append(f"╔{'═' * (name_width + 4)}╦{'═' * (cols_width + 4)}╗")
    table.append(f"│ {'Nom':<{name_width}}   │ {'Colonnes':<{cols_width}}   │")
    table.append(f"╠{'═' * (name_width + 4)}╬{'═' * (cols_width + 4)}╣")
    
    for t in tables:
        # Formater les colonnes de manière concise
        cols_preview = ", ".join([f"{name}({info['type']})" for name, info in t["columns"].items()])
        if len(cols_preview) > cols_width:
            cols_preview = cols_preview[:cols_width-3] + "..."
        table.append(f"│ {t['name']:<{name_width}}   │ {cols_preview:<{cols_width}}   │")
    
    table.append(f"╚{'═' * (name_width + 4)}╩{'═' * (cols_width + 4)}╝")
    
    return "\n".join(table)


def format_users(users):
    """Formater la liste des utilisateurs en tableau ASCII"""
    if not users:
        return "║ Aucun joueur trouvé dans cette quête ! ║"
    
    # Construire le tableau avec colonnes "Login" et "Permissions"
    table = []
    widths = {
        "Login": max(len("Login"), max((len(u["login"]) for u in users), default=0)),
        "Permissions": max(len("Permissions"), max((len(str(u["permissions"])) for u in users), default=0))
    }
    table.append(f"╔{'═' * (widths['Login'] + 4)}╦{'═' * (widths['Permissions'] + 4)}╗")
    table.append(f"│ {'Login':<{widths['Login']}}   │ {'Permissions':<{widths['Permissions']}}   │")
    table.append(f"╠{'═' * (widths['Login'] + 4)}╬{'═' * (widths['Permissions'] + 4)}╣")
    for u in users:
        table.append(f"│ {u['login']:<{widths['Login']}}   │ {str(u['permissions']):<{widths['Permissions']}}   │")
    table.append(f"╚{'═' * (widths['Login'] + 4)}╩{'═' * (widths['Permissions'] + 4)}╝")
    
    return "\n".join(table)

def format_user_permissions(user_data):
    """Formater les permissions d’un utilisateur en tableau ASCII"""
    if not user_data:
        return "║ Aucune permission trouvée pour ce joueur ! ║"
    
    # Construire le tableau avec colonnes "Login" et "Permissions"
    table = []
    widths = {
        "Login": max(len("Login"), len(user_data[0]["login"])),
        "Permissions": max(len("Permissions"), len(str(user_data[0]["permissions"])))
    }
    table.append(f"╔{'═' * (widths['Login'] + 4)}╦{'═' * (widths['Permissions'] + 4)}╗")
    table.append(f"│ {'Login':<{widths['Login']}}   │ {'Permissions':<{widths['Permissions']}}   │")
    table.append(f"╠{'═' * (widths['Login'] + 4)}╬{'═' * (widths['Permissions'] + 4)}╣")
    table.append(f"│ {user_data[0]['login']:<{widths['Login']}}   │ {str(user_data[0]['permissions']):<{widths['Permissions']}}   │")
    table.append(f"╚{'═' * (widths['Login'] + 4)}╩{'═' * (widths['Permissions'] + 4)}╝")
    
    return "\n".join(table)


def format_transaction_status(status):
    """Formater le statut des transactions"""
    if not status["active"]:
        return "║ Aucune transaction active"
    
    table = []
    table.append(f"╔════════════════════════════════════")
    table.append(f"║ Transaction active : {status['current']}")
    table.append(f"║ Transactions empilées : {status['count']}")
    table.append(f"╚════════════════════════════════════")
    return "\n".join(table)


def format_views(views):
    """Formater la liste des vues en tableau ASCII"""
    if not views:
        return "║ Aucune vue craftée dans cette map ! ║"
    
    # Construire le tableau avec colonnes "Nom", "Requête", "Créée par"
    table = []
    widths = {
        "Nom": max(len("Nom"), max((len(v["name"]) for v in views), default=0)),
        "Requête": max(len("Requête"), max((len(v["query"]) for v in views), default=0)),
        "Créée par": max(len("Créée par"), max((len(v["created_by"]) for v in views), default=0))
    }
    
    # Ajuster la largeur de la requête pour ne pas être trop large
    widths["Requête"] = min(widths["Requête"], 50)
    
    table.append(f"╔{'═' * (widths['Nom'] + 4)}╦{'═' * (widths['Requête'] + 4)}╦{'═' * (widths['Créée par'] + 4)}╗")
    table.append(f"│ {'Nom':<{widths['Nom']}}   │ {'Requête':<{widths['Requête']}}   │ {'Créée par':<{widths['Créée par']}}   │")
    table.append(f"╠{'═' * (widths['Nom'] + 4)}╬{'═' * (widths['Requête'] + 4)}╬{'═' * (widths['Créée par'] + 4)}╣")
    
    for v in views:
        query_preview = v["query"][:47] + "..." if len(v["query"]) > 50 else v["query"]
        table.append(f"│ {v['name']:<{widths['Nom']}}   │ {query_preview:<{widths['Requête']}}   │ {v['created_by']:<{widths['Créée par']}}   │")
    
    table.append(f"╚{'═' * (widths['Nom'] + 4)}╩{'═' * (widths['Requête'] + 4)}╩{'═' * (widths['Créée par'] + 4)}╝")
    
    return "\n".join(table)

def format_snapshots(snapshots):
    """Formater la liste des snapshots"""
    if not snapshots:
        return "║ Aucun snapshot trouvé pour cette table ! ║"
    
    table = []
    widths = {
        "ID": max(len("ID"), max((len(s["id"]) for s in snapshots), default=0)),
        "Description": max(len("Description"), max((len(s["description"]) for s in snapshots), default=0)),
        "Date": max(len("Date"), max((len(s["created_at"]) for s in snapshots), default=0)),
        "Par": max(len("Par"), max((len(s["created_by"]) for s in snapshots), default=0)),
        "Lignes": max(len("Lignes"), max((len(str(s["row_count"])) for s in snapshots), default=0))
    }
    
    table.append(f"╔{'═' * (widths['ID'] + 4)}╦{'═' * (widths['Description'] + 4)}╦{'═' * (widths['Date'] + 4)}╦{'═' * (widths['Par'] + 4)}╦{'═' * (widths['Lignes'] + 4)}╗")
    table.append(f"│ {'ID':<{widths['ID']}}   │ {'Description':<{widths['Description']}}   │ {'Date':<{widths['Date']}}   │ {'Par':<{widths['Par']}}   │ {'Lignes':<{widths['Lignes']}}   │")
    table.append(f"╠{'═' * (widths['ID'] + 4)}╬{'═' * (widths['Description'] + 4)}╬{'═' * (widths['Date'] + 4)}╬{'═' * (widths['Par'] + 4)}╬{'═' * (widths['Lignes'] + 4)}╣")
    
    for s in snapshots:
        table.append(f"│ {s['id']:<{widths['ID']}}   │ {s['description']:<{widths['Description']}}   │ {s['created_at']:<{widths['Date']}}   │ {s['created_by']:<{widths['Par']}}   │ {s['row_count']:<{widths['Lignes']}}   │")
    
    table.append(f"╚{'═' * (widths['ID'] + 4)}╩{'═' * (widths['Description'] + 4)}╩{'═' * (widths['Date'] + 4)}╩{'═' * (widths['Par'] + 4)}╩{'═' * (widths['Lignes'] + 4)}╝")
    
    return "\n".join(table)

def format_quests(quests):
    """Formater la liste des quêtes"""
    if not quests:
        return "║ Aucune quête craftée ! ║"
    
    table = []
    widths = {
        "Nom": max(len("Nom"), max((len(q["name"]) for q in quests), default=0)),
        "Requête": 30,  # Largeur fixe
        "Intervalle": max(len("Intervalle"), max((len(q["interval"]) for q in quests), default=0)),
        "Dernière exécution": max(len("Dernière exécution"), max((len(str(q["last_run"])) for q in quests), default=0)),
        "Résultats": max(len("Résultats"), max((len(str(q["last_results"])) for q in quests), default=0)),
        "Statut": max(len("Statut"), max((len(q["status"]) for q in quests), default=0))
    }
    
    table.append(f"╔{'═' * (widths['Nom'] + 4)}╦{'═' * (widths['Requête'] + 4)}╦{'═' * (widths['Intervalle'] + 4)}╦{'═' * (widths['Dernière exécution'] + 4)}╦{'═' * (widths['Résultats'] + 4)}╦{'═' * (widths['Statut'] + 4)}╗")
    table.append(f"│ {'Nom':<{widths['Nom']}}   │ {'Requête':<{widths['Requête']}}   │ {'Intervalle':<{widths['Intervalle']}}   │ {'Dernière exécution':<{widths['Dernière exécution']}}   │ {'Résultats':<{widths['Résultats']}}   │ {'Statut':<{widths['Statut']}}   │")
    table.append(f"╠{'═' * (widths['Nom'] + 4)}╬{'═' * (widths['Requête'] + 4)}╬{'═' * (widths['Intervalle'] + 4)}╬{'═' * (widths['Dernière exécution'] + 4)}╬{'═' * (widths['Résultats'] + 4)}╬{'═' * (widths['Statut'] + 4)}╣")
    
    for q in quests:
        query_preview = q["query"][:27] + "..." if len(q["query"]) > 30 else q["query"]
        
        # CORRECTION : Gestion sécurisée de last_run
        last_run = q.get("last_run")
        if last_run and last_run != "Jamais":
            last_run = last_run[:16]  # Tronquer si nécessaire
        else:
            last_run = "Jamais"
            
        table.append(f"│ {q['name']:<{widths['Nom']}}   │ {query_preview:<{widths['Requête']}}   │ {q['interval']:<{widths['Intervalle']}}   │ {last_run:<{widths['Dernière exécution']}}   │ {q['last_results']:<{widths['Résultats']}}   │ {q['status']:<{widths['Statut']}}   │")
    
    table.append(f"╚{'═' * (widths['Nom'] + 4)}╩{'═' * (widths['Requête'] + 4)}╩{'═' * (widths['Intervalle'] + 4)}╩{'═' * (widths['Dernière exécution'] + 4)}╩{'═' * (widths['Résultats'] + 4)}╩{'═' * (widths['Statut'] + 4)}╝")
    
    return "\n".join(table)

def run_cli():
    """Lancer l'interface en ligne de commande"""
    dbms = SGBDR()
    # Activer l’historique des commandes
    history_file = ".sgbdr_history"
    try:
        readline.read_history_file(history_file)
    except FileNotFoundError:
        pass
    readline.set_history_length(1000)  # Limite à 1000 commandes
    
    print(f"╔════════════════════════════════════")
    print(f"║ Bienvenue dans SGBDR ! Tape tes sorts SQL, ou QUITTER pour arrêter.")
    print(f"╚════════════════════════════════════")
    while True:
        try:
            # Construire le prompt dynamique
            user = dbms.current_user if dbms.current_user else "anonyme"
            db = dbms.current_db if dbms.current_db else "aucune"
            prompt = f"SGBDR[{user}@{db}]> "
            query = input(prompt).strip()
            if query:
                readline.add_history(query)
                readline.write_history_file(history_file)
            if query.upper() == "QUITTER":
                print(f"╔════════════════════════════════════")
                print(f"║ À plus, aventurier ! La quête s’arrête ici !")
                print(f"╚════════════════════════════════════")
                break
            result = dbms.execute_query(query)
            if result is not None:  # Pour LOOT, LISTE BASES, LISTE TABLEAUX, LISTE JOUEURS, LISTE PERMISSIONS JOUEUR
                if query.upper() == "LISTE BASES":
                    print(format_databases(result))
                
                elif query.upper() == "LISTE TABLEAUX":
                    print(format_tables(result))

                elif query.upper() == "STATUS TRANSACTION":
                    print(format_transaction_status(result))
                
                elif query.upper() == "LISTE JOUEURS":
                    print(format_users(result))
                
                elif query.upper().startswith("LISTE PERMISSIONS JOUEUR"):
                    print(format_user_permissions(result))
                
                elif query.upper().startswith("STATS TABLEAU"):
                    pass  # Les statistiques sont déjà affichées dans table_stat
                
                elif query.upper() == "LISTE VUES":
                    print(format_views(result))
                
                elif query.upper().startswith("LISTE SNAPSHOTS TABLEAU"):
                    print(format_snapshots(result))
                
                elif query.upper().startswith("SNAPSHOT TABLEAU"):
                    # Les snapshots créés retournent des données formatées spéciales
                    if isinstance(result, list) and result and isinstance(result[0], dict):
                        print(format_table(result))
                
                elif query.upper().startswith("VOIR SNAPSHOT"):
                    # Les visualisations de snapshot retournent les données de la table
                    if isinstance(result, list) and result and isinstance(result[0], dict):
                        print(format_table(result))

                elif query.upper() == "LISTE QUETES":
                    print(format_quests(result))

                
                elif query.upper() == "LISTE BASES":
                    print(format_databases(result))
               
                else:  # LOOT
                    print(format_table(result))
        except ValueError as e:
            print(f"╔════════════════════════════════════")
            print(f"║  Erreur : {e}")
            print(f"╚════════════════════════════════════")
        except KeyboardInterrupt:
            print(f"\n╔════════════════════════════════════")
            print(f"║ À plus, aventurier ! La quête s’arrête ici !")
            print(f"╚════════════════════════════════════")
            readline.write_history_file(history_file)
            break

if __name__ == "__main__":
    run_cli()