# Fantasy DBMS - A Python Database Management System

[![Python](https://img.shields.io/badge/Python-3.8%2B-blue)](https://python.org)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Production%20Ready-brightgreen)]()

> *"Forge your databases, craft your tables, and embark on data quests!"*

A complete DBMS (Database Management System) developed in Python with a medieval fantasy RPG-style interface.

## Features

### Player System
- **Secure authentication** with SHA-256 hashing
- **Granular permissions**: read, write, delete, admin
- **Profile management** with role-based system

### Database Management
- **Database crafting**: Create new databases
- **Navigation**: Select and switch between databases
- **Export/Import**: Backup and restore in ZIP format
- **Listing**: View all available databases

### Table Crafting
- **Data types**: INT, FLOAT, TEXT, DATE, BOOLEAN, VARCHAR(n)
- **Advanced constraints**:
  - `PRIMARY KEY` - Primary keys
  - `FOREIGN KEY` - Foreign keys
  - `UNIQUE` - Unique values
  - `NOT NULL` - Required fields
- **Complete metadata** with relational schema

### Data Operations
- **POP DANS** - Data insertion
- **LOOT** - Selection with complex conditions (AND/OR)
- **EDIT** - Updates with SET clauses
- **DEPOP DANS** - Conditional deletion
- **TRIER PAR** - Multi-column sorting (ASC/DESC)

### Advanced Quests
- **Joins** between tables with conditions
- **Views** - Virtual tables based on queries
- **Snapshots** - Point-in-time table backups
- **Transactions** - ACID support (BEGIN, COMMIT, ROLLBACK)

###  Automated Quests
- **Scheduling**: Periodic execution (1 DAYS, 1 HOURS, 30 MINUTES, 1 WEEK)
- **History**: Retention of last 50 executions
- **Alerts**: Notification of significant results
- **Scheduler**: Background automatic execution

##  Installation

### Prerequisites
- Python 3.8 or higher
- No external dependencies (100% Python standard library)

### Installation
```bash
# Clone the repository
git clone https://github.com/your-username/fantasy-dbms.git
cd fantasy-dbms

# Launch the application
python cli.py