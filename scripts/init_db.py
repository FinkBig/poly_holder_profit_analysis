
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.settings import DEFAULT_DB_PATH
from src.db.schema import init_database

def main():
    db_path = Path(__file__).parent.parent / DEFAULT_DB_PATH
    print(f"Initializing database at {db_path}")
    if db_path.exists():
        print("File exists, deleting...")
        db_path.unlink()
    
    init_database(str(db_path))
    print("Database initialized.")

if __name__ == "__main__":
    main()
