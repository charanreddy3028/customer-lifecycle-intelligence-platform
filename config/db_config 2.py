import yaml
# pyrefly: ignore [missing-import]
from sqlalchemy import create_engine

CONFIG_PATH = "config/config.yaml"

def load_config():
    with open(CONFIG_PATH, "r") as file:
        return yaml.safe_load(file)


def get_db_engine():
    config = load_config()
    db = config["database"]

    connection_string = (
        f"mysql+pymysql://{db['user']}:{db['password']}"
        f"@{db['host']}:{db['port']}/{db['name']}"
    )

    engine = create_engine(
        connection_string,
        pool_pre_ping=True  # prevents stale connections
    )

    return engine

# engine = create_engine(
#     f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
# )