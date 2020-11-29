import logging
import sys
from sqlalchemy import create_engine

def setup_logger(name):
    """
    This function sets up the logging function used throughout the module.
    """
    formatter = logging.Formatter('%(levelname)s - %(asctime)s - %(name)s - %(message)s')

    handler = logging.FileHandler('./ETL.log')
    terminal_handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    terminal_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.addHandler(terminal_handler)

    return logger

def create_db_connection():
    db_string = "postgresql://postgres:password@postgres_container/postgres"
    db = create_engine(db_string)
    return db