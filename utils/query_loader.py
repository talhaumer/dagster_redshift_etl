import os


def load_query(filename: str) -> str:
    """
    Load a SQL query from the queries folder.
    Args:
        filename (str): Name of the SQL file.
    Returns:
        str: SQL query as a string.
    """
    base_dir = os.path.dirname(os.path.dirname(__file__))
    queries_dir = os.path.join(base_dir, "queries")
    with open(os.path.join(queries_dir, filename), "r") as f:
        return f.read()
