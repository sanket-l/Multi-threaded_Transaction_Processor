from .store import RockStore


def open_database(path, options=None):
    """
    Context manager for opening a RockStore database.

    Args:
        path (str): Path to the database directory
        options (dict, optional): Configuration options for the database.
            See RockStore class documentation for supported options.

    Returns:
        RockStore: The database instance

    Example:
        >>> with open_database('my_db', options={'read_only': True}) as db:
        ...     value = db.get(b'some_key')
        ...     print(value)
    """
    if options is None:
        options = {}

    db = RockStore(path, options=options)
    return db
