sql_create_links_table = """
    CREATE TABLE IF NOT EXISTS links (
        id integer PRIMARY KEY,
        path text NOT NULL,
        domain text,
        protocol text
    )
"""