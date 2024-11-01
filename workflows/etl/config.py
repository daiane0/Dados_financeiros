def db_config(db, table):
    
    conf = {
        "url":f"jdbc:postgresql://localhost:5432/{db}",
        "dbtable":table,
        "user":"postgres",
        "password":"fcndq@9000",
        "driver":"org.postgresql.Driver"
    }

    return conf

