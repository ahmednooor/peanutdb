"""PeanutDB Tests"""
from peanutdb import PeanutDB

def test_create_table():
    """test_create_table
    check if table gets created
    """
    db = PeanutDB()
    table_name = "Employees"
    schema = {
        "Name": {"type": "text", "unique": False, "notnull": True},
        "Phone": {"type": "number", "unique": True, "notnull": True},
        "Email": {"type": "list", "unique": True, "notnull": True},
        "Address": {"type": "dict", "unique": False, "notnull": False}
    }
    db.create_table(
        table_name=table_name,
        schema=schema
    )
    assert "Employees" in db._db["__schemas__"] and "Employees" in db._db

def test_delete_table():
    """test_delete_table
    check if table gets deleted
    """
    db = PeanutDB()
    table_name = "Employees"
    schema = {
        "Name": {"type": "text", "unique": False, "notnull": True},
        "Phone": {"type": "number", "unique": True, "notnull": True},
        "Email": {"type": "list", "unique": True, "notnull": True},
        "Address": {"type": "dict", "unique": False, "notnull": False}
    }
    db.create_table(
        table_name=table_name,
        schema=schema
    )
    table_was_created = False
    if "Employees" in db._db["__schemas__"] and "Employees" in db._db:
        table_was_created = True
    db.delete_table(
        table_name=table_name
    )
    assert (
        table_was_created is True and
        "Employees" not in db._db["__schemas__"] and
        "Employees" not in db._db
    )

def test_catch_create_table():
    """test_catch_anomally_create_table
    check table sould not be created if,
    - table name is not a non-empty string,
    - type not supported e.g. "abcd" instead of "number"/"text etc.
    """
    db = PeanutDB()
    table_name = 1234
    schema = {
        "Name": {"type": "abcdefg", "unique": False, "notnull": True}
    }
    db.create_table(
        table_name=table_name,
        schema=schema
    )
    assert "Employees" not in db._db["__schemas__"] and "Employees" not in db._db

def test_insert():
    """test_insert
    check if values get inserted into table
    """
    db = PeanutDB()
    table_name = "Employees"
    schema = {
        "Name": {"type": "text", "unique": False, "notnull": True},
        "Phone": {"type": "number", "unique": True, "notnull": True}
    }
    db.create_table(
        table_name=table_name,
        schema=schema
    )
    result = db.insert(
        table_name=table_name,
        fields={
            "Name": "John Doe",
            "Phone": 123456789
        }
    )
    assert (
        result is not False and
        db._db[table_name][0]["Name"] == "John Doe" and
        db._db[table_name][0]["Phone"] == 123456789
    )

def test_catch_insert():
    """test_create_table
    check if invalid type of fields not get inserted
    """
    db = PeanutDB()
    table_name = "Employees"
    schema = {
        "Name": {"type": "text", "unique": False, "notnull": True},
        "Phone": {"type": "number", "unique": True, "notnull": True}
    }
    db.create_table(
        table_name=table_name,
        schema=schema
    )
    result = db.insert(
        table_name=table_name,
        fields={
            "Name": "John Doe",
            "Phone": "123456789"
        }
    )
    assert (
        len(db._db[table_name]) == 0 and
        result is False
    )

def test_select():
    """test_select
    check if values get selected
    """
    db = PeanutDB()
    table_name = "Employees"
    schema = {
        "Name": {"type": "text", "unique": False, "notnull": True},
        "Phone": {"type": "number", "unique": True, "notnull": True}
    }
    db.create_table(
        table_name=table_name,
        schema=schema
    )
    result = db.insert(
        table_name=table_name,
        fields={
            "Name": "John Doe",
            "Phone": 123456789
        }
    )
    result_b = db.select(
        table_name=table_name,
        where={
            "Phone": 123456789
        }
    )
    assert (
        result is not False and
        result_b is not False and
        db._db[table_name][0]["Name"] == result_b[0]["Name"] and
        db._db[table_name][0]["Phone"] == result_b[0]["Phone"]
    )

def test_update():
    """test_update
    check if values get updated
    """
    db = PeanutDB()
    table_name = "Employees"
    schema = {
        "Name": {"type": "text", "unique": False, "notnull": True},
        "Phone": {"type": "number", "unique": True, "notnull": True}
    }
    db.create_table(
        table_name=table_name,
        schema=schema
    )
    result = db.insert(
        table_name=table_name,
        fields={
            "Name": "John Doe",
            "Phone": 123456789
        }
    )
    result_b = db.update(
        table_name=table_name,
        fields={
            "Name": "Steve Doe"
        },
        where={
            "Phone": 123456789
        }
    )
    assert (
        result is not False and
        result_b is not False and
        db._db[table_name][0]["Name"] == "Steve Doe"
    )

def test_delete():
    """test_delete
    check if values get deleted
    """
    db = PeanutDB()
    table_name = "Employees"
    schema = {
        "Name": {"type": "text", "unique": False, "notnull": True},
        "Phone": {"type": "number", "unique": True, "notnull": True}
    }
    db.create_table(
        table_name=table_name,
        schema=schema
    )
    result = db.insert(
        table_name=table_name,
        fields={
            "Name": "John Doe",
            "Phone": 123456789
        }
    )
    result_b = db.delete(
        table_name=table_name,
        where={
            "Phone": 123456789
        }
    )
    assert (
        result is not False and
        result_b is not False and
        result_b[0]["Name"] == "John Doe" and
        result_b[0]["Phone"] == 123456789 and
        len(db._db[table_name]) == 0
    )