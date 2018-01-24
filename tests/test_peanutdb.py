"""PeanutDB Tests"""
from peanutdb import PeanutDB

"""
Check,
- create_table
    - 1) with schema
    - 2) without schema
    - 3) does not create table if schema type is incorrect
- insert
    - 1) with schema
    - 2) without schema
    - 3) does not insert fields into table if fields does not match with schema
- select
    - 1) selects the whole table if parameters are not provided
    - 2) selects according to parameters
    - 3) does not select if inputs are incorrect
- update
    - 1) according to schema if schema exists
    - 2) can add new fields if schema does not exist
    - 3) does not update if input is incorrect
- delete
    - 1) deletes according to parameters
    - 2) does not delete data if input is incorrect
- delete_table
    - 1) deletes table according to name
    - 2) does not delete if input is incorrect

--- None should be returned in case of failure

"""

DB = PeanutDB()

def test_create_table_1():
    """
    with schema
    """
    table_name = "table_1"
    schema = {
        "field_1": {"type": "number", "unique": True, "notnull": True},
        "field_2": {"type": "text", "unique": True, "notnull": True},
        "field_3": {"type": "boolean", "unique": False, "notnull": True},
        "field_4": {"type": "list", "unique": False, "notnull": False},
        "field_5": {"type": "dict", "unique": False, "notnull": False},
        "field_6": {"type": "any", "unique": False, "notnull": False}
    }

    result = DB.create_table(
        table_name=table_name,
        schema=schema
    )

    schema["__ID"] = {"type": "text", "unique": True, "notnull": True}
    assert(
        DB._db["__schemas__"][table_name] == schema and
        result == schema and
        table_name in DB._db
    )

def test_create_table_2():
    """
    without schema
    """
    table_name = "table_2"
    
    result = DB.create_table(
        table_name=table_name
    )

    assert(
        DB._db["__schemas__"][table_name] is None and
        result is None and
        table_name in DB._db
    )

def test_create_table_3():
    """
    does not create table if schema type is incorrect
    """
    result_1 = DB.create_table(
        table_name=""
    )
    result_2 = DB.create_table(
        table_name=1234
    )
    result_3 = DB.create_table(
        table_name="table_3",
        schema={
            "field_1": {"type": "abcd", "unique": False, "notnull": False}
        }
    )
    result_4 = DB.create_table(
        table_name="table_3",
        schema={
            "field_1": {"type": "text", "unique": 12345, "notnull": False}
        }
    )
    result_5 = DB.create_table(
        table_name="table_3",
        schema={
            "field_1": {"type": "text", "unique": False, "notnull": "False"}
        }
    )
    result_6 = DB.create_table(
        table_name="table_1"
    )

    assert(
        result_1 is None and
        result_2 is None and
        result_3 is None and
        result_4 is None and
        result_5 is None and
        result_6 is None and
        "table_3" not in DB._db and
        "table_3" not in DB._db["__schemas__"]
    )

def test_insert_1():
    """
    with schema
    """
    fields = {
        "field_1": 1,
        "field_2": "john",
        "field_3": True,
        "field_4": [
            "item 1",
            234
        ],
        "field_5": {"type": "dict", "unique": False, "notnull": False},
        "field_6": "This is any type"
    }
    result = DB.insert(
        table_name="table_1",
        fields=fields
    )

    assert(
        result is not None and
        result[0]["field_1"] == fields["field_1"] and
        DB._db["table_1"][0]["field_1"] == fields["field_1"]
    )

def test_insert_2():
    """
    without schema
    """
    fields = {
        "field_1": "1",
        "field_2": "john"
    }
    result_1 = DB.insert(
        table_name="table_2",
        fields=fields
    )

    fields_2 = {
        "field_6": True,
        "field_8": "john"
    }
    result_2 = DB.insert(
        table_name="table_2",
        fields=fields_2
    )

    assert(
        result_1 is not None and
        result_2 is not None and
        result_1[0]["field_1"] == fields["field_1"] and
        result_2[0]["field_6"] == fields_2["field_6"] and
        DB._db["table_2"][0]["field_1"] == fields["field_1"] and
        DB._db["table_2"][1]["field_6"] == fields_2["field_6"]
    )

def test_insert_3():
    """
    does not insert fields into table if fields does not match with schema
    """
    fields = {
        "field_1": 1,
        "field_2": "jane",
        "field_3": True,
        "field_4": [
            "item 1",
            234
        ],
        "field_5": {"type": "dict", "unique": False, "notnull": False},
        "field_6": "This is any type"
    }
    result_1 = DB.insert(
        table_name="table_1",
        fields=fields
    )

    fields["field_1"] = "2"
    result_2 = DB.insert(
        table_name="table_1",
        fields=fields
    )
    
    del fields["field_1"]
    result_3 = DB.insert(
        table_name="table_1",
        fields=fields
    )

    assert(
        result_1 is None and
        result_2 is None and
        result_3 is None and
        len(DB._db["table_1"]) == 1
    )

def test_select_1():
    """
    selects the whole table if parameters are not provided
    """
    table_name = "table_2"
    result = DB.select(
        table_name=table_name
    )

    assert(
        result == DB._db[table_name]
    )

def test_select_2():
    """
    selects according to parameters
    """
    table_name = "table_2"
    result = DB.select(
        table_name=table_name,
        where={
            "field_1": "1"
        }
    )

    assert(
        result != DB._db[table_name] and
        result[0] == DB._db[table_name][0]
    )

def test_select_3():
    """
    does not select if inputs are incorrect
    """
    table_name = "table_2"
    result_1 = DB.select(
        table_name=table_name,
        where={
            "field_1": "3"
        }
    )
    result_2 = DB.select(
        table_name=table_name,
        where={
            "field_5": "3"
        }
    )

    assert(
        result_1 is None and
        result_2 is None
    )

def test_update_1():
    """
    according to schema if schema exists
    """
    table_name = "table_1"
    result_1 = DB.update(
        table_name=table_name,
        fields={
            "field_2": "jane",
            "field_3": False
        },
        where={
            "field_2": "john"
        }
    )
    result_2 = DB.select(
        table_name=table_name,
        where={
            "field_2": "john"
        }
    )
    result_3 = DB.select(
        table_name=table_name,
        where={
            "field_2": "jane"
        }
    )

    assert(
        result_1[0]["field_2"] == "jane" and
        result_2 is None and
        result_3[0]["field_2"] == "jane" and
        DB._db[table_name][0]["field_2"] == "jane"
    )

def test_update_2():
    """
    can add new fields if schema does not exist
    """
    table_name = "table_2"
    result_1 = DB.update(
        table_name=table_name,
        fields={
            "field_1": "2",
            "field_12": "New Field"
        },
        where={
            "field_1": "1"
        }
    )

    assert(
        result_1[0]["field_12"] == "New Field" and
        result_1[0]["field_1"] == "2" and
        DB._db[table_name][0]["field_12"] == "New Field" and
        DB._db[table_name][0]["field_1"] == "2"
    )

def test_update_3():
    """
    does not update if input is incorrect
    """
    table_name = "table_2"
    table_name_2 = "table_1"
    result_1 = DB.update(
        table_name=table_name,
        fields={
            "field_1": "2",
            "field_12": "New Field"
        },
        where={
            "field_8": "1"
        }
    )
    result_2 = DB.update(
        table_name=table_name_2,
        fields={
            "field_1": "2",
            "field_12": "New Field"
        },
        where={
            "field_8": "1"
        }
    )
    result_3 = DB.update(
        table_name=table_name_2,
        fields={
            "field_1": "2",
            "field_12": "New Field"
        },
        where={
            "field_1": "1"
        }
    )
    result_4 = DB.update(
        table_name=table_name_2,
        fields={
            "field_1": 2,
            "field_12": "New Field"
        },
        where={
            "field_1": 1
        }
    )

    assert(
        result_2 is None and
        result_3 is None and
        result_4 is None and
        result_1[0]["field_12"] == "New Field" and
        result_1[0]["field_1"] == "2" and
        DB._db[table_name][0]["field_12"] == "New Field" and
        DB._db[table_name][0]["field_1"] == "2" and
        DB._db[table_name_2][0]["field_1"] == 1
    )

def test_delete_1():
    """
    deletes according to parameters
    """
    table_name = "table_2"
    result_1 = DB.delete(
        table_name=table_name,
        where={
            "field_1": "2"
        }
    )

    assert(
        result_1 is not None and
        len(DB._db[table_name]) == 1
    )

def test_delete_2():
    """
    does not delete data if input is incorrect
    """
    table_name = "table_1"
    result_1 = DB.delete(
        table_name=table_name,
        where={
            "field_1": "2"
        }
    )

    assert(
        result_1 is None and
        len(DB._db[table_name]) == 1
    )

def test_delete_table_1():
    """
    deletes table according to name
    """
    table_name = "table_1"
    to_be_deleted = DB._db[table_name]
    result_1 = DB.delete_table(
        table_name=table_name
    )

    assert(
        result_1 == to_be_deleted and
        table_name not in DB._db["__schemas__"] and
        table_name not in DB._db
    )

def test_delete_table_2():
    """
    does not delete if input is incorrect
    """
    table_name = "table_1"
    result_1 = DB.delete_table(
        table_name=table_name
    )
    result_2 = DB.delete_table(
        table_name=""
    )
    result_3 = DB.delete_table(
        table_name="abcd"
    )
    result_4 = DB.delete_table(
        table_name=1234
    )

    assert(
        result_1 is None and
        result_2 is None and
        result_3 is None and
        result_4 is None
    )
