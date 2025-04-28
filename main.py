# import duckdb
import iceduck as duckdb

def teste1():
    with duckdb.connect() as con: 
        print('teste 1') 
        con.sql("CREATE TABLE test (i INTEGER)")
        con.sql("INSERT INTO test VALUES (42)")
        df = con.table("test").arrow()
    return df
        
def teste2():
    with duckdb.connect() as con: 
        print('teste 2') 
        con.sql("CREATE TABLE test (i INTEGER)")
        con.sql("INSERT INTO test VALUES (42)")
        df = con.execute("select * from test").fetch_df()
    return df
        
        
def teste3():
    with duckdb.connect() as con: 
        print('teste 3') 
        con.sql("CREATE TABLE test (i INTEGER)")
        con.sql("INSERT INTO test VALUES (42)")
        df = con.execute("select * from test").arrow()
    return df
        
        
def teste4():
    with duckdb.connect() as con: 
        print('teste 4') 
        con.sql("CREATE TABLE test (i INTEGER)")
        con.sql("INSERT INTO test VALUES (42)")
        df = con.execute("select * from test").fetchnumpy()
    return df   
    
def teste5():
    with duckdb.connect() as con: 
        print('teste 5')
        print(con)
        con.sql("CREATE TABLE test (i INTEGER)")
        con.sql("INSERT INTO test VALUES (42)")
        df = con.execute("select * from test").pl()
    return df

def teste6():
    with duckdb.connect() as con: 
        print('teste 6') 
        con.sql("CREATE TABLE test (i INTEGER)")
        con.sql("INSERT INTO test VALUES (42)")
        
        warehouse_path = "./warehouse"
        params = {
                'type': 'sql',
                "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                "warehouse": f"file://{warehouse_path}",
        }
    
        catalog = con.load_catalog(name='local', **params)
          
        con.execute("select * from test").to_iceberg('default3.test_iceberg')
        df = con.execute("select * from test").upsert_iceberg('default3.test_iceberg')
    return df
        
# def create_catalog():
#     # from pyiceberg.catalog import Catalog
#     # from pyiceberg.catalog.s3 import S3Catalog

#     # catalog = S3Catalog(
#     #     warehouse="s3://warehouse/wh",
#     #     region="us-east-1",
#     #     access_key_id="your_access_key_id",
#     #     secret_access_key="your_secret_access_key",
#     # )
#     from pyiceberg.catalog import load_catalog

#     warehouse_path = "./warehouse"
#     catalog = load_catalog(
#         "default",
#         **{
#             'type': 'sql',
#             "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
#             "warehouse": f"file://{warehouse_path}",
#         },
#     )
#     return catalog

# def create_namespace(catalog,name_space):
#     try:
#         catalog.create_namespace(name_space)
#     except Exception as e:
#         print(f"Namespace {name_space} already exists: {e}")
#         return False
#     return True
 
# def create_table(catalog, table_name, schema):
#     try:
#         catalog.create_table(
#             identifier=table_name,
#             schema=schema,
#         )
#     except Exception as e:
#         print(f"Table {table_name} already exists: {e}")
#         return False
#     return True

# def create_iceberg_table(catalog, table_name, arrow_table):
#     try:
#         tbl = catalog.create_table(
#             identifier=table_name,
#             schema=arrow_table.schema,
#             partition_spec=PartitionSpec(PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="city_identity"))
#         )
#         tbl.append(arrow_table)
#     except Exception as e:
#         print(f"Table {table_name} already exists: {e}")
#         return False
#     return True

# def upsert_iceberg_table(catalog, table_name, arrow_table):
#     try:
#         tbl = catalog.load_table(table_name)
#         tbl.upsert(arrow_table)
#     except Exception as e:
#         print(f"Table {table_name} does not exist: {e}")
#         return False
#     return True
    
    
def main():
    df_arrow = teste1()
    print(df_arrow)
    
    df_pandas = teste2()
    print(df_pandas)
    
    
    df_pandas = teste3()
    print(df_pandas)
    
    
    df_pandas = teste4()
    print(df_pandas)
    
    
    df_pandas = teste5()
    print(df_pandas)
     
    
    df_pandas = teste6()
    print(df_pandas)
     
    
  
def temp():
    
    warehouse_path = "./warehouse"
    params = {
            'type': 'sql',
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
    }
    
    catalog = duckdb.load_catalog(name='local', **params)
    catalog = duckdb.load_catalog(name='local', **params)
    df_arrow = main()
    
    print(df_arrow)
     
    if catalog._namespace_exists("docs_example"):
        print("Namespace already exists")
    else:
        print("Creating namespace")
        # catalog.create_namespace("docs_example")
        catalog.create_namespace("docs_example")
    # ns = catalog.list_namespaces()

    # print(assert ns == [("docs_example",)])
    print(catalog.list_tables("docs_example"))
    import pyarrow as pa

    schema = pa.schema(
        [
            pa.field("foo", pa.string(), nullable=True),
            pa.field("bar", pa.int32(), nullable=False),
            pa.field("baz", pa.bool_(), nullable=True),
        ]
    )
    try:
        catalog.create_table(
            identifier="docs_example.bids",
            schema=schema,
        )
    except Exception as e:
        print(f"Table docs_example.bids already exists: {e}")
        # return False
    # print(catalog.list_tables("docs_example"))
    
    # table = catalog.load_table("docs_example.bids")
    # print(table.scan().to_arrow())
    from pyiceberg.schema import Schema
    from pyiceberg.types import IntegerType, NestedField, StringType

    import pyarrow as pa

    schema = Schema(
        NestedField(1, "city", StringType(), required=True),
        NestedField(2, "inhabitants", IntegerType(), required=True),
        # Mark City as the identifier field, also known as the primary-key
        identifier_field_ids=[1]
    )
    
    arrow_schema = pa.schema(
            [
                pa.field("city", pa.string(), nullable=False),
                pa.field("inhabitants", pa.int32(), nullable=False),
            ]
        )
    
    try:
        tbl = catalog.create_table("docs_example.cities", schema=schema)

        

        # Write some data
        df = pa.Table.from_pylist(
            [
                {"city": "Amsterdam", "inhabitants": 921402},
                {"city": "San Francisco", "inhabitants": 808988},
                {"city": "Drachten", "inhabitants": 45019},
                {"city": "Paris", "inhabitants": 2103000},
            ],
            schema=arrow_schema
        )
        tbl.append(df)
    except Exception as e:
        print(f"Table docs_example.cities already exists: {e}")
        # return False


    import pyarrow as pa

    df = pa.Table.from_pylist(
        [
            {"city": "Goiana", "inhabitants": 40},
             
        ],
            schema=arrow_schema
    )
    
    duckdb.upsert_iceberg_table(catalog,"docs_example.cities",df)
    
    tbl = catalog.load_table("docs_example.cities")
    #tbl.delete(delete_filter="city == 'Recife2'")
    print( tbl.scan().to_arrow().to_pandas() )
    
    duckdb.execute("INSTALL iceberg;")
    duckdb.execute("INSTALL https;")
    duckdb.execute("LOAD https;")
    duckdb.execute("LOAD iceberg;")
    duckdb.execute("set unsafe_enable_version_guessing = true;")
    
    # df = duckdb.execute("SELECT * FROM iceberg_scan('warehouse/docs_example.db/cities', allow_moved_paths = true);").fetchdf()
    # df = duckdb.execute("SELECT * FROM read_json('https://pokeapi.co/api/v2/pokemon/ditto')").arrow()
    df = duckdb.execute("SELECT * FROM read_json('file.json')").arrow()
    print(df)
    # from pyiceberg.table import StaticTable
    duckdb.upsert_iceberg_table(catalog,"docs_example.json_ice",df)
    # static_table = StaticTable.from_metadata(
    #     "s3://warehouse/wh/nyc.db/taxis/metadata/00002-6ea51ce3-62aa-4197-9cf8-43d07c3440ca.metadata.json"
    # )
    # static_table.show()
  
    
if __name__ == "__main__":
    main()
    
    
  