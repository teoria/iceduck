
      
def create_catalog(warehouse_path = "./warehouse"):
    # from pyiceberg.catalog import Catalog
    # from pyiceberg.catalog.s3 import S3Catalog

    # catalog = S3Catalog(
    #     warehouse="s3://warehouse/wh",
    #     region="us-east-1",
    #     access_key_id="your_access_key_id",
    #     secret_access_key="your_secret_access_key",
    # )
    from pyiceberg.catalog import load_catalog

    
    catalog = load_catalog(
        "default",
        **{
            'type': 'sql',
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )
    return catalog

def create_namespace(catalog,name_space):
    try:
        catalog.create_namespace(name_space)
    except Exception as e:
        print(f"Namespace {name_space} already exists: {e}")
        return False
    return True
 
def create_table(catalog, table_name, schema):
    try:
        catalog.create_table(
            identifier=table_name,
            schema=schema,
        )
    except Exception as e:
        print(f"Table {table_name} already exists: {e}")
        return False
    return True

def create_iceberg_table(catalog, table_name, arrow_table):
    try:
        tbl = catalog.create_table(
            identifier=table_name,
            schema=arrow_table.schema,
            partition_spec=PartitionSpec(PartitionField(source_id=1, field_id=1001, transform=IdentityTransform(), name="city_identity"))
        )
        tbl.append(arrow_table)
    except Exception as e:
        print(f"Table {table_name} already exists: {e}")
        return False
    return True

def upsert_iceberg_table(catalog, table_name, arrow_table):
    try:
        tbl = catalog.load_table(table_name)
        tbl.upsert(arrow_table)
    except Exception as e:
        print(f"Table {table_name} does not exist: {e}")
        return False
    return True
    
  