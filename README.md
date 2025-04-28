# iceduck
DuckDB wrapper with iceberg writer

## usage

### duckdb 
```
import iceduck as duckdb

with duckdb.connect() as con:  
        con.sql("CREATE TABLE test (i INTEGER)")
        con.sql("INSERT INTO test VALUES (42)")
        df = con.execute("select * from test").fetch_df()
        print(df) 
```

### duckdb with Iceberg ( using local sqllite catalog )
Create a local folder
```
mkdir warehouse
```

```
import iceduck as duckdb

with duckdb.connect() as con:  
        con.sql("CREATE TABLE test (i INTEGER)")
        con.sql("INSERT INTO test VALUES (42)")
        
        warehouse_path = "./warehouse"
        params = {
                'type': 'sql',
                "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
                "warehouse": f"file://{warehouse_path}",
        }
    
        catalog = con.load_catalog(name='local', **params)

        #create a namespace
        if catalog._namespace_exists("default"):
            print("Namespace already exists")
        else:
            print("Creating namespace") 
            catalog.create_namespace("default")
          
       
        query_result = con.execute("select * from test")
        result = query_result.upsert_iceberg('default.test_iceberg')
        print(result)
```

### Read iceberg table

``` 
import iceduck as duckdb

with duckdb.connect() as con: 
    warehouse_path = "./warehouse"
    params = {
            'type': 'sql',
            "uri": f"sqlite:///{warehouse_path}/pyiceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
    }
    
    catalog = con.load_catalog(name='local', **params)
    table = catalog.load_table("default.test_iceberg")
    print(table.scan().to_arrow())
```
 



