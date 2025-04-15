import iceduck as duckdb

def main():
    with duckdb.connect() as con:
        con.sql("CREATE TABLE test (i INTEGER)")
        con.sql("INSERT INTO test VALUES (42)")
        df = con.table("test").arrow()
    return df
        
if __name__ == "__main__":
     
    df_arrow = main()
    
    print(df_arrow)