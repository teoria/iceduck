from pyiceberg.catalog import load_catalog as pyiceberg_load_catalog
from pyiceberg.catalog import Catalog

from typing import Optional
from duckdb import DuckDBPyRelation, DuckDBPyConnection, duckdb, PythonExceptionHandling,Expression, Statement, StatementType
import pyarrow as pa
import pandas 

import duckdb.typing as typing
import duckdb.functional as functional
from duckdb.typing import DuckDBPyType
from duckdb.functional import FunctionNullHandling, PythonUDFType

import duckdb.typing as typing
import duckdb.functional as functional
from duckdb.typing import DuckDBPyType
from duckdb.functional import FunctionNullHandling, PythonUDFType
from duckdb.value.constant import (
    Value,
    NullValue,
    BooleanValue,
    UnsignedBinaryValue,
    UnsignedShortValue,
    UnsignedIntegerValue,
    UnsignedLongValue,
    BinaryValue,
    ShortValue,
    IntegerValue,
    LongValue,
    HugeIntegerValue,
    FloatValue,
    DoubleValue,
    DecimalValue,
    StringValue,
    UUIDValue,
    BitValue,
    BlobValue,
    DateValue,
    IntervalValue,
    TimestampValue,
    TimestampSecondValue,
    TimestampMilisecondValue,
    TimestampNanosecondValue,
    TimestampTimeZoneValue,
    TimeValue,
    TimeTimeZoneValue,
)

# We also run this in python3.7, where this is needed
from typing_extensions import Literal
# stubgen override - missing import of Set
from typing import Any, ClassVar, Set, Optional, Callable
from io import StringIO, TextIOBase
from pathlib import Path

from typing import overload, Dict, List, Union, Tuple
import pandas
# stubgen override - unfortunately we need this for version checks
import sys
import fsspec
import pyarrow.lib
import polars
      
def load_catalog(name: Optional[str] = None, **properties: Optional[str] ) -> Catalog:
    
    catalog = pyiceberg_load_catalog(
        name,
        **properties
       
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

def create_iceberg_table(catalog, table_name, arrow_table,partitions=None):
    try:
         
        tbl = catalog.create_table(
            identifier=table_name,
            schema=arrow_table.schema,
            #partition_spec=partitions
        )
        tbl.append(arrow_table)
    except Exception as e:
        print(f"Table {table_name} already exists: {e}")
        return False
    return True

def upsert_iceberg_table(catalog, table_name, arrow_table):
    try:
        tbl = catalog.load_table(table_name)
        schema = tbl.schema().as_arrow()
        print(f"Schema: {schema}")
        df = arrow_table.to_pandas()
       
        arrow_table = pa.Table.from_pandas(df,schema=schema)
        
        tbl.append(arrow_table)
    except Exception as e:
        print(f"upsert Table {table_name} error: {e}")
        create_iceberg_table(catalog, table_name, arrow_table)
        return False
    return True
    


class DuckDBPyRelationIceberg(DuckDBPyRelation):
    def __init__(self, *args, **kwargs):
        """
        Initialize the DuckDBPyRelationIceberg object.
        """
        super().__init__(*args)
      
        
    def to_iceberg(self, catalog: Catalog, table_name: str) -> bool:
        """
        Convert the DuckDBPyRelationIceberg object to an Iceberg table.
        """
        # Convert the relation to a Pandas DataFrame
        df = self.to_df()
        
        # Convert the DataFrame to an Arrow Table
        arrow_table = pa.Table.from_pandas(df)
        
        # Create the Iceberg table
        return create_iceberg_table(catalog, table_name, arrow_table)
    
    def upsert_iceberg(self, catalog: Catalog, table_name: str) -> bool:
        """
        Upsert the DuckDBPyRelationIceberg object to an Iceberg table.
        """
        # Convert the relation to a Pandas DataFrame
        df = self.to_df()
        
        # Convert the DataFrame to an Arrow Table
        arrow_table = pa.Table.from_pandas(df)
        
        # Upsert the Iceberg table
        return upsert_iceberg_table(catalog, table_name, arrow_table)

      

class DuckDBPyConnectionIceberg():
   
    def __init__(self, database=":memory:", read_only=False, config={}):
        """
        Initialize the DuckDBPyConnectionIceberg object.
        """ 
        # Create a connection to the DuckDB database
        self._conn = duckdb.connect(database=database, read_only=read_only, config=config)  
        self._catalog = None
        
    def __enter__(self): 
        return self
         
    def __exit__(self, exc_type, exc_value, exc_tb): 
        if exc_tb:
            print(exc_type, exc_value, exc_tb, sep="\n")  
        self._conn.close()   
    
 
    def sql(self, query: str, *, alias: str = "", params: object = None) : #-> DuckDBPyRelationIceberg:
        
        """
        Execute a SQL query and return the result as a DuckDBPyRelationIceberg object.
        """
        if not isinstance(query, str):
            raise TypeError("Query must be a string.")
         
        # Execute the query
        #result = self._conn.execute(query, params)
        result = self._conn.execute(query, params) 
        # Return the result as a DuckDBPyRelationIceberg object
        return result #DuckDBPyRelationIceberg(result, alias=alias)
    
    
    
    def cursor(self) -> DuckDBPyConnection: 
        """
        Return a cursor object for the connection.
        """
        return self._conn.cursor()
    
    def register_filesystem(self, filesystem) -> None: 
        """
        Register a filesystem with the connection.
        """
        self._conn.register_filesystem(filesystem)
        
    def unregister_filesystem(self, name: str) -> None: 
        """
        Unregister a filesystem from the connection.
        """
        self._conn.unregister_filesystem(name)
        
    def list_filesystems(self) -> list: 
        """
        List all registered filesystems.
        """
        return self._conn.list_filesystems()
    def filesystem_is_registered(self, name: str) -> bool: 
        """
        Check if a filesystem is registered.
        """
        return self._conn.filesystem_is_registered(name)
    # def create_function(self, name: str, function: function, parameters: Optional[List[DuckDBPyType]] = None, return_type: Optional[DuckDBPyType] = None, *, type: Optional[PythonUDFType] = PythonUDFType.NATIVE, null_handling: Optional[FunctionNullHandling] = FunctionNullHandling.DEFAULT, exception_handling: Optional[PythonExceptionHandling] = PythonExceptionHandling.DEFAULT, side_effects: bool = False) -> DuckDBPyConnection: 
    #     """
    #     Create a user-defined function (UDF) in the connection.
    #     """
    #     self._conn.create_function(name, function, parameters, return_type, type=type, null_handling=null_handling, exception_handling=exception_handling, side_effects=side_effects)
    #     return self
    def remove_function(self, name: str) -> DuckDBPyConnection: 
        """
        Remove a user-defined function (UDF) from the connection.
        """
        self._conn.remove_function(name)
        return self
    
    def sqltype(self, type_str: str) -> DuckDBPyType: 
        """
        Convert a SQL type string to a DuckDBPyType object.
        """
        return self._conn.sqltype(type_str)
    def dtype(self, type_str: str) -> DuckDBPyType: 
        """
        Convert a SQL type string to a DuckDBPyType object.
        """
        return self._conn.dtype(type_str)
    def type(self, type_str: str) -> DuckDBPyType: 
        """
        Convert a SQL type string to a DuckDBPyType object.
        """
        return self._conn.type(type_str)
    def array_type(self, type: DuckDBPyType, size: int) -> DuckDBPyType: 
        """
        Create an array type with the specified size.
        """
        return self._conn.array_type(type, size)
    def list_type(self, type: DuckDBPyType) -> DuckDBPyType:
        """
        Create a list type.
        """
        return self._conn.list_type(type)
    def union_type(self, members: DuckDBPyType) -> DuckDBPyType: 
        """
        Create a union type.
        """
        return self._conn.union_type(members)
    def string_type(self, collation: str = "") -> DuckDBPyType: 
        """
        Create a string type with the specified collation.
        """
        return self._conn.string_type(collation)
    def enum_type(self, name: str, type: DuckDBPyType, values: List[Any]) -> DuckDBPyType: 
        """
        Create an enum type with the specified name and values.
        """
        return self._conn.enum_type(name, type, values)
    def decimal_type(self, width: int, scale: int) -> DuckDBPyType: 
        """
        Create a decimal type with the specified width and scale.
        """
        return self._conn.decimal_type(width, scale)
    def struct_type(self, fields: Union[Dict[str, DuckDBPyType], List[str]]) -> DuckDBPyType: 
        """
        Create a struct type with the specified fields.
        """
        return self._conn.struct_type(fields)
    def row_type(self, fields: Union[Dict[str, DuckDBPyType], List[str]]) -> DuckDBPyType: 
        """
        Create a row type with the specified fields.
        """
        return self._conn.row_type(fields)
    def map_type(self, key: DuckDBPyType, value: DuckDBPyType) -> DuckDBPyType:     
        """
        Create a map type with the specified key and value types.
        """
        return self._conn.map_type(key, value)
    def duplicate(self) -> DuckDBPyConnection: 
        """
        Duplicate the connection.
        """
        return self._conn.duplicate()
    def execute(self, query: object, parameters: object = None) -> DuckDBPyConnection: 
        """
        Execute a SQL query and return the result as a DuckDBPyConnection object.
        """
        if not isinstance(query, str):
            raise TypeError("Query must be a string.")
        
        # Execute the query
        result = self._conn.execute(query, parameters)
        
        # Return the result as a DuckDBPyConnection object
        return self
    def executemany(self, query: object, parameters: object = None) -> DuckDBPyConnection: 
        """
        Execute a SQL query with multiple parameters and return the result as a DuckDBPyConnection object.
        """
        if not isinstance(query, str):
            raise TypeError("Query must be a string.")
        
        # Execute the query
        result = self._conn.executemany(query, parameters)
        
        # Return the result as a DuckDBPyConnection object
        return result
    def close(self) -> None: 
        """
        Close the connection.
        """
        self._conn.close()
    def interrupt(self) -> None: 
        """
        Interrupt the connection.
        """
        self._conn.interrupt()
    def fetchone(self) -> Optional[tuple]: 
        """
        Fetch one row from the result set.
        """
        return self._conn.fetchone()
    def fetchmany(self, size: int = 1) -> List[Any]: 
        """
        Fetch multiple rows from the result set.
        """
        return self._conn.fetchmany(size)
    def fetchall(self) -> List[Any]: 
        """
        Fetch all rows from the result set.
        """
        return self._conn.fetchall()
    def fetchnumpy(self) -> dict: 
        """
        Fetch the result set as a NumPy array.
        """
        return self._conn.fetchnumpy()
    def fetchdf(self, *, date_as_object: bool = False) -> pandas.DataFrame: 
        """
        Fetch the result set as a Pandas DataFrame.
        """
        return self._conn.fetchdf(date_as_object=date_as_object)
    def fetch_df(self, *, date_as_object: bool = False) -> pandas.DataFrame: 
        """
        Fetch the result set as a Pandas DataFrame.
        """
        return self._conn.fetch_df(date_as_object=date_as_object)
    def df(self, *, date_as_object: bool = False) -> pandas.DataFrame: 
        """
        Fetch the result set as a Pandas DataFrame.
        """
        return self._conn.df(date_as_object=date_as_object)
    def fetch_df_chunk(self, vectors_per_chunk: int = 1, *, date_as_object: bool = False) -> pandas.DataFrame: 
        """
        Fetch the result set as a Pandas DataFrame in chunks.
        """
        return self._conn.fetch_df_chunk(vectors_per_chunk=vectors_per_chunk, date_as_object=date_as_object)
    def pl(self, rows_per_batch: int = 1000000) -> polars.DataFrame: 
        """
        Fetch the result set as a Polars DataFrame.
        """
        return self._conn.pl(rows_per_batch=rows_per_batch)
    def fetch_arrow_table(self, rows_per_batch: int = 1000000) -> pyarrow.lib.Table: 
        """
        Fetch the result set as a PyArrow Table.
        """
        return self._conn.fetch_arrow_table(rows_per_batch=rows_per_batch)
    def arrow(self, rows_per_batch: int = 1000000) -> pyarrow.lib.Table: 
        """
        Fetch the result set as a PyArrow Table.
        """
        return self._conn.arrow(rows_per_batch=rows_per_batch)
    def fetch_record_batch(self, rows_per_batch: int = 1000000) -> pyarrow.lib.RecordBatchReader: 
        """
        Fetch the result set as a PyArrow RecordBatchReader.
        """
        return self._conn.fetch_record_batch(rows_per_batch=rows_per_batch)
    def torch(self) -> dict: 
        """
        Fetch the result set as a PyTorch tensor.
        """
        return self._conn.torch()
    def tf(self) -> dict: 
        """
        Fetch the result set as a TensorFlow tensor.
        """
        return self._conn.tf()
    def begin(self) -> DuckDBPyConnection: 
        """
        Begin a transaction.
        """
        return self._conn.begin()
    def commit(self) -> DuckDBPyConnection: 
        """
        Commit a transaction.
        """
        return self._conn.commit()
    def rollback(self) -> DuckDBPyConnection: 
        """
        Rollback a transaction.
        """
        return self._conn.rollback()
    def checkpoint(self) -> DuckDBPyConnection: 
        """
        Create a checkpoint.
        """
        return self._conn.checkpoint()
    def append(self, table_name: str, df: pandas.DataFrame, *, by_name: bool = False) -> DuckDBPyConnection: 
        """
        Append a DataFrame to a table.
        """
        return self._conn.append(table_name, df, by_name=by_name)
    def register(self, view_name: str, python_object: object) -> DuckDBPyConnection: 
        """
        Register a Python object as a view in the connection.
        """
        return self._conn.register(view_name, python_object)
    def unregister(self, view_name: str) -> DuckDBPyConnection: 
        """
        Unregister a view from the connection.
        """
        return self._conn.unregister(view_name)
    def table(self, table_name: str) -> DuckDBPyRelation: 
        """
        Get a DuckDBPyRelation object for the specified table.
        """
        return self._conn.table(table_name)
    def view(self, view_name: str) -> DuckDBPyRelation: 
        """
        Get a DuckDBPyRelation object for the specified view.
        """
        return self._conn.view(view_name)
    def values(self, *args: Union[List[Any],Expression, Tuple[Expression]]) -> DuckDBPyRelation: 
        """
        Create a DuckDBPyRelation object with the specified values.
        """
        return self._conn.values(*args)
    def table_function(self, name: str, parameters: object = None) -> DuckDBPyRelation: 
        """
        Create a DuckDBPyRelation object for the specified table function.
        """
        return self._conn.table_function(name, parameters)
    def read_json(self, path_or_buffer: Union[str, StringIO, TextIOBase], *, columns: Optional[Dict[str,str]] = None, sample_size: Optional[int] = None, maximum_depth: Optional[int] = None, records: Optional[str] = None, format: Optional[str] = None, date_format: Optional[str] = None, timestamp_format: Optional[str] = None, compression: Optional[str] = None, maximum_object_size: Optional[int] = None, ignore_errors: Optional[bool] = None, convert_strings_to_integers: Optional[bool] = None, field_appearance_threshold: Optional[float] = None, map_inference_threshold: Optional[int] = None, maximum_sample_files: Optional[int] = None, filename: Optional[bool | str] = None, hive_partitioning: Optional[bool] = None, union_by_name: Optional[bool] = None, hive_types: Optional[Dict[str, str]] = None, hive_types_autocast: Optional[bool] = None) -> DuckDBPyRelation: 
        """
        Read a JSON file and return the result as a DuckDBPyRelation object.
        """
        return self._conn.read_json(path_or_buffer, columns=columns, sample_size=sample_size, maximum_depth=maximum_depth, records=records, format=format, date_format=date_format, timestamp_format=timestamp_format, compression=compression, maximum_object_size=maximum_object_size, ignore_errors=ignore_errors, convert_strings_to_integers=convert_strings_to_integers, field_appearance_threshold=field_appearance_threshold, map_inference_threshold=map_inference_threshold, maximum_sample_files=maximum_sample_files, filename=filename, hive_partitioning=hive_partitioning, union_by_name=union_by_name, hive_types=hive_types, hive_types_autocast=hive_types_autocast)
    def extract_statements(self, query: str) -> List[Statement]: 
        """
        Extract statements from a SQL query.
        """
        return self._conn.extract_statements(query)
    def sql(self, query: str, *, alias: str = "", params: object = None) -> DuckDBPyRelation: 
        """
        Execute a SQL query and return the result as a DuckDBPyRelation object.
        """
        if not isinstance(query, str):
            raise TypeError("Query must be a string.")
        
        # Execute the query
        result = self._conn.execute(query, params)
        
        # Return the result as a DuckDBPyRelation object
        return result # DuckDBPyRelationIceberg(result, alias=alias)
    def query(self, query: str, *, alias: str = "", params: object = None) -> DuckDBPyRelation: 
        """
        Execute a SQL query and return the result as a DuckDBPyRelation object.
        """
        if not isinstance(query, str):
            raise TypeError("Query must be a string.")
        
        # Execute the query
        result = self._conn.execute(query, params)
        
        # Return the result as a DuckDBPyRelation object
        return result #DuckDBPyRelationIceberg(result, alias=alias)
    
    def from_query(self, query: str, *, alias: str = "", params: object = None) -> DuckDBPyRelation: 
        """
        Execute a SQL query and return the result as a DuckDBPyRelation object.
        """
        if not isinstance(query, str):
            raise TypeError("Query must be a string.")
        
        # Execute the query
        result = self._conn.execute(query, params)
        
        # Return the result as a DuckDBPyRelation object
        return result #DuckDBPyRelationIceberg(result, alias=alias)
    def read_csv(self, path_or_buffer: Union[str, StringIO, TextIOBase], *, header: Optional[bool | int] = None, compression: Optional[str] = None, sep: Optional[str] = None, delimiter: Optional[str] = None, dtype: Optional[Dict[str, str] | List[str]] = None, na_values: Optional[str| List[str]] = None, skiprows: Optional[int] = None, quotechar: Optional[str] = None, escapechar: Optional[str] = None, encoding: Optional[str] = None, parallel: Optional[bool] = None, date_format: Optional[str] = None, timestamp_format: Optional[str] = None, sample_size: Optional[int] = None, all_varchar: Optional[bool] = None, normalize_names: Optional[bool] = None, null_padding: Optional[bool] = None, names: Optional[List[str]] = None, lineterminator: Optional[str] = None, columns: Optional[Dict[str, str]] = None, auto_type_candidates: Optional[List[str]] = None, max_line_size: Optional[int] = None, ignore_errors: Optional[bool] = None, store_rejects: Optional[bool] = None, rejects_table: Optional[str] = None, rejects_scan: Optional[str] = None, rejects_limit: Optional[int] = None, force_not_null: Optional[List[str]] = None, buffer_size: Optional[int] = None, decimal: Optional[str] = None, allow_quoted_nulls: Optional[bool] = None, filename: Optional[bool | str] = None, hive_partitioning: Optional[bool] = None, union_by_name: Optional[bool] = None, hive_types: Optional[Dict[str, str]] = None, hive_types_autocast: Optional[bool] = None) -> DuckDBPyRelation: 
        """
        Read a CSV file and return the result as a DuckDBPyRelation object.
        """
        return self._conn.read_csv(path_or_buffer, header=header, compression=compression, sep=sep, delimiter=delimiter, dtype=dtype, na_values=na_values, skiprows=skiprows, quotechar=quotechar, escapechar=escapechar, encoding=encoding, parallel=parallel, date_format=date_format, timestamp_format=timestamp_format, sample_size=sample_size, all_varchar=all_varchar, normalize_names=normalize_names, null_padding=null_padding, names=names, lineterminator=lineterminator, columns=columns, auto_type_candidates=auto_type_candidates, max_line_size=max_line_size, ignore_errors=ignore_errors, store_rejects=store_rejects, rejects_table=rejects_table, rejects_scan=rejects_scan, rejects_limit=rejects_limit, force_not_null=force_not_null, buffer_size=buffer_size, decimal=decimal, allow_quoted_nulls=allow_quoted_nulls, filename=filename, hive_partitioning=hive_partitioning, union_by_name=union_by_name, hive_types=hive_types)
    def from_csv_auto(self, path_or_buffer: Union[str, StringIO, TextIOBase], *, header: Optional[bool | int] = None, compression: Optional[str] = None, sep: Optional[str] = None, delimiter: Optional[str] = None, dtype: Optional[Dict[str, str] | List[str]] = None, na_values: Optional[str| List[str]] = None, skiprows: Optional[int] = None, quotechar: Optional[str] = None, escapechar: Optional[str] = None, encoding: Optional[str] = None, parallel: Optional[bool] = None, date_format: Optional[str] = None, timestamp_format: Optional[str] = None, sample_size: Optional[int] = None, all_varchar: Optional[bool] = None, normalize_names: Optional[bool] = None, null_padding: Optional[bool] = None, names: Optional[List[str]] = None, lineterminator: Optional[str] = None, columns: Optional[Dict[str, str]] = None, auto_type_candidates: Optional[List[str]] = None, max_line_size: Optional[int] = None, ignore_errors: Optional[bool] = None, store_rejects: Optional[bool] = None, rejects_table: Optional[str] = None, rejects_scan: Optional[str] = None, rejects_limit: Optional[int] = None, force_not_null: Optional[List[str]] = None, buffer_size: Optional[int] = None, decimal: Optional[str] = None, allow_quoted_nulls: Optional[bool] = None, filename: Optional[bool | str] = None, hive_partitioning: Optional[bool] = None, union_by_name: Optional[bool] = None, hive_types: Optional[Dict[str, str]] = None, hive_types_autocast: Optional[bool] = None) -> DuckDBPyRelation: 
        """
        Read a CSV file and return the result as a DuckDBPyRelation object.
        """
        return self._conn.from_csv_auto(path_or_buffer, header=header, compression=compression, sep=sep, delimiter=delimiter, dtype=dtype, na_values=na_values, skiprows=skiprows, quotechar=quotechar, escapechar=escapechar, encoding=encoding, parallel=parallel, date_format=date_format, timestamp_format=timestamp_format, sample_size=sample_size, all_varchar=all_varchar, normalize_names=normalize_names, null_padding=null_padding, names=names, lineterminator=lineterminator, columns=columns, auto_type_candidates=auto_type_candidates, max_line_size=max_line_size, ignore_errors=ignore_errors, store_rejects=store_rejects, rejects_table=rejects_table, rejects_scan=rejects_scan, rejects_limit=rejects_limit, force_not_null=force_not_null, buffer_size=buffer_size, decimal=decimal, allow_quoted_nulls=allow_quoted_nulls, filename=filename, hive_partitioning=hive_partitioning)
    def from_df(self, df: pandas.DataFrame) -> DuckDBPyRelation: 
        """
        Convert a Pandas DataFrame to a DuckDBPyRelation object.
        """
        return self._conn.from_df(df)
    def from_arrow(self, arrow_object: object) -> DuckDBPyRelation: 
        """
        Convert a PyArrow object to a DuckDBPyRelation object.
        """
        return self._conn.from_arrow(arrow_object)
    
    def from_parquet(self, file_glob: str, binary_as_string: bool = False, *, file_row_number: bool = False, filename: bool = False, hive_partitioning: bool = False, union_by_name: bool = False, compression: Optional[str] = None) -> DuckDBPyRelation: 
        """
        Read a Parquet file and return the result as a DuckDBPyRelation object.
        """
        return self._conn.from_parquet(file_glob, binary_as_string=binary_as_string, file_row_number=file_row_number, filename=filename, hive_partitioning=hive_partitioning, union_by_name=union_by_name, compression=compression)
    
    def read_parquet(self, file_glob: str, binary_as_string: bool = False, *, file_row_number: bool = False, filename: bool = False, hive_partitioning: bool = False, union_by_name: bool = False, compression: Optional[str] = None) -> DuckDBPyRelation: 
        """
        Read a Parquet file and return the result as a DuckDBPyRelation object.
        """
        return self._conn.read_parquet(file_glob, binary_as_string=binary_as_string, file_row_number=file_row_number, filename=filename, hive_partitioning=hive_partitioning, union_by_name=union_by_name, compression=compression)
    
    def get_table_names(self, query: str) -> Set[str]: 
        """
        Get the names of all tables in the database.
        """
        return self._conn.get_table_names(query)
    
    
    def install_extension(self, extension: str, *, force_install: bool = False, repository: Optional[str] = None, repository_url: Optional[str] = None, version: Optional[str] = None) -> None: 
        """
        Install a DuckDB extension.
        """
        self._conn.install_extension(extension, force_install=force_install, repository=repository, repository_url=repository_url, version=version)
    def load_extension(self, extension: str) -> None: 
        """
        Load a DuckDB extension.
        """
        self._conn.load_extension(extension)
      
      
      
      
      
      
      
    def load_catalog(self,name: Optional[str] = None, **properties: Optional[str] ) -> Catalog:
        catalog = pyiceberg_load_catalog(
            name,
            **properties
        
        )
        self._catalog = catalog 
        return catalog
    
    def to_iceberg(self, table_name: str, catalog: Catalog=None) -> bool:
        """
        Convert the DuckDBPyRelationIceberg object to an Iceberg table.
        """
        # Convert the relation to a Pandas DataFrame
        df = self._conn.fetch_df()
        
        # Convert the DataFrame to an Arrow Table
        arrow_table = pa.Table.from_pandas(df)
        if catalog is None:
            catalog = self._catalog
        # Create the Iceberg table
        
        database= table_name.split(".")[0] 
        if catalog._namespace_exists(database):
            print("Namespace already exists")
        else:
            print("Creating namespace")
            # catalog.create_namespace("docs_example")
            catalog.create_namespace(database)
            
        return create_iceberg_table(catalog, table_name, arrow_table)
    
    def upsert_iceberg(self, table_name: str, catalog: Catalog=None) -> bool:
        """
        Upsert the DuckDBPyRelationIceberg object to an Iceberg table.
        """
        # Convert the relation to a Pandas DataFrame
        df = self._conn.fetch_df()
        
        # Convert the DataFrame to an Arrow Table
        arrow_table = pa.Table.from_pandas(df)
        if catalog is None:
            catalog = self._catalog
            
        # Upsert the Iceberg table
        return upsert_iceberg_table(catalog, table_name, arrow_table)

    

def connect(
    database: str =  ":memory:", 
    read_only: bool = False, 
    config: dict = {}) -> DuckDBPyConnectionIceberg:
    """
    Connect to a DuckDB database and return a DuckDBPyConnectionIceberg object.
    """
    # Create a connection to the DuckDB database 
    conn = DuckDBPyConnectionIceberg(database=database, read_only=read_only, config=config)
    
    return conn