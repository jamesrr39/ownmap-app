package parquetqueryengine

/*
package parquetqueryengine is a pure-go implementation of a SQL-style query engine for querying parquet files.

Fill in the `*Query` object, and call `Run`.

Internally, it works like this:

- Parquet files are a collection of "row groups", which contain a number of rows. How many rows per "row group" depends on RowGroupSize setting when you write your parquet file. If you have repeated fields (i.e. a list) as part of your schema, there can be more, less or the same amount of value in that column than in other columns.
- 

*/
