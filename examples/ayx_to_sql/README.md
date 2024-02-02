## ayx_to_sql

ayx_to_sql is a small command-line application that loads YXDB files to SQL Server. The application is fully functional and showcases how to do complete file transfers using yxdb-go.

Run the application using the following arguments:

`ayx_to_sql -table={SQL_TABLE_NAME} -yxdb={PATH_TO_YXDB} -createTable=true|false`

- `-table`: The name of the SQL table.
- `-yxdb`: The path to the YXDB file. The path can be enclosed in quotes if it contains a space.
- `-createTable`: An optional flag. If true, the application creates a table in SQL Server using the YXDB's metadata. If false, the application will skip this step and upload the data to SQL Server, assuming the specified table already exists and matches the YXDB fields.

A well-formed connection string to the SQL Server instance must be present in the SQL_CONN_STR environment variable.
