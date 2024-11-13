# data_app
 Project template. Make queries, clean data, save the tables and create plots. It follows the ETL classic flow as the modules indicates

 # Connections
 * **Google:** Contents GoogleEnv object and Google Drive object. In case you want to manage sheets and Drive, you can use it with the same object. GoogleDrive object works with Drive's API so it can be managed for:
   * Create folder
   * List folders
   * Delete files
   * Download files
   * Upload files

 * **Spark:** Loads the files needed to Spark's session. Pending to make easier Spark's setting update with `.env` file.
 * **SQL:** Connect to Postgres using `Psycopg2`, where you will only need to call the `query` funciton. Pending to add `MySQL` connetion using `mysql` pip lib for better fit. Here you will have `query` function, which manage the connection and cursor, so you can query on a fastest and easy way. Remember that it only connects to the DB setted on you `.env` file.

# Extract
It refers, in general terms, to api calls for structured procedures or frequently used queries, at a level that you can stored on a query file. The types of apis can be found are:

1. **API**:
   1. Google Geocoding.
   2. Google Sheets
   3. Airtable 
   * Pending:
     1. WIX Site:
         * Authentication Token.
         * Account ID
         * Site ID

All permisions must be stored on a .env file according to the type. If the file stores the api keys, so the file must be `rest_api.env`, it there is a PostgreSQL or MySQL, then `sql.env`, etc.

## queries
Here goes all the query made by an ORM or a `.sql` files, where a query has to be done due to its complexity or permissions. Remember that ORM queries must be functions. Here is shown a way to acces to `.sql`

```python
from extract.sql import get_queries

queries = get_queries()
```

`get_queries` function will return a dictionary where `keys` are `.sql` file name and stores the queries inside the file.

# transform
Here the transformations required for a job are stored as a python script. In order to this works right, every script must have a `clean` function (that works as `main` but helps with the sintaxis when its called), where it must receive only the **dirty data** and returns the data in the structured needed. For example:

```python
from extract import sql
from transform import sells_report

dirty_data = sql.get(query)

sells_data = sells_report.clean(dirty_data)

```

Another way to manage it, is with the storage of the processes made on a script and call them by calling this module.

```python
from extract import sql
from transform import reports 

dirty_data = sql.get(query)

sells_data = reports.sells.clean(dirty_data)

```

# load
Loads and saves the files in the way the you need:
* Excel
* CSV
* Parquet
* DB Table

For **postgresql** there's a special function designed to optimize loads greater that + 1.5 M in +/- 1.5 min (not joing on this ratio).

Supossing that you want to load all the tables needed on a created schema (can be clean), you just have to sort your dataframes and use the next lines:

```python
from load.to_sql import insert_many_from

for df in sorted_dfs:
    insert_many_from(table = df)

```

For **UPSERT** scenarios, a UpsertConstraint class is designed, so you will only have to specify the reference columns and the columns to be updated, like this:


```python
from connections.sql import UpsertConstraint
from load.to_sql import insert_many_from

# Example 1
upsert_case_1 = UpsertConstriant(
  ref_col = "id",
  update_cols = tuple(
    ['price']
  )
)

# Example 2
upsert_case_2 = UpsertConstriant(
  ref_col = tuple(
    ['name', 'date']
  ),
  update_cols = tuple(
    ['dept', 'days', 'last_record']
  )
)

for df in sorted_dfs:
    insert_many_from(table = df, upsert = upsert_case_1)
```

# Reporter (TO DO)
Creates the report on the tool needed.
* Excel using openpyxl
* Plotly using graph objects

Minimum requires to set Palette. For every tool used, first you must call the Plot Object, setting from the start the type of it by the object used. If you decide, you could pass a list of the graphs that you need, so they will be generated and called instead everytime you need it. Preseted values are `plotly` and `excel`, so you can check the plot on a jupyter notebook before you send it to an excel file, by using the common .show().

```python
from reporter.plots import Pie, Bar, Line

month_sells = Line(data, layout, types = ['plotly', 'excel'])
group_sells = Pie(data, layout)
quarter_sells = Bar(data, layout)

motnh_sells.show()
```
