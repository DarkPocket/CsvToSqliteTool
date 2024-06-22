# CsvToSqliteTool
csv文件数据导入sqlite3数据库工具




支持 csv单独导入，或者文件夹批量导入


```bash
python  .\csv_to_sqlite.py
usage: csv_to_sqlite.py [-h] [--csv_file CSV_FILE] [--table_name TABLE_NAME]
                        [--db_file DB_FILE]CSV to SQLiteoptional arguments:
  -h, --help            show this help message and exit
  --csv_file CSV_FILE   Path to the CSV file or folder
  --table_name TABLE_NAME
                        Name of the table to create
  --db_file DB_FILE     Path to the SQLite database file
```


eg: 批量文件转化sqlite db

```bash
python  .\csv_to_sqlite.py --csv_file=./csv_data  --db_file=demo_db.db  --table_name=csv_table
```