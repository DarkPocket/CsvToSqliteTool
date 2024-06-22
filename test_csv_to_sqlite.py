#!/usr/bin/python3


from typing import List

from csv_to_sqlite import CsvToSqlite, fun_import_csv_to_sqlite


# 输入 cvs 文件路径 导入sqlite数据库中
#  支持 csv单独导入，或者文件夹批量导入
# 命令行参数 1. csv文件路径/ csv文件夹路径 2. 数据库文件路径默认 ./default_database.db 3. 数据库表名 4.是否覆盖数据库文件  5. 是否覆盖数据库表


def test_function():
    db_file_path = "./monitor_test.db"
    tool = CsvToSqlite(db_file_path)
    csv_file = "monitor_2023-05-26.csv"
    table_name = "monitor_2023"
    tool.import_csv_to_sqlite(csv_file, table_name)


def test_batch_import_function():

    csv_file = "./csv_data"
    table_name = "monitor_server_psutil"
    db_file_path = "./monitor_server.db"
    fun_import_csv_to_sqlite(csv_file, table_name, db_file_path)


if __name__ == "__main__":

    #     # test_function()
    test_batch_import_function()
