#!/usr/bin/python3


import argparse
import os
import csv
import sqlite3
import sys
from typing import List


# 输入 cvs 文件路径 导入sqlite数据库中
# 支持 csv单独导入，或者文件夹批量导入
# 命令行参数 1. csv文件路径/ csv文件夹路径 2. 数据库文件路径默认 ./default_database.db 3. 数据库表名 4.是否覆盖数据库文件  5. 是否覆盖数据库表


class CsvToSqlite:
    conn = None
    db_file_path = ""

    def __init__(self,  db_file_path="default_database.db"):
        # self.csv_file_path = csv_file_path
        # csv_file_path,
        if not db_file_path:
            db_file_path = 'default_database.db'

        self.db_file_path = db_file_path
        self.conn = self.connect_db(db_file_path)

        # conn = sqlite3.connect(db_file_path)
        # self.conn = conn
    def connect_db(self, db_file_path: str) -> sqlite3.Connection:
        if not os.path.exists(db_file_path):
            print(f"Database file {db_file_path} does not exist. Creating a new one.")
        return sqlite3.connect(db_file_path)

    def close_db(self):
        if self.conn:
            self.conn.close()

    def reconnect_db(self, db_file_path: str):
        self.db_file_path = db_file_path
        self.conn = sqlite3.connect(self.db_file_path)

    # def close_db(self):
    #     self.conn.close()

    def create_table_old(self, table_name: str, column_list: List[str], field_types: List[str]):
        # # 创建表
        # 检查表是否存在
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if cursor.fetchone() is None:
            # 表不存在，创建新表
            #  c.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT)")
            sql = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT)"
            cursor.execute(sql)

            print(f" 创建表 {table_name} {sql}  ")
            sql0 = f"CREATE TABLE IF NOT EXISTS  {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT , {','.join(column_list)})"
            print(f" 创建表 {table_name} {sql0}  ")

            # 添加列名到表定义
            for i in range(len(column_list)):
                column_name = column_list[i]
                field_type = field_types[i]
                sql = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {field_type}"
            # for column_name in column_list:
            #     sql=f"ALTER TABLE {table_name} ADD COLUMN {column_name} TEXT"
                print(f" 添加列 {column_name} , {sql}  ")
                cursor.execute(sql)
            self.conn.commit()
        else:
            print(f" 表 {table_name} 已存在")

    def create_table(self, table_name: str, column_list: List[str], field_types: List[str]):
        # # 创建表
        cursor = self.conn.cursor()
        # 检查表是否存在
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if cursor.fetchone() is None:
            # 表不存在，创建新表
            columns = ", ".join([f"{col} {ftype}" for col, ftype in zip(column_list, field_types)])
            sql = f"CREATE TABLE {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, {columns})"
            cursor.execute(sql)
            print(f"Created table {table_name} with SQL: {sql}")
        self.conn.commit()

    def parse_csv(self, csv_file_path: str):
        # 解析csv 获取列名 和数据

        #    FIELD_TYPES = {
        # 'ID': 'INTEGER',
        # 'NAME': 'TEXT',
        # 'AGE': 'INTEGER',
        # 'SALARY': 'FLOAT',
        # 'HIRE_DATE': 'DATE',
        # 'IS_ACTIVE': 'BOOLEAN'
        # }
        if not os.path.exists(csv_file_path):
            print(f" 文件 {csv_file_path} 不存在")
            raise Exception(f" 文件 {csv_file_path} 不存在")
        with open(csv_file_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            column_names = reader.fieldnames
            column_data = [row for row in reader]
            field_types = []
            new_column_data = []
            row = column_data[0]

            for column_name in column_names:
                # print(f" 列名 {column_name} 数据 {row.get(column_name, '')}  ")
                val = row.get(column_name, '')
                if val.isdigit():
                    val = int(val)
                    field_types.append('INTEGER')
                elif val.replace('.', '', 1).isdigit():
                    val = float(val)
                    field_types.append('FLOAT')
                elif val.lower() in ['true', 'false']:
                    val = bool(val)
                    field_types.append('BOOLEAN')
                # elif val.replace('-','',1).isdigit():
                #     val=datetime.strptime(val, '%Y-%m-%d')
                #     field_types.append('DATE')
                else:
                    field_types.append('TEXT')
                # new_column_data.append(val)

            # 遍历CSV数据， 部分str字段 转换为其他类型
            for j in range(len(column_data)):
                row = column_data[j]
            # for row in column_data:
                new_row = {}
                for i in range(len(column_names)):
                    column_name = column_names[i]
                    field_type = field_types[i]
                    val = row.get(column_name, '')
                    if field_type == 'INTEGER':
                        if val.isdigit():
                            val = int(val)
                        else:
                            val = 0
                    elif field_type == 'FLOAT':
                        if val.replace('.', '', 1).isdigit():
                            val = float(val)
                        else:
                            val = 0.0
                    # elif field_type == 'DATE':
                    #     val = datetime.strptime(val, '%Y-%m-%d')
                    #     val = val.strftime('%Y-%m-%d')
                    elif field_type == 'BOOLEAN':
                        if val.lower() in ['true', 'false']:
                            val = bool(val)
                        else:
                            val = False
                    new_row[column_name] = val
                column_data[j] = new_row

            return column_names, column_data, field_types

    def import_csv_to_sqlite(self, csv_file, table_name):

        if not table_name:
            print("未指定表名，将使用文件名称作为表名")
            table_name = os.path.basename(table_name)
        # if db_file:
        #     self.reconnect_db(db_file)
        conn = self.conn
        cursor = conn.cursor()

        # 解析csv 获取列名 和数据
        column_names, column_data, field_types = self.parse_csv(csv_file)

        # 创建表
        self.create_table(table_name, column_names, field_types)

        #  将CSV数据插入到表中
        cursor = conn.cursor()

        # 遍历CSV数据，逐行插入到数据库中
        for row in column_data:
            # 构建插入语句
            values = [row.get(column_name, '') for column_name in column_names]
            sql = f"INSERT INTO {table_name}   ( {','.join(column_names)} )   VALUES ({','.join(['?']*len(values))})"
            print(f" 插入数据 sql {sql}  ")
            ret = cursor.execute(sql, values)
            print(f" 插入数据 {ret}  ")

        conn.commit()
        conn.close()

    def import_folder_to_sqlite(self, folder_path, table_name, db_file=""):

        csv_file_list = []
        for filename in os.listdir(folder_path):
            if filename.endswith('.csv'):
                csv_file = os.path.join(folder_path, filename)
                csv_file_list.append(csv_file)
        if not table_name:
            print("未指定表名，将使用文件夹名称作为表名")
            table_name = os.path.basename(folder_path)
        if not csv_file_list:
            print("未找到csv文件")
            return
        if db_file:
            self.reconnect_db(db_file)
        conn = self.conn
        cursor = conn.cursor()
        for i in range(len(csv_file_list)):
            csv_file = csv_file_list[i]

            # 解析csv 获取列名 和数据
            column_names, column_data, field_types = self.parse_csv(csv_file)

            # 创建表
            self.create_table(table_name, column_names, field_types)

            #  将CSV数据插入到表中
            cursor = conn.cursor()

            # 遍历CSV数据，逐行插入到数据库中
            for row in column_data:
                # 构建插入语句
                values = [row.get(column_name, '') for column_name in column_names]
                sql = f"INSERT INTO {table_name}   ( {','.join(column_names)} )   VALUES ({','.join(['?']*len(values))})"
                cursor.execute(sql, values)

            conn.commit()

            # 估算进度 百分比
            progress = (i + 1) / len(csv_file_list)
            print(f"进度：{progress * 100:.2f}%")

        conn.close()


def fun_import_csv_to_sqlite(csv_file_path: str, table_name: str = "", db_file_path=""):

    if os.path.exists(csv_file_path):

        if os.path.isdir(csv_file_path):
            # 导入文件夹
            tool = CsvToSqlite(db_file_path)
            tool.import_folder_to_sqlite(csv_file_path, table_name)
        else:
            # 导入单个文件
            tool = CsvToSqlite(db_file_path)
            tool.import_csv_to_sqlite(csv_file_path, table_name)


def parse_command_line():

    # 实现命令行参数解析

    parser = argparse.ArgumentParser(description="CSV to SQLite")
    parser.add_argument("--csv_file", help="Path to the CSV file or folder")
    parser.add_argument("--table_name", default="", help="Name of the table to create")
    parser.add_argument("--db_file", default="", help="Path to the SQLite database file")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
        return
    return parser.parse_args()


# # 测试命令行参数解析
if __name__ == "__main__":
    options = parse_command_line()
    print(options)

    # 测试CSV导入
    fun_import_csv_to_sqlite(options.csv_file, options.table_name, options.db_file)
