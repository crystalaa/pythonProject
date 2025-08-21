# db_handler.py
import sqlite3
import pandas as pd
import re
import os
from data_handler import read_excel_fast

# 数据库文件路径
DB_FILE = 'excel_compare.db'


# =========================================================
# 基础初始化
# =========================================================
def init_database():
    """创建数据库、删旧表"""
    try:
        # 删除旧数据库文件（如果存在）
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)

        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # 删除旧表（如果存在）
        cursor.execute("DROP TABLE IF EXISTS temp_table1")
        cursor.execute("DROP TABLE IF EXISTS temp_table2")
        cursor.execute("DROP TABLE IF EXISTS temp_mapping_table")

        conn.close()
        return True
    except Exception as e:
        print(f"数据库初始化失败: {str(e)}")
        return False


def sanitize_column_name(col_name):
    """把任意列名变成合法 SQLite 列名"""
    clean = re.sub(r'[^\w]', '_', str(col_name))
    if clean and clean[0].isdigit():
        clean = 'col_' + clean
    return clean[:64] or 'unnamed_column'


# =========================================================
# 表与数据导入
# =========================================================
def import_excel_to_db(file_path, sheet_name, table_name, is_file1=True, skip_rows=0, chunk_size=5000):
    """把 Excel 分块写入 SQLite"""
    try:
        conn = sqlite3.connect(DB_FILE)

        df = read_excel_fast(file_path, sheet_name, is_file1=is_file1,
                             skip_rows=skip_rows, chunk_size=chunk_size)

        if df.empty:
            conn.close()
            return 0

        df.columns = [sanitize_column_name(c) for c in df.columns]

        # 建表
        create_sql = _generate_create_table_sql(df, table_name)
        conn.execute(create_sql)

        # 分块插入
        total_rows = len(df)
        for start in range(0, total_rows, chunk_size):
            chunk = df.iloc[start:start + chunk_size]
            _insert_data(conn, table_name, chunk)
            conn.commit()

        conn.close()
        return total_rows
    except Exception as e:
        raise Exception(f"导入Excel到数据库失败: {str(e)}")


def prepare_asset_category_mapping(rules, rule_file):
    """
    预先准备资产分类映射表数据
    """
    # 检查是否有资产分类字段需要对比
    has_asset_category = any(field_name == "资产分类" for field_name in rules.keys())
    if not has_asset_category:
        return False
    try:
        conn = sqlite3.connect(DB_FILE)

        # 加载资产分类映射表
        mapping_df = _load_asset_category_mapping(rule_file)
        if mapping_df.empty or '同源目录完整名称' not in mapping_df.columns or '同源目录编码' not in mapping_df.columns:
            return False

        # 创建临时映射表
        create_mapping_table_sql = """
        CREATE TABLE temp_mapping_table (
            同源目录完整名称 TEXT,
            同源目录编码 TEXT
        )
        """
        conn.execute(create_mapping_table_sql)

        # 批量插入映射数据
        if not mapping_df.empty:
            # 准备批量插入数据
            insert_data = []
            for _, row in mapping_df.iterrows():
                try:
                    insert_data.append((str(row['同源目录完整名称']), str(row['同源目录编码'])))
                except:
                    continue

            if insert_data:
                insert_sql = """
                INSERT INTO temp_mapping_table (同源目录完整名称, 同源目录编码)
                VALUES (?, ?)
                """
                # 分批插入，避免数据量过大
                batch_size = 1000
                for i in range(0, len(insert_data), batch_size):
                    batch = insert_data[i:i + batch_size]
                    conn.executemany(insert_sql, batch)
                    conn.commit()
        conn.close()
        return True
    except Exception as e:
        raise Exception(f"准备资产分类映射表时出错: {str(e)}")


def _load_asset_category_mapping(rule_file):
    """
    从规则文件中加载资产分类映射表
    """
    try:
        # 读取规则文件中的"资产分类映射表"页签，跳过第一行
        mapping_df = pd.read_excel(rule_file, sheet_name='资产分类映射表', skiprows=1)
        return mapping_df
    except Exception as e:
        raise Exception(f"读取资产分类映射表失败: {str(e)}")


def _generate_create_table_sql(df, table_name):
    cols = [f"`{col}` TEXT" for col in df.columns]
    sql = f"""
    CREATE TABLE `{table_name}` (
        `id` INTEGER PRIMARY KEY AUTOINCREMENT,
        {', '.join(cols)}
    )
    """
    return sql


def _insert_data(conn, table_name, df):
    if df.empty:
        return
    cols = [f"`{c}`" for c in df.columns]
    placeholders = ",".join(["?"] * len(df.columns))
    sql = f"INSERT INTO `{table_name}` ({','.join(cols)}) VALUES ({placeholders})"

    # 判断是否为表二
    is_table2 = table_name == 'temp_table2'

    processed_data = []
    for _, row in df.iterrows():
        processed_row = []
        for i, col_name in enumerate(df.columns):
            value = row[col_name]
            # 如果是表二且字段名包含"折旧"，则取绝对值
            if is_table2 and "折旧" in col_name:
                try:
                    # 尝试将值转换为数值并取绝对值
                    if pd.notna(value):
                        numeric_value = float(value)
                        processed_row.append(str(abs(numeric_value)))
                    else:
                        processed_row.append(None)
                except (ValueError, TypeError):
                    # 如果转换失败，保持原始值
                    processed_row.append(str(value) if pd.notna(value) else None)
            else:
                processed_row.append(str(value) if pd.notna(value) else None)
        processed_data.append(tuple(processed_row))

    conn.executemany(sql, processed_data)


# =========================================================
# 通用查询
# =========================================================
def execute_query(query, params=None, executemany=False):
    """执行 SQL 并返回 DataFrame"""
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.row_factory = sqlite3.Row  # 使结果可以通过列名访问
        conn.autocommit = True
        if params:
            if executemany:
                conn.executemany(query, params)
            else:
                cursor = conn.execute(query, params)
        else:
            cursor = conn.execute(query)

        # 获取列名
        columns = [description[0] for description in cursor.description] if cursor.description else []

        # 获取数据
        rows = cursor.fetchall()

        # 转换为 DataFrame
        df = pd.DataFrame(rows, columns=columns)
        conn.close()
        return df
    except Exception as e:
        raise Exception(f"执行查询失败: {str(e)}")


# =========================================================
# 主键相关工具
# =========================================================
def create_compare_index(table: str, pk_cols: list):
    """给 _pk_concat 建索引"""
    idx_name = f"idx_{table}_pk"
    col_str = ",".join([f"`{c}`" for c in pk_cols])
    sql = f"CREATE INDEX IF NOT EXISTS {idx_name} ON `{table}` ({col_str})"
    try:
        execute_query(sql)
    except Exception:
        pass  # 已存在


def add_concat_pk_column(table: str, expr: str):
    """给表增加 _pk_concat 列并填充"""
    try:
        execute_query(f"ALTER TABLE `{table}` ADD COLUMN `_pk_concat` TEXT")
    except Exception:
        pass  # 列已存在
    execute_query(f"UPDATE `{table}` SET `_pk_concat` = {expr}")


def fetch_rows_by_pk(table: str, pk_cols: list, wanted_keys: set):
    """根据 _pk_concat 拉取行"""
    if not wanted_keys:
        return pd.DataFrame()
    keys = list(wanted_keys)
    placeholders = ",".join(["?"] * len(keys))
    sql = f"SELECT * FROM `{table}` WHERE _pk_concat IN ({placeholders})"
    return execute_query(sql, params=keys)


# =========================================================
# 清理
# =========================================================
def drop_tables():
    try:
        conn = sqlite3.connect(DB_FILE)
        conn.execute("DROP TABLE IF EXISTS temp_table1")
        conn.execute("DROP TABLE IF EXISTS temp_table2")
        conn.execute("DROP TABLE IF EXISTS temp_mapping_table")
        conn.commit()
        conn.close()

        # 删除数据库文件
        if os.path.exists(DB_FILE):
            os.remove(DB_FILE)
    except Exception as e:
        print(f"删除表失败: {str(e)}")
