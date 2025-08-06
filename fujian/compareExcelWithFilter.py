import argparse
import pandas as pd
import os
import logging
import chardet


# 配置日志
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

def detect_encoding(file_path):
    """检测文件编码"""
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']


def read_file(file_path, sheet_name=None):
    """根据文件扩展名读取文件"""
    _, ext = os.path.splitext(file_path)
    encoding = detect_encoding(file_path)
    logging.debug(f"Detected encoding for {file_path}: {encoding}")

    if ext.lower() == '.csv':
        try:
            with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                return pd.read_csv(f).fillna('').astype(str)
        except UnicodeDecodeError:
            logging.warning(f"Failed to read {file_path} with detected encoding {encoding}. Trying gbk encoding.")
            with open(file_path, 'r', encoding='gbk', errors='replace') as f:
                return pd.read_csv(f).fillna('').astype(str)
    elif ext.lower() in ['.xls', '.xlsx']:
        return pd.read_excel(file_path, sheet_name=sheet_name).fillna('').astype(str)
    else:
        raise ValueError(f"Unsupported file format: {ext}")


def find_combinations_filtered(file1, sheet1, col_a, col_b, compid_filter, caption_filter, file2, sheet2, col_c, col_d):
    # 加载第一个Excel文件，并添加过滤条件
    # 从第一个Excel文件中读取指定表格，并预处理（转换为字符串并删除含空值的行）
    df1 = read_file(file1, sheet1)
    df1_filtered = df1[(df1['COMPID'] == compid_filter) & (df1['CAP1'] == caption_filter)]

    if not df1_filtered.empty:
        # 直接检查并处理ZJM为空的情况
        df1_filtered[col_b] = df1_filtered[col_b].where(df1_filtered[col_b].astype(str).fillna('').notnull(),
                                                        df1_filtered[
                                                            ['CODE1', 'CODE2', 'CODE3', 'CODE4', 'CODE5', 'CODE6',
                                                             'CODE7']]
                                                        .apply(lambda row: ''.join(row.dropna().astype(str)), axis=1))
        df1_filtered[col_b] = df1_filtered[col_b].str.strip()
        df1_filtered[col_b] = df1_filtered[col_b].str.replace(' ', '')
        df1_filtered['combined'] = df1_filtered[col_a] + df1_filtered[col_b]
        # 现在df1_filtered的'ZJM'列，如果原计算结果为空，则会被替换为code1到code7的非空拼接值
        print(df1_filtered[col_b])
        # 如果需要，可以在这里保存修改后的DataFrame回Excel
        df1_filtered.loc[:, 'combined'] = df1_filtered[col_a] + df1_filtered[col_b]
        # 从第二个Excel文件中读取指定表格，并预处理
        df2 = read_file(file2, sheet2)
        df2['combined'] = df2[col_c] + df2[col_d]
        # 找出未在df2中匹配到的行
        unmatched_rows = df1_filtered[~df1_filtered['combined'].isin(df2['combined'])]
        unmatched_count = len(unmatched_rows)
        if not unmatched_rows.empty:
            print(f"Data from Excel 1 not found in Excel 2. Unmatched count: {unmatched_count}")

            # 显示所有列
            pd.set_option('display.max_columns', None)

            # 显示所有行
            pd.set_option('display.max_rows', None)

            # 设置最大宽度，防止换行
            pd.set_option('max_colwidth', None)

            # 现在打印DataFrame
            # print(your_dataframe)
            print(unmatched_rows[['combined', col_a, col_b]])
            # print(unmatched_data)
        else:
            print("All data from Excel 1 was found in Excel 2.")
        # df1_filtered['found_in_df2'] = df1_filtered['combined'].isin(df2['combined'])
        #
        # print("Filtered and Processed Data:")
        # print(df1_filtered[['combined', 'found_in_df2']])
    else:
        print("No matching records found with the given filters.")

def main():
    parser = argparse.ArgumentParser(description="Compare Excel files based on filters.")
    parser.add_argument("file1", help="Path to the first Excel file")
    parser.add_argument("sheet1", help="Sheet name in the first Excel file")
    parser.add_argument("col_a", help="Column A in the first Excel file")
    parser.add_argument("col_b", help="Column B in the first Excel file")
    parser.add_argument("compid_filter", help="Compid filter value")
    parser.add_argument("caption_filter", help="Caption filter value")
    parser.add_argument("file2", help="Path to the second Excel file")
    parser.add_argument("sheet2", help="Sheet name in the second Excel file")
    parser.add_argument("col_c", help="Column C in the second Excel file")
    parser.add_argument("col_d", help="Column D in the second Excel file")

    args = parser.parse_args()

    find_combinations_filtered(args.file1, args.sheet1, args.col_a, args.col_b,
                               args.compid_filter, args.caption_filter,
                               args.file2, args.sheet2, args.col_c, args.col_d)


if __name__ == "__main__":
    main()



# compareExcelWithFilter.exe xtitemsyw.csv xtitemsyw CAPTION ZJM 9999 现金流量分类 现金流量分类20240628.xlsx 现金流量分类 标签全称 标签编码 > log.txt 2>&1