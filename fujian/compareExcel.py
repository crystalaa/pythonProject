import pandas as pd


def find_combinations(file1, sheet1, col_a, col_b, file2, sheet2, col_c, col_d):
    # 加载第一个Excel文件
    df1 = pd.read_excel(file1, sheet_name=sheet1)
    # 创建一个新列，该列是两个指定列的组合
    df1['combined'] = df1[col_a].astype(str) + df1[col_b].astype(str)

    # 加载第二个Excel文件
    df2 = pd.read_excel(file2, sheet_name=sheet2)
    # 创建一个新列，该列是两个指定列的组合
    df2['combined'] = df2[col_c].astype(str) + df2[col_d].astype(str)

    # 使用isin函数查找第一个DataFrame中的组合是否存在于第二个DataFrame中
    df1['found_in_df2'] = df1['combined'].isin(df2['combined'])

    # 打印结果
    print(df1[['combined', 'found_in_df2']])


# 示例用法
file1 = '/Users/liuzongchen/Downloads/xtitemsyw.xls'
sheet1 = 'Selectxtitemsyw'
col_a = 'CAPTION'  # 第一个Excel文件的第一列
col_b = 'ZJM'  # 第一个Excel文件的第二列

compid_filter = '1700'
caption_filter = '往来款项性质'

file2 = '/Users/liuzongchen/Downloads/往来款项性质20240621.xlsx'
sheet2 = '往来款项性质'
col_c = '标签全称'  # 第二个Excel文件的第一列
col_d = '标签编码'  # 第二个Excel文件的第二列

find_combinations(file1, sheet1, col_a, col_b, file2, sheet2, col_c, col_d)