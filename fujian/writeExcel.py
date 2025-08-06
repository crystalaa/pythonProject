import pandas as pd


def set_column_value(file_path, sheet_name, column_to_set='管控管理对象类型', fixed_value='8254'):
    """
    设置Excel文件中指定列的所有值为固定值。

    :param file_path: Excel文件路径
    :param sheet_name: 工作表名称
    :param column_to_set: 要设置值的列（默认为'B'，根据Excel列标识）
    :param fixed_value: 要设置的固定值
    """
    # 读取Excel文件
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    # 设置指定列的值为固定值
    df[column_to_set] = fixed_value
    # 将B列的值设置为对应行G列的值
    df['财务中台内部ID'] = df['管控管理对象ID']
    df['填写人'] = '刘宗晨'
    df['财务中台名称'] = df['管控管理对象名称']
    df['所属网省COMPID'] = '1700'
    df['所属网省名称'] = '国网福建省电力有限公司'
    df['财务中台管理对象类型'] = '1047'
    # df['管控管理对象类型'] = '5069'

    # 将更改写回到Excel文件
    df.to_excel(file_path, sheet_name=sheet_name, index=False)
    print(f"Column {column_to_set} in '{sheet_name}' has been set to '{fixed_value}' successfully.")
# 使用示例
file_path = '/Users/liuzongchen/Desktop/维度映射/管理对象维度数据ID映射收集清单-采购订单3.xlsx'
sheet_name = '映射清单(实施填写)'
set_column_value(file_path, sheet_name)

# import argparse
# import pandas as pd
#
#
# def set_column_value_from_another(file_path, sheet_name, source_column, target_column):
#     """
#     根据命令行参数设置Excel文件中指定列的值。
#     """
#     # 读取Excel文件
#     df = pd.read_excel(file_path, sheet_name=sheet_name)
#
#     # 确保源列存在于DataFrame中
#     if source_column not in df.columns:
#         print(f"Column '{source_column}' not found in the DataFrame.")
#         return
#
#     # 设置目标列的值
#     df[target_column] = df[source_column]
#
#     # 写回Excel文件
#     df.to_excel(file_path, sheet_name=sheet_name, index=False)
#     print(
#         f"Column {target_column} in '{sheet_name}' has been set to the value of Column {source_column} for each row successfully.")
#
#
# def main():
#     parser = argparse.ArgumentParser(description="Set values in one column of an Excel sheet to match another column.")
#     parser.add_argument("file_path", help="Path to the Excel file")
#     parser.add_argument("sheet_name", help="Name of the worksheet within the Excel file")
#     parser.add_argument("source_column", help="Source column to copy values from")
#     parser.add_argument("target_column", help="Target column to set values to")
#     # parser.add_argument("fixed_value", help="column to set")
#
#     args = parser.parse_args()
#
#     set_column_value_from_another(args.file_path, args.sheet_name, args.source_column, args.target_column)
#
#
# if __name__ == "__main__":
#     main()
