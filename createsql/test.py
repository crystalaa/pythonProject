# import pandas as pd
#
#
# def pad_zeros_and_write_excel(input_file, output_file):
#     """
#     读取Excel文件，对Sheet1中B列的数字字符串进行前导补零至10位，
#     然后将结果写入新的Excel文件。
#
#     参数:
#     input_file (str): 输入Excel文件的路径。
#     output_file (str): 输出处理后Excel文件的路径。
#     """
#     # 读取Excel文件
#     df = pd.read_excel(input_file)
#
#     # 对B列数据进行前导补零操作
#     df['客户'] = df['客户'].apply(
#         lambda x: str(int(x)).zfill(10) if isinstance(x, (int, float)) and str(x).isdigit() else x)
#     # 写入新的Excel文件
#     df.to_excel(output_file, index=False)
#     print('success1111')
#
# # 使用示例
# input_excel_path = '/Users/liuzongchen/Desktop/export客户主数据(已自动还原).xlsx'  # 请替换为你的输入文件路径
# output_excel_path = '/Users/liuzongchen/Desktop/your_output_excel.xlsx'  # 请替换为你希望输出的文件路径
#
# pad_zeros_and_write_excel(input_excel_path, output_excel_path)




# import pandas as pd
#
# # 定义读取和写入的Excel文件路径
# excel_file_path = '/Users/liuzongchen/Desktop/11111.xlsx'  # 请替换为您的Excel文件路径
#
# # 读取Excel文件中的两个工作表
# df_sheet1 = pd.read_excel(excel_file_path, sheet_name='Sheet1')
# df_sheet2 = pd.read_excel(excel_file_path, sheet_name='Sheet2')
#
# # 数据预处理
# # 确保"客户"列和"SAPID"列都是字符串类型，并且不足10位的前导补零
# df_sheet1['客户'] = df_sheet1['客户'].astype(str).apply(lambda x: x.zfill(10) if len(x) < 10 else x)
# df_sheet2['SAPID'] = df_sheet2['SAPID'].astype(str).apply(lambda x: x.zfill(10) if len(x) < 10 else x)
#
# # 检查并处理可能的空值或缺失值
# df_sheet1.dropna(subset=['客户', 'NAME1'], inplace=True)
# df_sheet2.dropna(subset=['SAPID', 'DXMC'], inplace=True)
#
# # 打印数据概况，便于检查
# print("Sheet1预处理后数据形状:", df_sheet1.shape)
# print("Sheet2预处理后数据形状:", df_sheet2.shape)
#
# # 直接进行数据匹配
# # 注意：这里假设"客户"对应"SAPID"，"NAME1"对应"DXMC"，请根据实际情况调整
# merged_df = pd.merge(df_sheet1[['客户', 'NAME1']],
#                      df_sheet2[['SAPID', 'DXMC', 'DXID']],
#                      left_on=['客户', 'NAME1'],
#                      right_on=['SAPID', 'DXMC'],
#                      how='left')
#
# # 检查匹配结果
# if merged_df.empty:
#     print("匹配未找到任何记录，请检查数据是否符合预期格式或存在其他问题。")
# else:
#     # 如果有匹配记录，则填充DXID
#     df_sheet1['DXID'] = merged_df['DXID']
#     # 删除临时使用的列
#     del merged_df
#
#     # 将更新后的Sheet1数据写回到Excel文件
#     with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
#         df_sheet1.to_excel(writer, sheet_name='Sheet1', index=False)
#         df_sheet2.to_excel(writer, sheet_name='Sheet2', index=False)  # 重新写入Sheet2，以防万一
#
#     print("DXID填充完成！")



import pandas as pd

# 定义读取和写入的Excel文件路径
excel_file_path = '/Users/liuzongchen/Downloads/5069.xlsx'  # 请替换为您的Excel文件路径

# 读取Excel文件中的两个工作表
df_sheet1 = pd.read_excel(excel_file_path, sheet_name='Sheet1')
df_sheet2 = pd.read_excel(excel_file_path, sheet_name='Sheet2')

# 假设DXID列在Sheet1中，管控管理对象ID列在Sheet2中，我们想要找出Sheet1中DXID不在Sheet2相应列中的数据
# 首先确保比较的列数据类型一致，如果需要转换类型，请在这里进行

# 使用isin方法找出Sheet1中不在Sheet2中的DXID
not_in_sheet2 = ~df_sheet1['DXID'].isin(df_sheet2['管控管理对象ID'])
starts_with_1 = df_sheet1['SAPID'].str.startswith('1')
# 根据条件筛选出Sheet1中的数据
df_sheet3_data = df_sheet1[not_in_sheet2 & starts_with_1]
# 将筛选出的数据写入到Excel的新工作表Sheet3中
with pd.ExcelWriter(excel_file_path, engine='openpyxl', mode='a') as writer:
    # 检查Sheet3是否存在，如果不存在则创建
    if 'Sheet3' not in writer.book.sheetnames:
        df_sheet3_data.to_excel(writer, sheet_name='Sheet3', index=False)
    else:
        # 如果存在，则追加或更新数据（这里直接覆盖原有Sheet3）
        writer.book.remove(writer.book['Sheet3'])
        df_sheet3_data.to_excel(writer, sheet_name='Sheet3', index=False)

print("不在Sheet2中的数据已成功写入Sheet3。")