# import pandas as pd
#
# # 读取Excel文件
# def read_excel_file(file_path):
#     df = pd.read_excel(file_path)
#     return df
#
# # 生成UPDATE语句
# def generate_update_statements(df):
#     update_statements = []
#     for _, row in df.iterrows():
#         update_statement = f"""
#         UPDATE fmiscust.customize_invoice_verification
#         SET TRANSFER_VOUCHER_ID = '{row['TRANSFER_VOUCHER_ID']}',
#             TRANSFER_BILL_ID = '{row['TRANSFER_BILL_ID']}',
#             TRANSFER_BILL_DATE = TO_DATE('{row['TRANSFER_BILL_DATE']}', 'yyyy-MM-dd'),
#             TRANSFER_VOUCHER_DATE = TO_DATE('{row['TRANSFER_VOUCHER_DATE']}', 'yyyy-MM-dd'),
#             TRANSFER_CLERK = '{row['TRANSFER_CLERK']}'
#         WHERE invoice_code = '{row['INVOICE_CODE']}' AND invoice_no = '{row['INVOICE_NO']}';
#         """
#         update_statements.append(update_statement)
#     return update_statements
#
# # 写入SQL文件
# def write_sql_file(file_path, update_statements):
#     with open(file_path, 'w', encoding='utf-8') as file:
#         for statement in update_statements:
#             file.write(statement + '\n')
#
# # 主函数
# def main():
#     input_excel_path = '/Users/liuzongchen/Downloads/更新台账数据1.xlsx.log.xlsx'  # 输入Excel文件路径
#     output_sql_path = 'output4Update.sql'  # 输出SQL文件路径
#
#     # 读取Excel文件
#     df = read_excel_file(input_excel_path)
#
#     # 生成UPDATE语句
#     update_statements = generate_update_statements(df)
#
#     # 写入SQL文件
#     write_sql_file(output_sql_path, update_statements)
#
#     print(f"UPDATE语句已成功生成并保存在 {output_sql_path}")
#
# if __name__ == "__main__":
#     main()


import pandas as pd

# 读取Excel文件，并指定某些列的数据类型
def read_excel_file(file_path):
    dtype = {
        'TRANSFER_VOUCHER_ID': str,
        'INVOICE_CODE': str,
        'INVOICE_NO': str,
        'TRANSFER_BILL_ID': str,
        'TRANSFER_CLERK': str
    }
    df = pd.read_excel(file_path, dtype=dtype)
    return df

# 生成UPDATE语句
def generate_update_statements(df):
    update_statements = []
    for _, row in df.iterrows():
        update_statement = f"""
        UPDATE fmiscust.customize_invoice_verification
        SET TRANSFER_VOUCHER_ID = {int(row['TRANSFER_VOUCHER_ID']) if pd.notna(row['TRANSFER_VOUCHER_ID']) else 'NULL'},
            TRANSFER_BILL_ID = '{row['TRANSFER_BILL_ID']}',
            TRANSFER_BILL_DATE = TO_DATE('{row['TRANSFER_BILL_DATE']}', 'yyyy-MM-dd'),
            TRANSFER_VOUCHER_DATE = TO_DATE('{row['TRANSFER_VOUCHER_DATE']}', 'yyyy-MM-dd'),
            TRANSFER_CLERK = '{row['TRANSFER_CLERK']}'
        WHERE invoice_code = '{row['INVOICE_CODE']}' AND invoice_no = '{row['INVOICE_NO']}';
        """
        update_statements.append(update_statement)
    return update_statements

# 写入SQL文件
def write_sql_file(file_path, update_statements):
    with open(file_path, 'w', encoding='utf-8') as file:
        for statement in update_statements:
            file.write(statement + '\n')

# 主函数
def main():
    input_excel_path = '/Users/liuzongchen/Downloads/更新台账数据1.xlsx.log.xlsx'  # 输入Excel文件路径
    output_sql_path = 'output.sql'  # 输出SQL文件路径

    # 读取Excel文件
    df = read_excel_file(input_excel_path)

    # 生成UPDATE语句
    update_statements = generate_update_statements(df)

    # 写入SQL文件
    write_sql_file(output_sql_path, update_statements)

    print(f"UPDATE语句已成功生成并保存在 {output_sql_path}")

if __name__ == "__main__":
    main()
