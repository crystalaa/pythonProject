import re

# 读取SQL文件
def read_sql_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        sql_content = file.read()
    return sql_content

# 替换TIMESTAMP格式
def replace_timestamp_with_to_date(sql_content):
    # 正则表达式匹配TIMESTAMP 'YYYY-MM-DD HH:MM:SS'
    pattern = r"TIMESTAMP '(\d{4}-\d{2}-\d{2}) \d{2}:\d{2}:\d{2}'"
    # 替换为TO_DATE('YYYY-MM-DD', 'yyyy-MM-dd')
    replaced_content = re.sub(pattern, r"TO_DATE('\1', 'yyyy-MM-dd')", sql_content)
    return replaced_content

# 写回SQL文件
def write_sql_file(file_path, sql_content):
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(sql_content)

# 主函数
def main():
    input_file_path = '/Users/liuzongchen/Downloads/CUSTOMIZE_INVOICE_VERIFICATION_20241106095236.sql'  # 输入SQL文件路径
    output_file_path = 'output.sql'  # 输出SQL文件路径

    # 读取SQL文件内容
    sql_content = read_sql_file(input_file_path)

    # 替换TIMESTAMP格式
    replaced_content = replace_timestamp_with_to_date(sql_content)

    # 写回SQL文件
    write_sql_file(output_file_path, replaced_content)

    print(f"TIMESTAMP格式已成功替换为TO_DATE格式，结果保存在 {output_file_path}")

if __name__ == "__main__":
    main()
