import pandas as pd
import numpy as np
import uuid

# 读取原始Excel文件
file_path = '/Users/liuzongchen/Desktop/数据核对/源文件.log.xlsx'  # 替换为你的文件路径
sheet_name = '附表1资产卡片期初数据收集模板'
df = pd.read_excel(file_path, sheet_name=sheet_name)

# 获取原始数据的列名
columns = df.columns.tolist()
# columns1 = [col.replace('*', '') for col in df.columns.tolist()]

# 确保“资产编码”列存在
if '*资产编码' not in columns:
    raise ValueError("原始数据中缺少‘资产编码’列")

# 生成400万行数据
total_rows = 1_000_000
existing_codes = set(df['*资产编码'])  # 已有的资产编码
new_data = []

# 生成唯一资产编码
def generate_unique_code(existing_codes):
    while True:
        code = str(uuid.uuid4())  # 使用UUID生成唯一编码
        if code not in existing_codes:
            existing_codes.add(code)
            return code

# 随机生成其他列的数据
def generate_random_value():
    return np.random.choice(['A', 'B', 'C', 'D', 'E'], p=[0.2, 0.2, 0.2, 0.2, 0.2])

# 生成新数据
for _ in range(total_rows - len(df)):
    row = {
        '*资产编码': generate_unique_code(existing_codes)
    }
    for col in columns:
        if col != '*资产编码':
            row[col] = generate_random_value()
    new_data.append(row)

# 将新数据转换为DataFrame
new_df = pd.DataFrame(new_data, columns=columns)

# 合并原始数据和新数据
final_df = pd.concat([df, new_df], ignore_index=True)

# 保存为新的Excel文件
output_file = '/Users/liuzongchen/Desktop/数据核对/源文件new.log.xlsx'  # 输出文件路径
final_df.to_excel(output_file, index=False, engine='openpyxl')

print(f"数据生成完成，已保存至 {output_file}")
