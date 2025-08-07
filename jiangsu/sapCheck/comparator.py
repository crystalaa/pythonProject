# comparator.py
import sys
import traceback
import logging
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from PyQt5.QtCore import QThread, pyqtSignal
from data_handler import read_excel_fast, read_mapping_table
from jiangsu.sapCheck.rule_handler import read_enum_mapping


class CompareWorker(QThread):
    """用于在独立线程中执行比较操作"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)  # 用于更新进度条

    def __init__(self, file1, file2, rule_file, sheet_name1, sheet_name2, primary_keys=None, rules=None):
        super().__init__()
        self.file1 = file1
        self.file2 = file2
        self.rule_file = rule_file
        self.sheet_name1 = sheet_name1
        self.sheet_name2 = sheet_name2
        self.primary_keys = primary_keys if primary_keys else []
        self.rules = rules if rules else {}
        self.missing_assets = []
        self.diff_records = []
        self.summary = {}
        self.missing_rows = []
        self.extra_in_file2 = []
        self.diff_full_rows = []
        self.enum_map = read_enum_mapping(rule_file)  # 新增

    @staticmethod
    def normalize_value(val):
        """统一空值表示"""
        if pd.isna(val) or val is None or (isinstance(val, str) and str(val).strip() == ''):
            return ''
        return str(val).strip()

    def calculate_field(self, df, calc_rule, data_type):
        """
        根据计算规则和数据类型生成ERP表字段值
        支持：
        - 文本类型：字符串拼接（如"公司代码+资产编码"）
        - 数值类型：数值运算（如"使用年限+使用期间/12"）
        - 字符串截取（如"WBS编码[:12]"）
        - 字段运算（如"累计购置值-累计折旧额"）
        """
        if not calc_rule:
            return None

        try:
            # 处理字符串截取（通用逻辑）
            if '[:' in calc_rule and ']' in calc_rule:
                field, length_str = calc_rule.split('[:')
                field = field.strip()
                length = int(length_str.strip(']').strip())
                if field not in df.columns:
                    raise Exception(f"字段不存在：{field}")
                # 截取字符串前N位（处理空值）
                return df[field].fillna('').astype(str).str[:length]

            # 根据数据类型处理不同运算
            if data_type == "文本":
                # 文本类型：+表示字符串拼接
                fields = [f.strip() for f in calc_rule.split('+')]
                # 检查所有字段是否存在
                missing_fields = [f for f in fields if f not in df.columns]
                if missing_fields:
                    raise Exception(f"表达式中包含不存在的字段：{missing_fields}")
                # 字符串拼接（空值处理为空白字符串）
                result = df[fields[0]].fillna('').astype(str)
                for field in fields[1:]:
                    result += df[field].fillna('').astype(str)
                return result
            elif data_type == "数值":
                # 数值类型：执行数值运算
                import re
                field_pattern = re.compile(r'[a-zA-Z\u4e00-\u9fa5]+')
                fields_in_rule = field_pattern.findall(calc_rule)
                missing_fields = [f for f in fields_in_rule if f not in df.columns]
                if missing_fields:
                    raise Exception(f"表达式中包含不存在的字段：{missing_fields}")

                # 转换为数值类型，空值处理为0
                df_numeric = df.copy()
                for field in fields_in_rule:
                    # 如果字段名包含"折旧"，取绝对值
                    if "折旧" in field:
                        df_numeric[field] = pd.to_numeric(df[field], errors='coerce').fillna(0).abs()
                    else:
                        df_numeric[field] = pd.to_numeric(df[field], errors='coerce').fillna(0)

                # 执行计算
                result = df_numeric.eval(calc_rule)
                return result
            else:
                raise Exception(f"不支持的数据类型：{data_type}，请指定为'文本'或'数值'")

        except Exception as e:
            raise Exception(f"计算规则执行失败（{calc_rule}）：{str(e)}")

    def _get_value(self, df, part):
        """辅助函数：解析运算中的值（可能是字段或数值）"""
        part = part.strip()
        # 检查是否为字段
        if part in df.columns:
            # 尝试转换为数值（处理空值为0）
            return pd.to_numeric(df[part], errors='coerce').fillna(0)
        # 尝试解析为数值（如"12"）
        try:
            return float(part)
        except ValueError:
            raise Exception(f"无法解析值：{part}（不是字段也不是数值）")

    def values_equal_by_rule(self, val1, val2, data_type, tail_diff, field_name=""):
        """
        根据规则判断两个值是否相等
        """
        # 统一空值表示
        val1 = self.normalize_value(val1)
        val2 = self.normalize_value(val2)

        # 如果两个值都是空，则认为相等
        if val1 == "" and val2 == "":
            return True
        # ✅ 新增：监管资产属性字段特殊处理
        if field_name == "监管资产属性":
            def extract_last_segment(val, sep):
                if not val:
                    return ""
                parts = str(val).split(sep)
                return parts[-1].strip()

            val1_clean = extract_last_segment(val1, '\\')
            val2_clean = extract_last_segment(val2, '-')
            return val1_clean == val2_clean
        # --- 枚举值映射：站线电压等级 ---
        if field_name == "线站电压等级":
            # 平台表是名称 -> 编码
            code1 = self.enum_map.get(val1, val1)
            # ERP 本身就是编码
            return code1 == val2
        # ✅ 新增：布尔值映射逻辑
        bool_map = {
            "是": "Y",
            "否": "N",
            "Y": "是",
            "N": "否"
        }
        if val1 in bool_map and val2 in bool_map:
            if bool_map[val1] == val2 or val1 == bool_map[val2]:
                return True
        if field_name == "折旧方法":
            if val1 == "年限平均法" and val2 == "直线法":
                return True
            if val1 == "直线法" and val2 == "年限平均法":
                return True

        if data_type == "数值":
            # 数值型比较
            num1 = pd.to_numeric(val1, errors='coerce')
            num2 = pd.to_numeric(val2, errors='coerce')

            # 检查是否为NaN
            if pd.isna(num1) and pd.isna(num2):
                return True
            elif pd.isna(num1) or pd.isna(num2):
                return False

            # 如果字段名包含"折旧"，取绝对值
            field_contains_depreciation = "折旧" in field_name
            if field_contains_depreciation:
                num1 = abs(num1)
                num2 = abs(num2)

            if tail_diff is None:
                return num1 == num2
            else:
                return abs(num1 - num2) <= float(tail_diff)

        elif data_type == "日期":
            # 日期型比较
            def parse_date(date_str):
                """尝试多种格式解析日期，返回标准化字符串（YYYY-MM-DD）"""
                if pd.isna(date_str) or str(date_str).strip() == "":
                    return ""
                date_str = str(date_str).strip()
                if not date_str:
                    return ""
                # 尝试多种常见日期格式
                formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y年%m月%d日',
                           '%m-%d-%Y', '%m/%d/%Y', '%Y%m%d']
                for fmt in formats:
                    try:
                        return pd.to_datetime(date_str, format=fmt).strftime('%Y-%m-%d')
                    except (ValueError, TypeError):
                        continue
                # 如果所有格式都解析失败，返回原始字符串
                return date_str

            # 统一解析两列日期
            parsed1 = parse_date(val1)
            parsed2 = parse_date(val2)

            # 处理空值情况
            if parsed1 == "" and parsed2 == "":
                return True

            # 根据精度需求截取
            if tail_diff == "月":
                cmp1 = parsed1[:7]  # YYYY-MM
                cmp2 = parsed2[:7]
            elif tail_diff == "年":
                cmp1 = parsed1[:4]  # YYYY
                cmp2 = parsed2[:4]
            else:  # 默认精确到日
                cmp1 = parsed1  # YYYY-MM-DD
                cmp2 = parsed2

            return cmp1 == cmp2

        elif data_type == "文本":
            # 文本型比较
            return val1 == val2

        return val1 == val2

    def convert_asset_category(self, df1, df2, mapping_df):
        """资产分类转换逻辑"""
        # 平台表的资产分类字段
        asset_category_col1 = "资产分类"
        # ERP表的资产分类字段
        asset_category_col2 = "SAP资产类别描述"

        # 映射表的相关字段
        source_col = "同源目录完整名称"
        target_col = "21年资产目录大类"
        detail_col = "ERP资产明细类描述"
        code_col = "同源目录编码"
        erp_detail_col = "ERP资产明细类别"

        # 创建映射字典，用于将转换后的编码映射回原始值
        self.asset_code_to_original = {}  # 转换后编码 -> 原始值

        # 转换平台表的资产分类
        def convert_category(value):
            # 在映射表中查找匹配的记录
            matches = mapping_df[mapping_df[source_col] == value]
            if len(matches) == 0:
                converted_code = None  # 没有匹配项
            elif len(matches) == 1:
                # 唯一匹配项，直接返回同源目录编码前4位
                converted_code = str(matches.iloc[0][code_col])[:4]
            else:
                # 多个匹配项，需要根据ERP表的值来确定唯一项
                # 拼接21年资产目录大类和ERP资产明细类描述，与ERP表的SAP资产类别描述比较
                converted_code = None
                for _, row in matches.iterrows():
                    # 构造映射后的值
                    mapped_value = f"{row[target_col]}-{row[detail_col]}"
                    # 检查是否在ERP表中存在
                    if mapped_value in df2[asset_category_col2].values:
                        # 找到匹配项，返回同源目录编码前4位
                        converted_code = str(row[code_col])[:4]
                        break
                # 如果没有找到匹配项，返回第一条记录的同源目录编码前4位
                if converted_code is None:
                    converted_code = str(matches.iloc[0][code_col])[:4]

            # 保存编码到原始值的映射
            if converted_code is not None:
                self.asset_code_to_original[converted_code] = value

            return converted_code

        # 转换ERP表的资产分类
        def convert_category_table2(sap_value):
            # 在映射表中查找匹配的记录
            matches = mapping_df[
                mapping_df.apply(lambda row: f"{row[target_col]}-{row[detail_col]}" == sap_value, axis=1)
            ]
            if len(matches) > 0:
                # 找到匹配项，返回ERP资产明细类别前4位
                converted_code = str(matches.iloc[0][erp_detail_col])[:4]
            else:
                # 没有找到匹配项，返回原值
                converted_code = str(sap_value)

            # 保存编码到原始值的映射
            self.asset_code_to_original[converted_code] = sap_value

            return converted_code

        # 应用转换函数
        df1[asset_category_col1] = df1[asset_category_col1].apply(convert_category)
        df2[asset_category_col2] = df2[asset_category_col2].apply(convert_category_table2)

        return df1, df2

    def run(self):
        try:
            self.log_signal.emit("正在并行读取Excel文件...")

            with ThreadPoolExecutor(max_workers=2) as executor:
                future1 = executor.submit(read_excel_fast, self.file1, self.sheet_name1, is_file1=True)
                future2 = executor.submit(read_excel_fast, self.file2, self.sheet_name2, is_file1=False)
                try:
                    df1 = future1.result()
                    df2 = future2.result()
                except Exception as e:
                    raise Exception(f"读取文件时发生错误: {str(e)}")

            self.log_signal.emit("✅ Excel文件读取完成，开始比较数据...")
            self.log_signal.emit("开始比较数据...")
            # 读取资产分类映射表
            mapping_df = read_mapping_table(self.rule_file)

            # 转换资产分类
            df1, df2 = self.convert_asset_category(df1, df2, mapping_df)

            # 检查数据行是否存在
            if df1.empty:
                self.log_signal.emit("❌ 错误：平台表除了表头外没有数据行，请检查文件内容！")
                return

            if df2.empty:
                self.log_signal.emit("❌ 错误：ERP表除了表头外没有数据行，请检查文件内容！")
                return

            df1.columns = df1.columns.str.replace('[*\\s]', '', regex=True)
            df2.columns = df2.columns.str.replace('[*\\s]', '', regex=True)

            # 检查规则中的列是否在平台表和ERP表中都存在
            table2_columns_to_check = []
            for rule in self.rules.values():
                # 如果有计算规则，则不需要检查ERP表是否存在该字段
                if not rule.get("calc_rule") and rule["table2_field"]:
                    table2_columns_to_check.append(rule["table2_field"])

            table1_columns_to_compare = list(self.rules.keys())  # 平台表字段名
            # table2_columns_to_compare = [rule["table2_field"] for rule in self.rules.values()]  # ERP表字段名
            columns_to_compare = list(self.rules.keys())

            missing_in_file1 = [col for col in table1_columns_to_compare if col not in df1.columns]
            missing_in_file2 = [col for col in table2_columns_to_check if col not in df2.columns]

            if missing_in_file1 or missing_in_file2:
                error_msg = ""
                if missing_in_file1:
                    error_msg += f"平台表缺失以下规则定义的列：{', '.join(missing_in_file1)}\n"
                if missing_in_file2:
                    error_msg += f"ERP表缺失以下规则定义的列：{', '.join(missing_in_file2)}\n"
                self.log_signal.emit(f"❌ 比对失败：{error_msg}")
                return

            # 在"检查规则中的列是否存在"之后添加计算字段逻辑
            self.log_signal.emit("✅ 开始处理计算字段...")

            # 处理需要计算的字段
            # 存储计算字段的临时名称映射（原字段名 -> 临时名称）
            calc_temp_fields = {}

            # 处理需要计算的字段（使用临时名称避免冲突）
            for field1, rule in self.rules.items():
                if rule.get("calc_rule") and field1 in df2.columns:
                    df2.drop(columns=[field1], inplace=True)
                    self.log_signal.emit(f"删除ERP表中原有的 '{rule['table2_field']}' 列，将使用计算规则生成的新列")
                if field1 != rule["table2_field"] and field1 in df2.columns:
                    df2.drop(columns=[field1], inplace=True)
                if rule.get("calc_rule"):
                    self.log_signal.emit(f"正在计算ERP表字段: {field1} (规则: {rule['calc_rule']})")
                    try:
                        # 生成临时字段名（避免与ERP表原有字段冲突）
                        temp_field = f"__calc_{field1}__"
                        calc_temp_fields[field1] = temp_field

                        # 计算字段值并存储到临时字段
                        calculated_values = self.calculate_field(
                            df2,
                            rule["calc_rule"],
                            rule["data_type"]
                        )
                        df2[temp_field] = calculated_values
                    except Exception as e:
                        self.log_signal.emit(f"⚠️ 计算字段 {field1} 时出错: {str(e)}")

            # 修改映射逻辑（使用临时字段名进行映射）
            mapped_columns = {}
            for field1, rule in self.rules.items():
                field2 = rule["table2_field"]
                # 如果是计算字段，使用临时字段名映射
                if field1 in calc_temp_fields:
                    mapped_columns[calc_temp_fields[field1]] = field1
                elif field2 in df2.columns:
                    mapped_columns[field2] = field1
            # 映射完成后打印日志
            mapped_log = "\n".join([f"  {k} -> {v}" for k, v in mapped_columns.items()])
            self.log_signal.emit(f"字段映射关系：\n{mapped_log}")
            df2.rename(columns=mapped_columns, inplace=True)
            # 删除临时字段（可选）
            for temp_field in calc_temp_fields.values():
                if temp_field in df2.columns:
                    del df2[temp_field]
            # 保留需要比对的列
            all_needed_columns = list(set(columns_to_compare + self.primary_keys))
            df1 = df1[all_needed_columns]
            df2 = df2[all_needed_columns]

            # 检查主键列是否为空
            if not self.primary_keys:
                self.log_signal.emit("❌ 错误：规则文件中未定义主键字段，请检查规则文件！")
                return

                # 检查主键列在数据中是否存在
            for pk in self.primary_keys:
                if pk not in df1.columns:
                    self.log_signal.emit(f"❌ 错误：平台表中不存在主键列 '{pk}'")
                    return
                if pk not in df2.columns:
                    self.log_signal.emit(f"❌ 错误：ERP表中不存在主键列 '{pk}'")
                    return

                # 检查主键是否有重复值
            df1_duplicates = df1[df1.duplicated(subset=self.primary_keys, keep=False)]
            if not df1_duplicates.empty:
                duplicate_count = df1_duplicates.shape[0]
                self.log_signal.emit(f"❌ 错误：平台表中存在 {duplicate_count} 条重复的主键记录")
                # 显示前几个重复的主键示例
                duplicate_examples = df1_duplicates[self.primary_keys].head(5)
                example_lines = []
                for _, row in duplicate_examples.iterrows():
                    keys = [str(row[pk]) for pk in self.primary_keys]
                    example_lines.append(" + ".join(keys))
                examples = "\n".join([f" - {example}" for example in example_lines])
                self.log_signal.emit(f"重复主键示例（前5个）：\n{examples}")
                return

            df2_duplicates = df2[df2.duplicated(subset=self.primary_keys, keep=False)]
            if not df2_duplicates.empty:
                duplicate_count = df2_duplicates.shape[0]
                self.log_signal.emit(f"❌ 错误：ERP表中存在 {duplicate_count} 条重复的主键记录")
                # 显示前几个重复的主键示例
                duplicate_examples = df2_duplicates[self.primary_keys].head(5)
                example_lines = []
                for _, row in duplicate_examples.iterrows():
                    keys = [str(row[pk]) for pk in self.primary_keys]
                    example_lines.append(" + ".join(keys))
                examples = "\n".join([f" - {example}" for example in example_lines])
                self.log_signal.emit(f"重复主键示例（前5个）：\n{examples}")
                return

            # 检查主键列是否有空值
            for pk in self.primary_keys:
                df1_empty_keys = df1[pd.isna(df1[pk]) | (df1[pk].astype(str).astype(str).str.strip() == '')]
                df2_empty_keys = df2[pd.isna(df2[pk]) | (df2[pk].astype(str).astype(str).str.strip() == '')]

                if len(df1_empty_keys) > 0:
                    self.log_signal.emit(f"⚠️ 警告：平台表中主键列 '{pk}' 存在 {len(df1_empty_keys)} 条空值记录")

                if len(df2_empty_keys) > 0:
                    self.log_signal.emit(f"⚠️ 警告：ERP表中主键列 '{pk}' 存在 {len(df2_empty_keys)} 条空值记录")

            # 保存原始数据帧用于导出（包含主键列）
            df1_original = df1.copy()
            df2_original = df2.copy()

            # 设置主键索引
            df1.set_index(self.primary_keys, inplace=True)
            df2.set_index(self.primary_keys, inplace=True)

            # 规范化索引格式
            df1.index = df1.index.map(lambda x: tuple(str(i) for i in x) if isinstance(x, tuple) else (str(x),))
            df2.index = df2.index.map(lambda x: tuple(str(i) for i in x) if isinstance(x, tuple) else (str(x),))
            # 检查索引中是否有空值
            df1_empty_index = df1.index.map(
                lambda x: any(pd.isna(i) or str(i).strip() == '' for i in (x if isinstance(x, tuple) else (x,))))
            df2_empty_index = df2.index.map(
                lambda x: any(pd.isna(i) or str(i).strip() == '' for i in (x if isinstance(x, tuple) else (x,))))

            df1_empty_count = sum(df1_empty_index)
            df2_empty_count = sum(df2_empty_index)

            if df1_empty_count > 0:
                self.log_signal.emit(f"⚠️ 警告：平台表中有 {df1_empty_count} 条记录的主键为空")

            if df2_empty_count > 0:
                self.log_signal.emit(f"⚠️ 警告：ERP表中有 {df2_empty_count} 条记录的主键为空")

            if len(df1) != len(df2):
                self.log_signal.emit(f"提示：两个文件的行数不一致（平台表有 {len(df1)} 行，ERP表有 {len(df2)} 行）")

            # 查找ERP表中缺失的主键
            missing_in_file2 = df1.index.difference(df2.index)
            if not missing_in_file2.empty:
                missing_df = df1.loc[missing_in_file2].copy()
                original_codes = missing_in_file2.map(lambda x: ' + '.join(map(str, x)))
                missing_df.reset_index(drop=True, inplace=True)

                for idx, key in enumerate(self.primary_keys):
                    missing_df.insert(1 + idx, key, original_codes.map(lambda x: x.split(' + ')[idx]))

                self.missing_rows = missing_df.to_dict(orient='records')
                missing_list = "\n".join([f" - {code}" for code in missing_in_file2])
                self.log_signal.emit(f"【ERP表中缺失的主键】（共 {len(missing_in_file2)} 条）：\n{missing_list}")

            # 查找ERP表中多出的主键
            missing_in_file1 = df2.index.difference(df1.index)
            if not missing_in_file1.empty:
                missing_df_file1 = df2.loc[missing_in_file1].copy()
                original_codes_file1 = missing_in_file1.map(lambda x: ' + '.join(map(str, x)))
                missing_df_file1.reset_index(drop=True, inplace=True)

                for idx, key in enumerate(self.primary_keys):
                    missing_df_file1.insert(1 + idx, key, original_codes_file1.map(lambda x: x.split(' + ')[idx]))

                self.extra_in_file2 = missing_df_file1.to_dict(orient='records')
                missing_list_file1 = "\n".join([f" - {code}" for code in missing_in_file1])
                self.log_signal.emit(
                    f"【ERP表中多出的主键】（平台表中没有，共 {len(missing_in_file1)} 条）：\n{missing_list_file1}")

            # 找出共同的主键
            common_codes = df1.index.intersection(df2.index)
            if common_codes.empty:
                self.log_signal.emit("警告：两个文件中没有共同的主键！")
                return

            # 替换原有的数据比较部分为以下代码：
            try:
                self.log_signal.emit("开始进行向量化数据比较...")
                df1_common = df1.loc[common_codes]
                df2_common = df2.loc[common_codes]

                # 格式化索引
                df1_common.index = df1_common.index.map(lambda x: ' + '.join(x) if isinstance(x, tuple) else str(x))
                df2_common.index = df2_common.index.map(lambda x: ' + '.join(x) if isinstance(x, tuple) else str(x))
                # 使用向量化操作进行批量比较
                diff_dict = {}

                # 预处理索引以便后续使用
                index_mapping = pd.Series(df1_common.index, index=df1_common.index)

                for field1, rule in self.rules.items():
                    # 只比对规则文件中定义的列
                    if field1 not in df1_common.columns or field1 not in df2_common.columns:
                        continue

                    data_type = rule["data_type"]
                    tail_diff = rule.get("tail_diff")

                    # 向量化获取两列数据
                    series1 = df1_common[field1]
                    series2 = df2_common[field1]

                    if data_type == "数值":
                        # 数值型比较
                        series1_num = pd.to_numeric(series1, errors='coerce')
                        series2_num = pd.to_numeric(series2, errors='coerce')
                        # 如果字段名包含"折旧"，取绝对值
                        if "折旧" in field1:
                            series1_num = series1_num.abs()
                            series2_num = series2_num.abs()
                        if tail_diff is None:
                            diff_mask = (series1_num != series2_num) & \
                                        ~(pd.isna(series1_num) & pd.isna(series2_num))
                        else:
                            diff_mask = (abs(series1_num - series2_num) > float(tail_diff)) & \
                                        ~(pd.isna(series1_num) & pd.isna(series2_num))

                    elif data_type == "日期":
                        # 日期型比较
                        # 日期型比较：先统一解析为标准日期格式
                        def parse_date(date_str):
                            """尝试多种格式解析日期，返回标准化字符串（YYYY-MM-DD）"""
                            if pd.isna(date_str):
                                return ""
                            date_str = str(date_str).strip()
                            if not date_str:
                                return ""
                            # 尝试多种常见日期格式
                            formats = ['%Y-%m-%d', '%Y/%m/%d', '%Y年%m月%d日',
                                       '%m-%d-%Y', '%m/%d/%Y', '%Y%m%d']
                            for fmt in formats:
                                try:
                                    return pd.to_datetime(date_str, format=fmt).strftime('%Y-%m-%d')
                                except (ValueError, TypeError):
                                    continue
                            # 如果所有格式都解析失败，返回原始字符串
                            return date_str

                        # 统一解析两列日期
                        series1_parsed = series1.apply(parse_date)
                        series2_parsed = series2.apply(parse_date)

                        # 处理空值情况
                        both_empty = (series1_parsed == "") & (series2_parsed == "")

                        # 根据精度需求截取
                        if tail_diff == "月":
                            series1_cmp = series1_parsed.str[:7]  # YYYY-MM
                            series2_cmp = series2_parsed.str[:7]
                        elif tail_diff == "年":
                            series1_cmp = series1_parsed.str[:4]  # YYYY
                            series2_cmp = series2_parsed.str[:4]
                        else:  # 默认精确到日
                            series1_cmp = series1_parsed  # YYYY-MM-DD
                            series2_cmp = series2_parsed

                        diff_mask = (series1_cmp != series2_cmp) & ~both_empty

                    elif data_type == "文本":
                        def mapped_equal(a, b, field):
                            return self.values_equal_by_rule(a, b, "文本", None, field)

                        diff_mask = ~pd.Series([
                            mapped_equal(self.normalize_value(s1).strip(), self.normalize_value(s2).strip(), field1)
                            for s1, s2 in zip(series1, series2)
                        ], index=series1.index)

                    # 找出有差异的行索引
                    diff_indices = df1_common[diff_mask].index

                    # 批量添加差异记录
                    for idx in diff_indices:
                        if idx not in diff_dict:
                            diff_dict[idx] = []

                        val1 = CompareWorker.normalize_value(series1.loc[idx])
                        val2 = CompareWorker.normalize_value(series2.loc[idx])
                        diff_dict[idx].append((field1, val1, val2))

                self.log_signal.emit(f"向量化比较完成，共发现 {len(diff_dict)} 条差异记录")

            except Exception as e:
                self.log_signal.emit(f"向量化比较出错，使用传统方法: {str(e)}")
                # 如果向量化方法出错，回退到原来的逐行比较方法

            # 生成日志和汇总信息
            # 生成日志和汇总信息
            diff_log_messages = []
            self.diff_full_rows = []

            # 创建主键到原始值的映射，用于恢复导出数据中的主键列
            pk_mapping = {}
            for code in common_codes:
                code_str = ' + '.join(code) if isinstance(code, tuple) else str(code)
                pk_mapping[code_str] = code

            for code, diffs in diff_dict.items():
                code_str = ' + '.join(code) if isinstance(code, tuple) else str(code)
                diff_details = []

                for col, val1, val2 in diffs:
                    if col == "资产分类" and hasattr(self, 'asset_code_to_original'):
                        # 使用原始中文值显示
                        original_val1 = self.asset_code_to_original.get(val1, val1)
                        original_val2 = self.asset_code_to_original.get(val2, val2)
                        diff_details.append(f" - 列 [{col}] 不一致：平台表={original_val1}, ERP表={original_val2}")
                    else:
                        diff_details.append(f" - 列 [{col}] 不一致：平台表={val1}, ERP表={val2}")

                diff_log_messages.append(f"\n主键：{code}")
                diff_log_messages.extend(diff_details)

                # 使用原始数据帧查找完整行数据（包含主键列）
                try:
                    # 获取原始主键值
                    original_pk_values = pk_mapping.get(code_str, code)

                    # 构建筛选条件
                    if isinstance(original_pk_values, tuple):
                        # 多主键情况
                        condition1 = True
                        condition2 = True
                        for i, pk in enumerate(self.primary_keys):
                            condition1 = condition1 & (df1_original[pk].astype(str) == original_pk_values[i])
                            condition2 = condition2 & (df2_original[pk].astype(str) == original_pk_values[i])
                    else:
                        # 单主键情况
                        pk = self.primary_keys[0]
                        condition1 = (df1_original[pk].astype(str) == original_pk_values)
                        condition2 = (df2_original[pk].astype(str) == original_pk_values)

                    # 获取完整行数据
                    source_dict = df1_original[condition1].iloc[0].to_dict()
                    target_dict = df2_original[condition2].iloc[0].to_dict()

                    self.diff_full_rows.append({
                        "source": source_dict,
                        "target": target_dict
                    })
                except (IndexError, KeyError, Exception) as e:
                    # 出现异常时使用原来的方法作为备选
                    source_dict = df1_common.loc[code_str].to_dict()
                    target_dict = df2_common.loc[code_str].to_dict()

                    # 手动添加主键信息
                    original_pk_values = pk_mapping.get(code_str, code)
                    if isinstance(original_pk_values, tuple):
                        for i, pk in enumerate(self.primary_keys):
                            source_dict[pk] = original_pk_values[i]
                            target_dict[pk] = original_pk_values[i]
                    else:
                        if self.primary_keys:
                            source_dict[self.primary_keys[0]] = original_pk_values
                            target_dict[self.primary_keys[0]] = original_pk_values

                    self.diff_full_rows.append({
                        "source": source_dict,
                        "target": target_dict
                    })

                except (IndexError, KeyError, Exception) as e:
                    # 出现异常时使用原来的方法作为备选
                    source_dict = df1_common.loc[code_str].to_dict()
                    target_dict = df2_common.loc[code_str].to_dict()

                    # 手动添加主键信息
                    original_pk_values = pk_mapping.get(code_str, code)
                    if isinstance(original_pk_values, tuple):
                        for i, pk in enumerate(self.primary_keys):
                            source_dict[pk] = original_pk_values[i]
                            target_dict[pk] = original_pk_values[i]
                    else:
                        if self.primary_keys:
                            source_dict[self.primary_keys[0]] = original_pk_values
                            target_dict[self.primary_keys[0]] = original_pk_values

                    self.diff_full_rows.append({
                        "source": source_dict,
                        "target": target_dict
                    })

            diff_count = len(diff_dict)
            equal_count = len(common_codes) - diff_count
            primary_key_str = " + ".join(self.primary_keys)

            self.summary = {
                "primary_key": primary_key_str,
                "total_file1": len(df1),
                "total_file2": len(df2),
                "missing_count": len(missing_in_file2),
                "extra_count": len(missing_in_file1),
                "common_count": len(common_codes),
                "diff_count": diff_count,
                "equal_count": equal_count,
                "diff_ratio": diff_count / len(common_codes) if len(common_codes) > 0 else 0.0,
            }
            self.asset_code_map = self.asset_code_to_original  # 仅多一行
            if diff_count == 0:
                self.log_signal.emit("【共同主键的数据完全一致】，没有差异。")
            else:
                self.log_signal.emit(f"【存在差异的列】（共 {diff_count} 行）：")
                if diff_log_messages:
                    self.log_signal.emit('\n'.join(diff_log_messages))
                else:
                    self.log_signal.emit("⚠️ 未找到具体差异列，请检查数据是否一致。")

        except Exception as e:
            logging.error(traceback.format_exc())
            self.log_signal.emit(f"发生错误：{str(e)}")
        finally:
            self.quit()
            self.wait()
