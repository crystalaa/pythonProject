import logging
import traceback
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
from PyQt5.QtCore import QThread, pyqtSignal
from excel_operations import read_excel_columns, get_sheet_names, read_excel_fast


class LoadColumnWorker(QThread):
    """用于在独立线程中读取列名"""
    columns_loaded = pyqtSignal(str, list)  # 参数为文件路径和列名列表
    error_occurred = pyqtSignal(str)
    sheet_names_loaded = pyqtSignal(str, list)

    def __init__(self, file_path, sheet_name=None):
        super().__init__()
        self.file_path = file_path
        self.sheet_name = sheet_name

    def run(self):
        try:
            # 读取页签名称
            sheet_names = get_sheet_names(self.file_path)
            self.sheet_names_loaded.emit(self.file_path, sheet_names)
            if not self.sheet_name:
                return
            # 读取列名
            columns = read_excel_columns(self.file_path, self.sheet_name)
            if columns is None:
                return
            self.columns_loaded.emit(self.file_path, columns)
        except Exception as e:
            self.error_occurred.emit(str(e))


class CompareWorker(QThread):
    """用于在独立线程中执行比较操作"""
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)  # 用于更新进度条

    def __init__(self, file1, file2, sheet_name1, sheet_name2, primary_keys=None, rules=None):
        super().__init__()
        self.file1 = file1
        self.file2 = file2
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

    @staticmethod
    def normalize_value(val):
        """统一空值表示"""
        if pd.isna(val) or val is None or (isinstance(val, str) and str(val).strip() == ''):
            return ''
        return str(val).strip()

    def run(self):
        try:
            self.log_signal.emit("正在并行读取Excel文件...")

            with ThreadPoolExecutor(max_workers=2) as executor:
                future1 = executor.submit(read_excel_fast, self.file1, self.sheet_name1)
                future2 = executor.submit(read_excel_fast, self.file2, self.sheet_name2)
                try:
                    df1 = future1.result()
                    df2 = future2.result()
                except Exception as e:
                    raise Exception(f"读取文件时发生错误: {str(e)}")

            self.log_signal.emit("✅ Excel文件读取完成，开始比较数据...")
            # 检查数据行是否存在
            if df1.empty:
                self.log_signal.emit("❌ 错误：表一除了表头外没有数据行，请检查文件内容！")
                return

            if df2.empty:
                self.log_signal.emit("❌ 错误：表二除了表头外没有数据行，请检查文件内容！")
                return

            df1.columns = df1.columns.str.replace('[*\\s]', '', regex=True)
            df2.columns = df2.columns.str.replace('[*\\s]', '', regex=True)

            # 检查规则中的列是否在表一和表二表二中都存在
            table1_columns_to_compare = list(self.rules.keys())  # 表一字段名
            table2_columns_to_compare = [rule["table2_field"] for rule in self.rules.values()]  # 表二字段名
            columns_to_compare = list(self.rules.keys())

            missing_in_file1 = [col for col in table1_columns_to_compare if col not in df1.columns]
            missing_in_file2 = [col for col in table2_columns_to_compare if col not in df2.columns]

            if missing_in_file1 or missing_in_file2:
                error_msg = ""
                if missing_in_file1:
                    error_msg += f"表一缺失以下规则定义的列：{', '.join(missing_in_file1)}\n"
                if missing_in_file2:
                    error_msg += f"表二缺失以下规则定义的列：{', '.join(missing_in_file2)}\n"
                self.log_signal.emit(f"❌ 比对失败：{error_msg}")
                return

            # 映射 df2 的列名为 df1 的列名
            mapped_columns = {}
            for field1, rule in self.rules.items():
                field2 = rule["table2_field"]
                if field2 in df2.columns:
                    mapped_columns[field2] = field1
            df2.rename(columns=mapped_columns, inplace=True)

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
                    self.log_signal.emit(f"❌ 错误：表一中不存在主键列 '{pk}'")
                    return
                if pk not in df2.columns:
                    self.log_signal.emit(f"❌ 错误：表二中不存在主键列 '{pk}'")
                    return

            # 检查主键是否有重复值
            df1_duplicates = df1[df1.duplicated(subset=self.primary_keys, keep=False)]
            if not df1_duplicates.empty:
                duplicate_count = df1_duplicates.shape[0]
                self.log_signal.emit(f"❌ 错误：表一中存在 {duplicate_count} 条重复的主键记录")
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
                self.log_signal.emit(f"❌ 错误：表二中存在 {duplicate_count} 条重复的主键记录")
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
                df1_empty_keys = df1[pd.isna(df1[pk]) | (df1[pk].astype(str).str.strip() == '')]
                df2_empty_keys = df2[pd.isna(df2[pk]) | (df2[pk].astype(str).str.strip() == '')]

                if len(df1_empty_keys) > 0:
                    self.log_signal.emit(f"⚠️ 警告：表一中主键列 '{pk}' 存在 {len(df1_empty_keys)} 条空值记录")

                if len(df2_empty_keys) > 0:
                    self.log_signal.emit(f"⚠️ 警告：表二中主键列 '{pk}' 存在 {len(df2_empty_keys)} 条空值记录")

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
                self.log_signal.emit(f"⚠️ 警告：表一中有 {df1_empty_count} 条记录的主键为空")

            if df2_empty_count > 0:
                self.log_signal.emit(f"⚠️ 警告：表二中有 {df2_empty_count} 条记录的主键为空")

            if len(df1) != len(df2):
                self.log_signal.emit(f"提示：两个文件的行数不一致（表一有 {len(df1)} 行，表二有 {len(df2)} 行）")

            # 查找表二中缺失的主键
            missing_in_file2 = df1.index.difference(df2.index)
            if not missing_in_file2.empty:
                missing_df = df1.loc[missing_in_file2].copy()
                original_codes = missing_in_file2.map(lambda x: ' + '.join(map(str, x)))
                missing_df.reset_index(drop=True, inplace=True)

                for idx, key in enumerate(self.primary_keys):
                    missing_df.insert(1 + idx, key, original_codes.map(lambda x: x.split(' + ')[idx]))

                self.missing_rows = missing_df.to_dict(orient='records')
                missing_list = "\n".join([f" - {code}" for code in missing_in_file2])
                self.log_signal.emit(f"【表二中缺失的主键】（共 {len(missing_in_file2)} 条）：\n{missing_list}")

            # 查找表二中多出的主键
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
                    f"【表二中多出的主键】（表一中没有，共 {len(missing_in_file1)} 条）：\n{missing_list_file1}")

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

                        if tail_diff is None:
                            diff_mask = (series1_num != series2_num) & \
                                        ~(pd.isna(series1_num) & pd.isna(series2_num))
                        else:
                            diff_mask = (abs(series1_num - series2_num) > float(tail_diff)) & \
                                        ~(pd.isna(series1_num) & pd.isna(series2_num))

                    elif data_type == "日期":
                        # 日期型比较
                        both_empty = (series1.fillna('').astype(str).str.strip() == '') & \
                                     (series2.fillna('').astype(str).str.strip() == '')

                        s1_str = series1.astype(str)
                        s2_str = series2.astype(str)

                        if tail_diff == "月":
                            series1_cmp = s1_str.str[:7]
                            series2_cmp = s2_str.str[:7]
                        elif tail_diff == "日":
                            series1_cmp = s1_str.str[:10]
                            series2_cmp = s2_str.str[:10]
                        elif tail_diff == "时":
                            series1_cmp = s1_str.str[:13]
                            series2_cmp = s2_str.str[:13]
                        elif tail_diff == "分":
                            series1_cmp = s1_str.str[:16]
                            series2_cmp = s2_str.str[:16]
                        elif tail_diff == "秒":
                            series1_cmp = s1_str.str[:19]
                            series2_cmp = s2_str.str[:19]
                        else:
                            series1_cmp = s1_str.str[:4]
                            series2_cmp = s2_str.str[:4]

                        diff_mask = (series1_cmp != series2_cmp) & ~both_empty

                    elif data_type == "文本":
                        # 文本型比较
                        series1_norm = series1.fillna('').astype(str).str.strip()
                        series2_norm = series2.fillna('').astype(str).str.strip()
                        diff_mask = series1_norm != series2_norm

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
            diff_log_messages = []
            self.diff_full_rows = []

            # 创建主键到原始值的映射，用于恢复导出数据中的主键列
            pk_mapping = {}
            for code in common_codes:
                code_str = ' + '.join(code) if isinstance(code, tuple) else str(code)
                pk_mapping[code_str] = code

            for code, diffs in diff_dict.items():
                code_str = ' + '.join(code) if isinstance(code, tuple) else str(code)
                diff_details = [f" - 列 [{col}] 不一致：表一={val1}, 表二={val2}" for col, val1, val2 in diffs]
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