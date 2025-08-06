# 处理表二的主键列重复问题并去重
import pandas as pd


def process_primary_keys(self, df2):
    # 1. 从规则文件获取主键字段列表
    # 假设规则文件结构: self.rules = [{'column': '字段名', 'is_primary': True/False, ...}, ...]
    primary_columns = [
        rule['column'] for rule in self.rules 
        if rule.get('is_primary', False)
    ]
    
    if not primary_columns:
        self.log_signal.emit("警告：规则文件中未定义主键字段")
        return df2
    
    # 2. 处理每个主键字段的重复列问题
    for pk in primary_columns:
        # 检查当前主键在DataFrame中是否存在重复列
        pk_count = df2.columns.tolist().count(pk)
        if pk_count > 1:
            # 获取所有重复列的索引位置
            duplicate_indices = [i for i, col in enumerate(df2.columns) if col == pk]
            self.log_signal.emit(f"发现{pk_count}个重复的{pk}列，索引位置: {duplicate_indices}")
            
            # 确定需要保留的列（基于映射规则判断计算列）
            # 假设映射规则格式: self.mapping = {'目标列': '源列或计算逻辑'}
            keep_index = self._determine_keep_column(pk, duplicate_indices)
            
            # 构建保留掩码，删除其他重复列
            mask = []
            for i, col in enumerate(df2.columns):
                if col == pk:
                    mask.append(i == keep_index)  # 只保留指定索引的列
                else:
                    mask.append(True)  # 非主键列全部保留
            
            # 应用掩码过滤重复列
            df2 = df2.loc[:, mask]
            self.log_signal.emit(f"已保留{pk}的第{keep_index+1}个实例，删除其他重复列")
    
    # 3. 对处理后的主键进行去重操作
    df2 = self._deduplicate_primary_keys(df2, primary_columns)
    
    return df2

# 辅助方法：确定需要保留的主键列
def _determine_keep_column(self, pk, duplicate_indices):
    # 检查该主键是否是通过映射计算得到的
    if pk in self.mapping:
        mapping_source = self.mapping[pk]
        # 如果映射源不是原始列名（是计算逻辑），通常计算列会在后面生成
        if not isinstance(mapping_source, str) or mapping_source not in self原始列名列表:
            self.log_signal.emit(f"{pk}是计算列，保留最后出现的实例")
            return duplicate_indices[-1]  # 保留最后一个
    
    # 默认策略：如果是原始列，保留第一个出现的实例
    self.log_signal.emit(f"{pk}是原始列，保留第一个出现的实例")
    return duplicate_indices[0]  # 保留第一个

# 辅助方法：基于主键去重
def _deduplicate_primary_keys(self, df2, primary_columns):
    # 先统一转换主键列的数据类型为字符串并清洗
    for pk in primary_columns:
        # 确保是Series（单列）
        if isinstance(df2[pk], pd.DataFrame):
            # 极端情况：仍有多列，强制取第一列
            df2[pk] = df2[pk].iloc[:, 0]
        
        # 转换为字符串并清除空白字符
        df2[pk] = df2[pk].astype(str).str.strip().replace(r'\s+', '', regex=True)
    
    # 执行去重
    original_count = len(df2)
    df2 = df2.drop_duplicates(subset=primary_columns, keep='first')
    removed_count = original_count - len(df2)
    
    # 输出去重结果日志
    if len(primary_columns) == 1:
        self.log_signal.emit(f"表二{primary_columns[0]}去重完成，移除{removed_count}条重复记录，保留{len(df2)}条")
    else:
        self.log_signal.emit(f"表二联合主键去重完成，移除{removed_count}条重复记录，保留{len(df2)}条")
    
    return df2