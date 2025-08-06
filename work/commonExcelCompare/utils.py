# utils.py
import sys
import os
import pandas as pd


def resource_path(relative_path):
    """获取打包后资源的绝对路径"""
    if hasattr(sys, '_MEIPASS'):
        return os.path.join(sys._MEIPASS, relative_path)
    return os.path.join(os.path.abspath("."), relative_path)


def normalize_value(val):
    """统一空值表示"""
    if pd.isna(val) or val is None or (isinstance(val, str) and str(val).strip() == ''):
        return ''
    return str(val).strip()
