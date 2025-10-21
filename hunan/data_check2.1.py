import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkinterdnd2 import DND_FILES, TkinterDnD
import pandas as pd
import numpy as np
import os
import logging
from logging.handlers import RotatingFileHandler
import datetime
import traceback


# 配置日志
def setup_logging():
    """配置日志系统"""
    # 创建日志目录
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # 创建日志文件名（包含日期）
    current_date = datetime.datetime.now().strftime("%Y%m%d")
    log_file = os.path.join(log_dir, f"financial_reconciliation_{current_date}.log")

    # 创建日志记录器
    logger = logging.getLogger("FinancialReconciliation")
    logger.setLevel(logging.DEBUG)

    # 创建滚动日志处理器（最大10MB，保留5个备份）
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    # 创建日志格式
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(module)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 添加处理器
    if not logger.handlers:  # 避免重复添加处理器
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


# 初始化日志
logger = setup_logging()


class FinancialDataReconciliationApp:
    def __init__(self, root):
        self.root = root
        self.root.title("财务数据核对工具 - 规则1完善版")
        self.root.geometry("1200x800")

        # 初始化变量
        self.file_paths = {
            "期初往来数据与账务数据核对文件": None,
            "往来台账查询文件": None,
            "多维凭证明细文件": None
        }
        self.dataframes = {
            "期初往来数据与账务数据核对文件": None,
            "往来台账查询文件": None,
            "多维凭证明细文件": None
        }
        self.reconciliation_results = []
        self.matching_results = []  # 存储每条记录的匹配结果

        # 记录开始时间
        self.start_time = datetime.datetime.now()
        logger.info("财务数据核对工具启动")

        # 创建主框架
        self.main_frame = ttk.Frame(root, padding="10")
        self.main_frame.pack(fill=tk.BOTH, expand=True)

        # 创建文件上传区域
        self.create_file_upload_section()

        # 创建结果展示区域
        self.create_result_display_section()

        # 创建操作按钮区域
        self.create_action_buttons()

        # 创建日志显示区域
        self.create_log_display_section()

    def create_file_upload_section(self):
        """创建文件上传区域"""
        try:
            logger.debug("创建文件上传区域")
            file_upload_frame = ttk.LabelFrame(self.main_frame, text="文件上传", padding="10")
            file_upload_frame.pack(fill=tk.X, pady=5)

            # 创建文件上传组件
            self.file_entries = {}
            self.file_buttons = {}

            for i, file_type in enumerate(self.file_paths.keys()):
                frame = ttk.Frame(file_upload_frame)
                frame.pack(fill=tk.X, pady=2)

                label = ttk.Label(frame, text=f"{file_type}:", width=25)
                label.pack(side=tk.LEFT)

                entry = ttk.Entry(frame, state="readonly")
                entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)
                self.file_entries[file_type] = entry

                button = ttk.Button(frame, text="浏览", command=lambda ft=file_type: self.browse_file(ft))
                button.pack(side=tk.LEFT, padx=2)
                self.file_buttons[file_type] = button

                # 启用拖放功能
                entry.drop_target_register(DND_FILES)
                entry.dnd_bind('<<Drop>>', lambda e, ft=file_type: self.drop_file(e, ft))

            # 添加说明标签
            instruction_label = ttk.Label(file_upload_frame, text="提示: 您可以将文件直接拖放到输入框中",
                                          foreground="gray")
            instruction_label.pack(pady=5)

            logger.debug("文件上传区域创建完成")
        except Exception as e:
            logger.error(f"创建文件上传区域时发生错误: {str(e)}", exc_info=True)
            messagebox.showerror("错误", f"创建文件上传区域时发生错误: {str(e)}")

    def create_result_display_section(self):
        """创建结果展示区域"""
        try:
            logger.debug("创建结果展示区域")
            result_display_frame = ttk.LabelFrame(self.main_frame, text="核对结果（规则1）", padding="10")
            result_display_frame.pack(fill=tk.BOTH, expand=True, pady=5)

            # 创建结果表格
            self.result_tree = ttk.Treeview(result_display_frame, columns=(
                "问题编号", "问题描述", "涉及科目", "涉及往来单位", "差额", "状态"), show="headings")
            self.result_tree.heading("问题编号", text="问题编号")
            self.result_tree.heading("问题描述", text="问题描述")
            self.result_tree.heading("涉及科目", text="涉及科目")
            self.result_tree.heading("涉及往来单位", text="涉及往来单位")
            self.result_tree.heading("差额", text="差额")
            self.result_tree.heading("状态", text="状态")

            # 设置列宽
            self.result_tree.column("问题编号", width=80)
            self.result_tree.column("问题描述", width=250)
            self.result_tree.column("涉及科目", width=120)
            self.result_tree.column("涉及往来单位", width=150)
            self.result_tree.column("差额", width=100)
            self.result_tree.column("状态", width=100)

            # 添加滚动条
            scrollbar = ttk.Scrollbar(result_display_frame, orient=tk.VERTICAL, command=self.result_tree.yview)
            self.result_tree.configure(yscrollcommand=scrollbar.set)

            # 布局
            self.result_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
            logger.debug("结果展示区域创建完成")
        except Exception as e:
            logger.error(f"创建结果展示区域时发生错误: {str(e)}", exc_info=True)
            messagebox.showerror("错误", f"创建结果展示区域时发生错误: {str(e)}")

    def create_action_buttons(self):
        """创建操作按钮区域"""
        try:
            logger.debug("创建操作按钮区域")
            action_buttons_frame = ttk.Frame(self.main_frame, padding="10")
            action_buttons_frame.pack(fill=tk.X, pady=5)

            # 创建按钮
            start_button = ttk.Button(action_buttons_frame, text="开始核对（规则1）", command=self.start_reconciliation)
            start_button.pack(side=tk.LEFT, padx=5)

            export_button = ttk.Button(action_buttons_frame, text="导出结果", command=self.export_results,
                                       state=tk.DISABLED)
            export_button.pack(side=tk.LEFT, padx=5)
            self.export_button = export_button

            clear_button = ttk.Button(action_buttons_frame, text="清空", command=self.clear_all)
            clear_button.pack(side=tk.LEFT, padx=5)

            # 添加日志按钮
            log_button = ttk.Button(action_buttons_frame, text="查看日志", command=self.open_log_folder)
            log_button.pack(side=tk.RIGHT, padx=5)

            logger.debug("操作按钮区域创建完成")
        except Exception as e:
            logger.error(f"创建操作按钮区域时发生错误: {str(e)}", exc_info=True)
            messagebox.showerror("错误", f"创建操作按钮区域时发生错误: {str(e)}")

    def create_log_display_section(self):
        """创建日志显示区域"""
        try:
            logger.debug("创建日志显示区域")
            log_frame = ttk.LabelFrame(self.main_frame, text="运行日志", padding="10")
            log_frame.pack(fill=tk.X, pady=5)

            # 创建日志文本框
            self.log_text = tk.Text(log_frame, height=8, wrap=tk.WORD, state=tk.DISABLED)
            self.log_text.pack(fill=tk.X, expand=True)

            # 添加日志级别选择
            log_level_frame = ttk.Frame(log_frame)
            log_level_frame.pack(fill=tk.X, pady=2)

            self.log_level_var = tk.StringVar(value="INFO")
            log_level_label = ttk.Label(log_level_frame, text="日志级别:")
            log_level_label.pack(side=tk.LEFT)

            log_level_combo = ttk.Combobox(log_level_frame, textvariable=self.log_level_var,
                                           values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                                           state="readonly")
            log_level_combo.pack(side=tk.LEFT, padx=5)
            log_level_combo.bind("<<ComboboxSelected>>", self.update_log_display)

            # 清空日志按钮
            clear_log_button = ttk.Button(log_level_frame, text="清空日志", command=self.clear_log_display)
            clear_log_button.pack(side=tk.RIGHT)

            logger.debug("日志显示区域创建完成")
        except Exception as e:
            logger.error(f"创建日志显示区域时发生错误: {str(e)}", exc_info=True)
            messagebox.showerror("错误", f"创建日志显示区域时发生错误: {str(e)}")

    def browse_file(self, file_type):
        """浏览文件"""
        try:
            logger.info(f"浏览文件: {file_type}")
            file_path = filedialog.askopenfilename(filetypes=[("Excel files", "*.xlsx *.xls")])
            if file_path:
                self.file_paths[file_type] = file_path
                self.file_entries[file_type].config(state=tk.NORMAL)
                self.file_entries[file_type].delete(0, tk.END)
                self.file_entries[file_type].insert(0, file_path)
                self.file_entries[file_type].config(state="readonly")
                logger.info(f"选择文件: {file_type} -> {file_path}")

                # 添加到日志显示
                self.add_log_to_display(f"选择文件: {file_type} -> {os.path.basename(file_path)}", "INFO")
        except Exception as e:
            logger.error(f"浏览文件时发生错误: {str(e)}", exc_info=True)
            messagebox.showerror("错误", f"浏览文件时发生错误: {str(e)}")
            self.add_log_to_display(f"浏览文件时发生错误: {str(e)}", "ERROR")

    def drop_file(self, event, file_type):
        """拖放文件"""
        try:
            file_path = event.data.strip('{}')  # 移除拖放文件路径时可能出现的花括号
            logger.info(f"拖放文件: {file_type} -> {file_path}")

            if os.path.isfile(file_path) and file_path.endswith(('.xlsx', '.xls')):
                self.file_paths[file_type] = file_path
                self.file_entries[file_type].config(state=tk.NORMAL)
                self.file_entries[file_type].delete(0, tk.END)
                self.file_entries[file_type].insert(0, file_path)
                self.file_entries[file_type].config(state="readonly")
                logger.info(f"接受拖放文件: {file_type} -> {file_path}")

                # 添加到日志显示
                self.add_log_to_display(f"接受拖放文件: {file_type} -> {os.path.basename(file_path)}", "INFO")
            else:
                logger.warning(f"拒绝拖放文件: {file_path} 不是有效的Excel文件")
                self.add_log_to_display(f"拒绝拖放文件: {os.path.basename(file_path)} 不是有效的Excel文件", "WARNING")
        except Exception as e:
            logger.error(f"拖放文件时发生错误: {str(e)}", exc_info=True)
            messagebox.showerror("错误", f"拖放文件时发生错误: {str(e)}")
            self.add_log_to_display(f"拖放文件时发生错误: {str(e)}", "ERROR")

    def add_log_to_display(self, message, level="INFO"):
        """添加日志到显示区域"""
        try:
            current_time = datetime.datetime.now().strftime("%H:%M:%S")
            log_entry = f"[{current_time}] [{level}] {message}\n"

            # 获取当前选择的日志级别
            current_level = self.log_level_var.get()
            level_order = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}

            # 只显示等于或高于当前选择级别的日志
            if level_order.get(level, 1) >= level_order.get(current_level, 1):
                self.log_text.config(state=tk.NORMAL)
                self.log_text.insert(tk.END, log_entry)

                # 根据日志级别设置颜色
                start_index = self.log_text.index(f"end-2l linestart")
                end_index = self.log_text.index(f"end-1l lineend")

                if level == "ERROR" or level == "CRITICAL":
                    self.log_text.tag_add("error", start_index, end_index)
                elif level == "WARNING":
                    self.log_text.tag_add("warning", start_index, end_index)
                elif level == "DEBUG":
                    self.log_text.tag_add("debug", start_index, end_index)

                self.log_text.see(tk.END)
                self.log_text.config(state=tk.DISABLED)

            # 设置标签颜色
            self.log_text.tag_configure("error", foreground="red")
            self.log_text.tag_configure("warning", foreground="orange")
            self.log_text.tag_configure("debug", foreground="blue")
        except Exception as e:
            logger.error(f"添加日志到显示区域时发生错误: {str(e)}", exc_info=True)

    def update_log_display(self, event=None):
        """更新日志显示"""
        try:
            logger.debug("更新日志显示")
            self.clear_log_display()

            # 重新加载日志文件的最后几行
            log_dir = "logs"
            current_date = datetime.datetime.now().strftime("%Y%m%d")
            log_file = os.path.join(log_dir, f"financial_reconciliation_{current_date}.log")

            if os.path.exists(log_file):
                with open(log_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()

                    # 获取当前选择的日志级别
                    current_level = self.log_level_var.get()
                    level_order = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}

                    # 过滤并显示日志
                    for line in lines:
                        if " - " in line:
                            parts = line.split(" - ")
                            if len(parts) >= 3:
                                level = parts[2]
                                if level_order.get(level, 1) >= level_order.get(current_level, 1):
                                    self.log_text.config(state=tk.NORMAL)
                                    self.log_text.insert(tk.END, line)
                                    self.log_text.config(state=tk.DISABLED)

            self.log_text.see(tk.END)
        except Exception as e:
            logger.error(f"更新日志显示时发生错误: {str(e)}", exc_info=True)

    def clear_log_display(self):
        """清空日志显示"""
        try:
            self.log_text.config(state=tk.NORMAL)
            self.log_text.delete(1.0, tk.END)
            self.log_text.config(state=tk.DISABLED)
            logger.debug("日志显示已清空")
        except Exception as e:
            logger.error(f"清空日志显示时发生错误: {str(e)}", exc_info=True)

    def open_log_folder(self):
        """打开日志文件夹"""
        try:
            log_dir = os.path.abspath("logs")
            logger.info(f"打开日志文件夹: {log_dir}")

            if os.path.exists(log_dir):
                if os.name == 'nt':  # Windows
                    os.startfile(log_dir)
                else:  # macOS and Linux
                    import subprocess
                    subprocess.call(['open' if os.name == 'posix' else 'xdg-open', log_dir])
                self.add_log_to_display(f"打开日志文件夹: {log_dir}", "INFO")
            else:
                self.add_log_to_display(f"日志文件夹不存在: {log_dir}", "WARNING")
                logger.warning(f"日志文件夹不存在: {log_dir}")
        except Exception as e:
            logger.error(f"打开日志文件夹时发生错误: {str(e)}", exc_info=True)
            messagebox.showerror("错误", f"打开日志文件夹时发生错误: {str(e)}")
            self.add_log_to_display(f"打开日志文件夹时发生错误: {str(e)}", "ERROR")

    def calculate_difference(self, df):
        """
        根据往来台账余额方向和账务余额方向计算差额
        只看往来台账余额方向字段为贷的数据
        往来台账余额字段减去账务余额的值，即为差额
        金额字段空的话视为0
        账务余额的值需要根据账务余额方向判断，若为借，则金额取负数，否则为正数
        """
        try:
            logger.debug("开始计算差额")

            # 创建数据副本
            df_calc = df.copy()
            logger.debug(f"原始数据行数: {len(df_calc)}")

            # 只处理往来台账余额方向为贷的数据
            df_calc = df_calc[df_calc["往来台账余额方向"] == "贷"]
            logger.debug(f"往来台账余额方向为贷的数据行数: {len(df_calc)}")

            if df_calc.empty:
                logger.warning("没有找到往来台账余额方向为贷的数据，无法计算差额")
                self.add_log_to_display("没有找到往来台账余额方向为贷的数据，无法计算差额", "WARNING")
                return df

            # 处理空值，视为0
            df_calc["往来台账余额"] = pd.to_numeric(df_calc["往来台账余额"], errors='coerce').fillna(0)
            df_calc["账务余额"] = pd.to_numeric(df_calc["账务余额"], errors='coerce').fillna(0)

            # 根据账务余额方向调整账务余额值
            # 若为借，则金额取负数，否则为正数
            df_calc["调整后账务余额"] = df_calc.apply(
                lambda row: -row["账务余额"] if row["账务余额方向"] == "借" else row["账务余额"],
                axis=1
            )

            # 计算差额：往来台账余额 - 调整后账务余额
            df_calc["差额"] = df_calc["往来台账余额"] - df_calc["调整后账务余额"]

            logger.debug(f"计算出差额的数据行数: {len(df_calc)}")
            logger.debug(f"差额统计: {df_calc['差额'].describe()}")

            # 将计算结果合并回原数据框
            df = df.merge(
                df_calc[["科目", "往来单位", "差额"]],
                on=["科目", "往来单位"],
                how="left",
                suffixes=("", "_calc")
            )

            # 使用计算的差额（如果原差额为空）
            df["差额"] = df["差额"].fillna(df["差额_calc"])
            df = df.drop("差额_calc", axis=1)

            logger.debug("差额计算完成")
            self.add_log_to_display("差额计算完成", "INFO")

            return df
        except Exception as e:
            logger.error(f"计算差额时发生错误: {str(e)}", exc_info=True)
            self.add_log_to_display(f"计算差额时发生错误: {str(e)}", "ERROR")
            raise

    def load_files(self):
        """加载所有文件"""
        try:
            logger.info("开始加载文件")
            self.add_log_to_display("开始加载文件...", "INFO")

            # 验证文件路径是否有效
            for file_type, file_path in self.file_paths.items():
                if not os.path.exists(file_path):
                    logger.error(f"{file_type} 文件不存在: {file_path}")
                    self.add_log_to_display(f"{file_type} 文件不存在: {os.path.basename(file_path)}", "ERROR")
                    messagebox.showerror("错误", f"{file_type} 文件不存在:\n{file_path}")
                    return False

                if not os.path.isfile(file_path):
                    logger.error(f"{file_type} 不是一个有效的文件: {file_path}")
                    self.add_log_to_display(f"{file_type} 不是一个有效的文件: {os.path.basename(file_path)}", "ERROR")
                    messagebox.showerror("错误", f"{file_type} 不是一个有效的文件:\n{file_path}")
                    return False

                if not file_path.endswith(('.xlsx', '.xls')):
                    logger.error(f"{file_type} 不是Excel文件: {file_path}")
                    self.add_log_to_display(f"{file_type} 不是Excel文件: {os.path.basename(file_path)}", "ERROR")
                    messagebox.showerror("错误", f"{file_type} 不是Excel文件:\n{file_path}")
                    return False

            # 读取Excel文件
            for file_type, file_path in self.file_paths.items():
                logger.info(f"读取文件: {file_type} -> {file_path}")
                self.add_log_to_display(f"读取文件: {file_type} -> {os.path.basename(file_path)}", "INFO")

                df = pd.read_excel(file_path)
                logger.debug(f"读取 {file_type} 成功，数据行数: {len(df)}")
                logger.debug(f"列名: {', '.join(df.columns)}")

                # 处理期初往来数据与账务数据核对文件
                if file_type == "期初往来数据与账务数据核对文件":
                    # 确保必要的列存在
                    required_columns = ["科目", "往来单位", "问题类型", "往来款项性质"]
                    missing_columns = [col for col in required_columns if col not in df.columns]
                    if missing_columns:
                        logger.error(f"{file_type} 缺少必要的列: {', '.join(missing_columns)}")
                        self.add_log_to_display(f"{file_type} 缺少必要的列: {', '.join(missing_columns)}", "ERROR")
                        messagebox.showerror("错误", f"{file_type} 缺少必要的列:\n{', '.join(missing_columns)}")
                        return False

                    # 确保维度列存在
                    dimension_columns = ["项目", "采购订单", "产品服务", "职工"]
                    for col in dimension_columns:
                        if col not in df.columns:
                            df[col] = ""  # 如果不存在则添加空列
                            logger.warning(f"{file_type} 缺少 {col} 列，已添加空列")

                    # 计算差额（如果差额列为空）
                    # if "差额" not in df.columns or df["差额"].isna().all():
                    #     logger.info(f"{file_type} 中差额列为空，开始计算差额")
                    #     self.add_log_to_display(f"{file_type} 中差额列为空，开始计算差额", "INFO")
                    #
                    #     # 检查是否有计算差额所需的列
                    #     required_calc_columns = ["往来台账余额方向", "往来台账余额", "账务余额方向", "账务余额"]
                    #     missing_calc_columns = [col for col in required_calc_columns if col not in df.columns]
                    #     if missing_calc_columns:
                    #         logger.error(f"{file_type} 缺少计算差额所需的列: {', '.join(missing_calc_columns)}")
                    #         self.add_log_to_display(
                    #             f"{file_type} 缺少计算差额所需的列: {', '.join(missing_calc_columns)}", "ERROR")
                    #         messagebox.showerror("错误",
                    #                              f"{file_type} 缺少计算差额所需的列:\n{', '.join(missing_calc_columns)}")
                    #         return False
                    #
                    #     # 计算差额
                    #     df = self.calculate_difference(df)

                self.dataframes[file_type] = df

            # 显示文件加载成功信息
            logger.info(f"所有文件加载完成:\n"
                                                f"期初往来数据与账务数据核对文件: {len(self.dataframes['期初往来数据与账务数据核对文件'])} 行\n"
                                                f"往来台账查询文件: {len(self.dataframes['往来台账查询文件'])} 行\n"
                                                f"多维凭证明细文件: {len(self.dataframes['多维凭证明细文件'])} 行")

            # 添加到日志显示
            self.add_log_to_display("所有文件加载完成", "INFO")
            self.add_log_to_display(
                f"期初往来数据与账务数据核对文件: {len(self.dataframes['期初往来数据与账务数据核对文件'])} 行", "INFO")
            self.add_log_to_display(f"往来台账查询文件: {len(self.dataframes['往来台账查询文件'])} 行", "INFO")
            self.add_log_to_display(f"多维凭证明细文件: {len(self.dataframes['多维凭证明细文件'])} 行", "INFO")

            return True
        except Exception as e:
            logger.error(f"加载文件时发生错误: {str(e)}", exc_info=True)
            self.add_log_to_display(f"加载文件时发生错误: {str(e)}", "ERROR")
            messagebox.showerror("文件加载失败", f"加载文件时发生错误: {str(e)}")
            return False

    def start_reconciliation(self):
        """开始开始核对 - 规则1"""
        try:
            logger.info("开始核对（规则1）")
            self.add_log_to_display("开始核对（规则1）...", "INFO")

            # 记录开始时间
            start_time = datetime.datetime.now()

            # 检查是否选择了所有文件
            missing_files = [ft for ft, path in self.file_paths.items() if not path]
            if missing_files:
                logger.warning(f"缺少文件: {', '.join(missing_files)}")
                self.add_log_to_display(f"缺少文件: {', '.join(missing_files)}", "WARNING")
                messagebox.showerror("错误", f"请选择以下文件:\n{chr(10).join(missing_files)}")
                return

            # 加载文件
            if not self.load_files():
                return

            # 清空之前的结果
            self.reconciliation_results.clear()
            self.result_tree.delete(*self.result_tree.get_children())
            self.matching_results = []  # 清空匹配结果

            logger.info("开始执行规则1")
            self.add_log_to_display("开始执行规则1...", "INFO")

            # 执行规则1
            self.rule1()
            # self.rule5()
            # 记录结束时间
            end_time = datetime.datetime.now()
            elapsed_time = (end_time - start_time).total_seconds()

            # 显示结果统计
            logger.info(f"核对完成，共发现 {len(self.reconciliation_results)} 个问题，耗时 {elapsed_time:.2f} 秒")
            messagebox.showinfo("核对完成",
                                f"共发现 {len(self.reconciliation_results)} 个问题\n耗时: {elapsed_time:.2f} 秒")

            # 添加到日志显示
            self.add_log_to_display(f"核对完成，共发现 {len(self.reconciliation_results)} 个问题", "INFO")
            self.add_log_to_display(f"耗时: {elapsed_time:.2f} 秒", "INFO")

            # 启用导出按钮
            self.export_button.config(state=tk.NORMAL)
        except Exception as e:
            logger.error(f"执行核对时发生错误: {str(e)}", exc_info=True)
            self.add_log_to_display(f"执行核对时发生错误: {str(e)}", "ERROR")
            messagebox.showerror("核对失败", f"执行核对时发生错误: {str(e)}")

    def export_results(self):
        """导出结果 - 针对账务余额缺失场景处理往来台账页签数据"""
        try:
            if not self.reconciliation_results:
                logger.warning("没有可导出的结果")
                self.add_log_to_display("没有可导出的结果", "WARNING")
                messagebox.showwarning("警告", "没有可导出的结果")
                return

            logger.info("开始导出结果")
            self.add_log_to_display("开始导出结果...", "INFO")

            # 记录开始时间
            start_time = datetime.datetime.now()

            file_path = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel files", "*.xlsx")])
            if not file_path:
                logger.info("用户取消导出")
                self.add_log_to_display("用户取消导出", "INFO")
                return


            # 获取原始数据框的副本
            main_df = self.dataframes["期初往来数据与账务数据核对文件"].copy()
            ledger_df = self.dataframes["往来台账查询文件"].copy()
            voucher_df = self.dataframes["多维凭证明细文件"].copy()

            # 为主要数据框添加核对场景和核对结果列
            main_df["核对场景"] = ""
            main_df["核对结果"] = ""
            #存储需要处理的主数据的索引（核对结果为去往来业务信息补录的记录）
            target_indices = []
            target_dw_indexs = []
            if self.matching_results:
                logger.debug(f"为主要数据框添加核对场景和核对结果列，匹配结果数量: {len(self.matching_results)}")

                # 遍历匹配结果并更新主数据框
                for result in self.matching_results:
                    idx = result.get("索引")
                    check_result = result.get("核对结果", "")
                    if pd.notna(idx) and 0 <= idx < len(main_df):
                        main_df.at[idx, "核对场景"] = result.get("核对场景", "")
                        main_df.at[idx, "核对结果"] = check_result
                        main_df.at[idx, "问题编号"] = result.get("问题编号", "")
                        if check_result.startswith("去往来业务信息补录"):
                            target_indices.append(idx)
                        if check_result.startswith("多维明细凭证缺失维度"):
                            target_dw_indexs.append(idx)

            ledger_df["备注"] = ""
            ledger_df["问题编号"] = ""
            if target_indices:
                key_cols = ["科目", "往来单位", "往来款项性质","项目", "采购订单", "产品服务", "职工"]
                ledger_key_cols = ["科目", "往来单位", "往来款项性质","项目名称", "采购订单", "产品服务", "职工"]

                matched_ledger_rows = []
                for main_idx in target_indices:
                    main_row = main_df.iloc[main_idx]
                    match_conditions = True
                    for col in key_cols:
                        # 处理空值情况：如果主数据该字段为空，不参与匹配条件
                        if pd.notna(main_row[col]):
                            ledger_temp_col = ledger_key_cols[key_cols.index(col)]
                            match_conditions &= (ledger_df[ledger_temp_col] == main_row[col])

                    matched_ledgers = ledger_df[match_conditions]
                    # 准备处理后的往来台账数据
                    for _, ledger_row in matched_ledgers.iterrows():
                        # 复制行数据并添加备注（主数据行号=索引+2，因为表头占1行）
                        # new_row = ledger_row.copy()
                        # new_row["备注"] = f"序号1场景1，需要去往来业务补录信息补录，对应核对文件行号：{main_idx + 2}"
                        current_index = ledger_row.name
                        ledger_df.at[current_index, "备注"] = f"序号1场景1，需要去往来业务补录信息补录，对应核对文件行号：{main_idx + 2}"
                        ledger_df.at[current_index, "问题编号"] = main_row["问题编号"]

            #处理多维凭证明细，只输出匹配的数据
            if target_dw_indexs:
                key_cols = ["科目", "往来单位", "往来款项性质","项目", "采购订单", "产品服务", "职工"]
                dw_cols = ["科目名称", "往来单位", "往来款项性质", "项目库", "采购订单", "产品服务", "职工"]
                matched_dw_rows = []
                for main_idx in target_dw_indexs:
                    main_row = main_df.iloc[main_idx]
                    match_conditions = True
                    for col in key_cols:
                        # 处理空值情况：如果主数据该字段为空，不参与匹配条件
                        if pd.notna(main_row[col]):
                            dw_temp_col = dw_cols[key_cols.index(col)]
                            if col in ["科目", "往来单位", "往来款项性质", "项目库"]:
                                match_conditions &= (voucher_df[dw_temp_col].split(" ")[1] == (main_row[col]))
                            else :
                                match_conditions &= (voucher_df[dw_temp_col] == main_row[col])

                    matched_vouchers = voucher_df[match_conditions]
                    # 准备处理后的往来台账数据
                    for _, dw_row in matched_vouchers.iterrows():
                        # 复制行数据并添加备注（主数据行号=索引+2，因为表头占1行）
                        new_row = dw_row.copy()
                        new_row["备注"] = f"序号1场景1，多维明细凭证缺失维度，对应核对文件行号：{main_idx + 2}"
                        new_row["问题编号"] = main_row["问题编号"]
                        new_row["缺失维度"] = main_row["缺失维度"]
                        matched_dw_rows.append(new_row)

                    # 更新往来台账数据为匹配到的记录
                    if matched_dw_rows:
                        voucher_df = pd.DataFrame(matched_dw_rows)
                    else:
                        # 如果没有匹配到记录，保留空DataFrame
                        voucher_df = pd.DataFrame(columns=ledger_df.columns.tolist() + ["备注", "问题编号", "缺失维度"])
                else:
                    # 如果没有符合条件的主数据，保留空DataFrame并添加备注列
                    voucher_df = pd.DataFrame(columns=ledger_df.columns.tolist() + ["备注", "问题编号", "缺失维度"])

            # 保存到Excel文件，包含三个原始文件数据的页签
            with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
                # 保存主要数据（包含核对结果列）
                main_df.to_excel(writer, sheet_name="期初往来数据与账务数据核对", index=False)
                logger.debug("保存期初往来数据与账务数据核对完成")

                # 保存处理后的往来台账查询文件
                ledger_df.to_excel(writer, sheet_name="往来台账查询", index=False)
                logger.debug("保存往来台账查询完成")

                # 保存多维凭证明细文件
                voucher_df.to_excel(writer, sheet_name="多维凭证明细", index=False)
                logger.debug("保存多维凭证明细完成")





            # 记录结束时间
            end_time = datetime.datetime.now()
            elapsed_time = (end_time - start_time).total_seconds()

            logger.info(f"结果已导出到: {file_path}，耗时 {elapsed_time:.2f} 秒")
            messagebox.showinfo("导出成功", f"结果已导出到: {file_path}\n耗时: {elapsed_time:.2f} 秒")

            # 添加到日志显示
            self.add_log_to_display(f"结果已导出到: {os.path.basename(file_path)}", "INFO")
            self.add_log_to_display(f"耗时: {elapsed_time:.2f} 秒", "INFO")
        except Exception as e:
            logger.error(f"导出结果时发生错误: {str(e)}", exc_info=True)
            self.add_log_to_display(f"导出结果时发生错误: {str(e)}", "ERROR")
            messagebox.showerror("导出失败", str(e))
    def clear_all(self):
        """清空所有"""
        try:
            logger.info("清空所有数据")
            self.add_log_to_display("清空所有数据...", "INFO")

            # 清空文件选择
            for file_type in self.file_paths.keys():
                self.file_paths[file_type] = None
                self.file_entries[file_type].config(state=tk.NORMAL)
                self.file_entries[file_type].delete(0, tk.END)
                self.file_entries[file_type].config(state="readonly")

            # 清空结果
            self.reconciliation_results.clear()
            self.matching_results.clear()
            self.result_tree.delete(*self.result_tree.get_children())

            # 禁用导出按钮
            self.export_button.config(state=tk.DISABLED)

            logger.info("所有数据已清空")
            self.add_log_to_display("所有数据已清空", "INFO")
        except Exception as e:
            logger.error(f"清空数据时发生错误: {str(e)}", exc_info=True)
            self.add_log_to_display(f"清空数据时发生错误: {str(e)}", "ERROR")
            messagebox.showerror("错误", f"清空数据时发生错误: {str(e)}")


    def rule1(self):
        """
        序号1: 账务余额缺失匹配往来余额缺失
        1. 筛选问题类型为"账务余额缺失"的数据
        2. 对每条数据，按科目+往来单位筛选问题类型为"往来余额缺失"的数据
        3. 查找差额相同的记录（一对一匹配）
        4. 核对四个维度（项目、采购订单、产品服务、职工、往来款项性质）
           - 若双方都有同一维度但值不一致，则不匹配
           - 其他情况视为维度匹配
        5. 匹配成功，则两条数据都标记为"序号1场景1"
           - 若账务余额缺失数据行少了维度，则结果为"去往来业务信息补录"
           - 若往来余额缺失数据行少了维度，则结果为"多维明细凭证缺失维度"
        6. 未匹配的标记为"暂不处理"，结果为"暂不处理"
        """
        try:
            logger.info("开始执行规则1: 账务余额缺失匹配往来余额缺失")
            self.add_log_to_display("开始执行规则1: 账务余额缺失匹配往来余额缺失", "INFO")

            # 获取主数据框
            main_df = self.dataframes["期初往来数据与账务数据核对文件"].copy()

            # 检查必要列是否存在
            required_columns = ["科目", "往来单位", "往来款项性质", "问题类型", "差额",
                                "项目", "采购订单", "产品服务", "职工"]
            missing_columns = [col for col in required_columns if col not in main_df.columns]
            if missing_columns:
                logger.error(f"规则1执行失败，缺少必要列: {', '.join(missing_columns)}")
                self.add_log_to_display(f"规则1执行失败，缺少必要列: {', '.join(missing_columns)}", "ERROR")
                return

            # 初始化核对场景和结果列
            if "核对场景" not in main_df.columns:
                main_df["核对场景"] = "暂不处理"
            else:
                main_df["核对场景"] = main_df["核对场景"].fillna("暂不处理")

            if "核对结果" not in main_df.columns:
                main_df["核对结果"] = "暂不处理"
            else:
                main_df["核对结果"] = main_df["核对结果"].fillna("暂不处理")

            # 筛选问题类型为"账务余额缺失"的数据
            account_missing = main_df[main_df["问题类型"] == "账务余额缺失"].copy()
            logger.info(f"筛选出账务余额缺失数据 {len(account_missing)} 条")
            self.add_log_to_display(f"筛选出账务余额缺失数据 {len(account_missing)} 条", "INFO")

            # 筛选问题类型为"往来余额缺失"的数据
            ledger_missing = main_df[main_df["问题类型"] == "往来余额缺失"].copy()
            logger.info(f"筛选出往来余额缺失数据 {len(ledger_missing)} 条")
            self.add_log_to_display(f"筛选出往来余额缺失数据 {len(ledger_missing)} 条", "INFO")

            # 用于记录已匹配的往来余额缺失数据索引
            matched_ledger_indices = set()

            # 遍历每条账务余额缺失数据
            for idx, account_row in account_missing.iterrows():
                # 只处理未匹配的记录
                if idx in [m["索引"] for m in self.matching_results]:
                    continue

                # 获取匹配条件
                科目 = account_row["科目"]
                往来单位 = account_row["往来单位"]
                往来款项性质 = account_row["往来款项性质"]
                # 差额 = account_row["差额"]
                差额 = account_row.get("往来台账余额", 0)
                if account_row.get("往来台账余额方向") == '借':
                    差额 = 差额 * (-1)
                logger.debug(f"处理账务缺失记录: 科目={科目}, 往来单位={往来单位}, 差额={差额}")

                def calculate_adjusted_balance(row):
                    if row["账务余额方向"] == "借":
                        return row["账务余额"]
                    else:  # 贷方
                        return -row["账务余额"]

                ledger_missing = ledger_missing.copy()
                ledger_missing["调整后差额"] = ledger_missing.apply(calculate_adjusted_balance, axis=1)
                # 筛选符合条件的往来余额缺失数据
                mask = (
                        (ledger_missing["科目"] == 科目) &
                        (ledger_missing["往来单位"] == 往来单位) &
                        # (ledger_missing["往来款项性质"] == 往来款项性质) &
                        (np.isclose(ledger_missing["调整后差额"] + 差额, 0, atol=1e-6)) &
                        (~ledger_missing.index.isin(matched_ledger_indices))
                )

                matching_ledgers = ledger_missing[mask]

                # 检查是否有匹配的记录
                if len(matching_ledgers) > 0:
                    # 取第一条作为匹配记录（一对一匹配）
                    ledger_row = matching_ledgers.iloc[0]
                    ledger_idx = ledger_row.name
                    matched_ledger_indices.add(ledger_idx)

                    # 核对维度
                    dimensions = ["项目", "采购订单", "产品服务", "职工", "往来款项性质"]
                    dimension_match = True
                    # 记录双方缺失的维度
                    account_missing_dims = []
                    ledger_missing_dims = []

                    for dim in dimensions:
                        account_val = account_row[dim]
                        ledger_val = ledger_row[dim]

                        # 双方都有值但不相等，则维度不匹配
                        if pd.notna(account_val) and pd.notna(ledger_val) and account_val != ledger_val:
                            dimension_match = False
                            logger.debug(f"维度不匹配: {dim} (账务: {account_val}, 往来: {ledger_val})")
                            break
                        # 账务有值，往来无值 → 往来缺失该维度
                        elif pd.notna(account_val) and pd.isna(ledger_val):
                            ledger_missing_dims.append(dim)
                        # 往来有值，账务无值 → 账务缺失该维度
                        elif pd.isna(account_val) and pd.notna(ledger_val):
                            account_missing_dims.append(dim)

                    if dimension_match:
                        # 确定核对结果
                        if len(account_missing_dims) > 0:
                            account_result = f"去往来业务信息补录: {', '.join(account_missing_dims)}"
                        else:
                            account_result = ""
                        if len(ledger_missing_dims) > 0:
                            ledger_result = f"多维明细凭证缺失维度: {', '.join(ledger_missing_dims)}"
                        else:
                            ledger_result = ""

                        # 维度匹配，标记为序号1场景1
                        issue_id = f"R1-{len(self.reconciliation_results) + 1}"
                        result = {
                            "问题编号": issue_id,
                            "问题描述": "账务余额缺失与往来余额缺失匹配",
                            "涉及科目": 科目,
                            "涉及往来单位": 往来单位,
                            "差额": 差额,
                            "状态": "匹配成功",
                            "详细信息": (f"账务缺失记录索引: {idx}\n"
                                         f"往来缺失记录索引: {ledger_idx}\n"
                                         f"往来款项性质: {往来款项性质}\n"
                                         f"账务缺失维度数: {account_missing_dims}\n"
                                         f"往来缺失维度数: {ledger_missing_dims}")
                        }

                        self.reconciliation_results.append(result)
                        self.result_tree.insert("", tk.END, values=(
                            issue_id,
                            "账务余额缺失与往来余额缺失匹配",
                            科目,
                            往来单位,
                            差额,
                            "匹配成功"
                        ))

                        # 更新主数据框的核对场景和结果
                        main_df.at[idx, "核对场景"] = "序号1场景1"
                        main_df.at[idx, "核对结果"] = account_result
                        main_df.at[ledger_idx, "核对场景"] = "序号1场景1"
                        main_df.at[ledger_idx, "核对结果"] = ledger_result

                        # 记录匹配结果
                        self.matching_results.append({
                            "问题编号": issue_id,
                            "索引": idx,
                            "核对场景": "序号1场景1",
                            "核对结果": account_result,
                            "缺失维度": ', '.join(account_missing_dims)
                        })
                        self.matching_results.append({
                            "问题编号": issue_id,
                            "索引": ledger_idx,
                            "核对场景": "序号1场景1",
                            "核对结果": ledger_result,
                            "缺失维度": ', '.join(ledger_missing_dims)

                        })

                        logger.info(f"找到匹配记录: 账务索引={idx}, 往来索引={ledger_idx}")
                        self.add_log_to_display(f"找到匹配记录: 科目={科目}, 往来单位={往来单位}", "INFO")

            # 更新主数据框
            self.dataframes["期初往来数据与账务数据核对文件"] = main_df

            # 记录未匹配的记录
            unmatched = main_df[
                (main_df["问题类型"].isin(["账务余额缺失", "往来余额缺失"])) &
                (main_df["核对场景"] == "暂不处理")
                ]

            logger.info(f"规则1执行完成，找到 {len(self.reconciliation_results)} 个匹配项，未匹配 {len(unmatched)} 个")
            self.add_log_to_display(
                f"规则1执行完成，找到 {len(self.reconciliation_results)} 个匹配项，未匹配 {len(unmatched)} 个",
                "INFO"
            )

        except Exception as e:
            logger.error(f"执行规则1时发生错误: {str(e)}", exc_info=True)
            self.add_log_to_display(f"执行规则1时发生错误: {str(e)}", "ERROR")
            raise

    def rule5(self):
        """规则5:同一科目下同一往来单位名称但ID不一致"""
        try:
            logger.info("开始检查规则5：同一科目下同一往来单位名称但ID不一致")
            df = self.dataframes["期初往来数据与账务数据核对文件"].copy()
            # df = df[df["核对场景"] == "暂不处理"].copy()
            # 按科目和往来单位进行分组,检查每组的往来单位ID是否唯一
            grouped = df.groupby(["科目", "往来单位"])['往来单位ID'].nunique()
            #筛选ID不唯一的组
            problematic_groups = grouped[grouped > 1].reset_index()
            for _,row in problematic_groups.iterrows():
                subject = row['科目']
                unit_name = row['往来单位']

                condition = (df['科目'] == subject) & (df['往来单位'] == unit_name)
                problematic_rows = df[condition]
                ids = problematic_rows['往来单位ID'].unique()
                # 获取该科目下该往来单位的所有记录
                # ids = df[(df['科目'] == subject) & (df['往来单位'] == unit_name)]['往来单位ID'].unique()

                self.reconciliation_results.append({
                    "问题编号": f"R5-{len(self.reconciliation_results) + 1}",
                    "问题描述": "同一科目下同一往来单位名称但ID不一致",
                    "涉及科目": subject,
                    "涉及往来单位": unit_name,
                    "差额": "",
                    "状态": "匹配成功",
                    "详细信息": f"该科目下该往来单位存在不同ID：: {', '.join(ids)}"
                })
            logger.info(f"规则5执行完成，找到 {len(problematic_groups)} 个问题")
            self.add_log_to_display(f"规则5执行完成，找到 {len(problematic_groups)} 个问题", "INFO")
        except Exception as e:
            logger.error(f"执行规则5时发生错误: {str(e)}", exc_info=True)
            self.add_log_to_display(f"执行规则5时发生错误: {str(e)}", "ERROR")



if __name__ == "__main__":
    root = TkinterDnD.Tk()
    app = FinancialDataReconciliationApp(root)
    root.mainloop()