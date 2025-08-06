import sys
import pandas as pd
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QFileDialog, QLabel, QVBoxLayout, QHBoxLayout, \
    QPlainTextEdit, QProgressBar
from PyQt5.QtCore import QThread, pyqtSignal, Qt
from openpyxl import load_workbook


def read_excel_fast(file_path, sheet_name, skip_rows=3):
    """使用 openpyxl 快速读取 Excel，跳过指定行数"""
    wb = load_workbook(filename=file_path, read_only=True, data_only=True)
    ws = wb[sheet_name]

    data = []
    columns = None
    for i, row in enumerate(ws.rows):
        if i == 0:
            columns = [cell.value for cell in row]
        elif i < skip_rows:
            continue
        else:
            data.append([cell.value for cell in row])
    return pd.DataFrame(data, columns=columns)


class CompareWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int)  # ✅ 新增：用于更新进度条
    def __init__(self, file1, file2):
        super().__init__()
        self.file1 = file1
        self.file2 = file2
        self.missing_assets = []
        self.diff_records = []
        self.summary = {}
        self.missing_rows = []  # 存储文件2中缺失的资产编码对应的文件1整行数据
        self.diff_full_rows = []  # 存储列不一致的文件1和文件2整行数据

    def run(self):
        try:
            df1 = read_excel_fast(self.file1, "附表1资产卡片期初数据收集模板")
            df2 = read_excel_fast(self.file2, "附表1资产卡片期初数据收集模板")
        except KeyError:
            self.log_signal.emit("发生错误：指定的页签不存在，请确认页签名是否正确！")
            return
        except Exception as e:
            self.log_signal.emit(f"发生未知错误：{str(e)}")
            return

        df1.columns = df1.columns.str.replace('[*\\s]', '', regex=True)
        df2.columns = df2.columns.str.replace('[*\\s]', '', regex=True)

        if not df1.columns.equals(df2.columns):
            self.log_signal.emit("错误：两个文件的列不一致，请检查列名或顺序是否相同！")
            return

        if '资产编码' not in df1.columns:
            self.log_signal.emit("错误：列中缺少【资产编码】，请检查文件结构！")
            return

        df1.set_index('资产编码', inplace=True)
        df2.set_index('资产编码', inplace=True)

        # 提示行数不一致
        if len(df1) != len(df2):
            self.log_signal.emit(f"提示：两个文件的行数不一致（文件1有 {len(df1)} 行，文件2有 {len(df2)} 行）")

        # 找出缺失的资产编码
        missing_in_file2 = df1.index.difference(df2.index)
        if not missing_in_file2.empty:
            self.missing_assets = missing_in_file2.tolist()
            self.missing_rows = df1.loc[missing_in_file2].reset_index().to_dict(orient='records')
            missing_list = "\n".join([f" - {code}" for code in missing_in_file2])
            self.log_signal.emit(f"【文件2中缺失的资产编码】（共 {len(missing_in_file2)} 条）：\n{missing_list}")

        # 找出共同资产编码
        common_codes = df1.index.intersection(df2.index)
        if common_codes.empty:
            self.log_signal.emit("警告：两个文件中没有共同的资产编码！")
            return

        df1_common = df1.loc[common_codes]
        df2_common = df2.loc[common_codes]

        # ✅ 向量化比对，避免逐行处理
        # comparison = df1_common.fillna("").compare(df2_common.fillna(""), align_axis=0)
        # comparison = comparison.swaplevel(0, 1).sort_index()
        df1_compare = df1_common.astype(str).replace('nan', '')
        df2_compare = df2_common.astype(str).replace('nan', '')
        comparison = df1_compare.compare(df2_compare, align_axis=0)
        # ✅ 限制 comparison 的索引为 df1_common 和 df2_common 的交集
        valid_index = df1_common.index.intersection(df2_common.index)
        comparison = comparison.loc[valid_index]
        # 提取差异数据
        self.diff_full_rows = []
        diff_count = 0
        diff_log_messages = []  # ✅ 用于存储差异日志

        mask = (df1_compare == df2_compare).all(axis=1)
        diff_rows = df1_compare[~mask]

        for asset_code, row in diff_rows.iterrows():
            diff_details = []
            for col in df1_compare.columns:
                val1 = df1_compare.loc[asset_code, col]
                val2 = df2_compare.loc[asset_code, col]
                if val1 != val2:
                    diff_details.append(f" - 列 [{col}] 不一致：文件1={val1}, 文件2={val2}")
                    self.diff_records.append({
                        "资产编码": asset_code,
                        "列名": col,
                        "文件1值": val1,
                        "文件2值": val2
                    })

            if diff_details:
                diff_log_messages.append(f"\n资产编码：{asset_code}")
                diff_log_messages.extend(diff_details)
                self.diff_full_rows.append({
                    "source": df1_common.loc[asset_code].to_dict(),
                    "target": df2_common.loc[asset_code].to_dict()
                })
                diff_count += 1
                if diff_count % 1000 == 0 or diff_count == len(comparison):
                    self.progress_signal.emit(int(diff_count / len(comparison) * 100))
            else:
                self.log_signal.emit(f"⚠️ 资产编码 {asset_code} 不在原始数据中，跳过。")

        equal_count = len(common_codes) - diff_count

        self.summary = {
            "total_file1": len(df1),
            "total_file2": len(df2),
            "missing_count": len(self.missing_assets),
            "common_count": len(common_codes),
            "diff_count": diff_count,
            "equal_count": equal_count,
            "diff_ratio": diff_count / len(common_codes) if len(common_codes) > 0 else 0
        }

        if diff_count == 0:
            self.log_signal.emit("【共同资产编码的数据完全一致】，没有差异。")
        else:
            self.log_signal.emit(f"【存在差异的列】（共 {diff_count} 行）：")
            if diff_log_messages:
                self.log_signal.emit('\n'.join(diff_log_messages))
            else:
                self.log_signal.emit("⚠️ 未找到具体差异列，请检查数据是否一致。")
            # # ✅ 可选：输出部分差异数据
            # self.log_signal.emit("仅展示部分差异数据，完整数据请导出查看。")



from PyQt5.QtWidgets import QTabWidget, QWidget, QVBoxLayout, QPushButton
class ExcelComparer(QWidget):
    def __init__(self):
        super().__init__()
        self.file1 = ""
        self.file2 = ""
        self.initUI()
        self.worker = None
        self.summary_data = {}
    def initUI(self):
        self.setWindowTitle("Excel文件比较工具")
        self.resize(1000, 700)

        main_layout = QVBoxLayout()

        top_layout = QHBoxLayout()

        file_select_layout = QVBoxLayout()
        self.label1 = QLabel("未选择文件1")
        self.btn1 = QPushButton("选择文件 1")
        self.btn1.clicked.connect(self.select_file1)

        self.label2 = QLabel("未选择文件2")
        self.btn2 = QPushButton("选择文件 2")
        self.btn2.clicked.connect(self.select_file2)

        file_select_layout.addWidget(self.label1)
        file_select_layout.addWidget(self.btn1)
        file_select_layout.addWidget(self.label2)
        file_select_layout.addWidget(self.btn2)

        button_layout = QVBoxLayout()
        self.compare_btn = QPushButton("比较文件")
        self.compare_btn.setFixedWidth(150)
        self.compare_btn.clicked.connect(self.compare_files)

        self.export_btn = QPushButton("导出报告")
        self.export_btn.setFixedWidth(150)
        self.export_btn.setEnabled(False)
        self.export_btn.clicked.connect(self.export_report)

        button_layout.addStretch()
        button_layout.addWidget(self.compare_btn)
        button_layout.addWidget(self.export_btn)
        button_layout.addStretch()

        top_layout.addLayout(file_select_layout, 2)
        top_layout.addLayout(button_layout, 1)

        self.tab_widget = QTabWidget()
        self.log_area = QPlainTextEdit()
        self.log_area.setReadOnly(True)
        self.log_area.setStyleSheet("background-color: #f0f0f0;")

        self.summary_area = QPlainTextEdit()
        self.summary_area.setReadOnly(True)
        self.summary_area.setStyleSheet("background-color: #f0f0f0;")

        self.tab_widget.addTab(self.log_area, "比对日志")
        self.tab_widget.addTab(self.summary_area, "汇总报告")

        # ✅ 新增进度条
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        self.progress_bar.setFixedHeight(20)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setAlignment(Qt.AlignCenter)



        main_layout.addLayout(top_layout)
        main_layout.addWidget(self.tab_widget)
        main_layout.addWidget(self.progress_bar)  # ✅ 添加进度条

        self.setLayout(main_layout)

    def select_file1(self):
        file, _ = QFileDialog.getOpenFileName(self, "选择 Excel 文件", "", "Excel 文件 (*.xlsx *.xls)")
        if file:
            self.file1 = file
            self.label1.setText(f"文件1: {file}")

    def select_file2(self):
        file, _ = QFileDialog.getOpenFileName(self, "选择 Excel 文件", "", "Excel 文件 (*.xlsx *.xls)")
        if file:
            self.file2 = file
            self.label2.setText(f"文件2: {file}")

    def compare_files(self):
        if not self.file1 or not self.file2:
            self.log("请先选择两个 Excel 文件！")
            return

        self.log_area.clear()
        self.summary_area.clear()
        self.export_btn.setEnabled(False)
        self.worker = CompareWorker(self.file1, self.file2)
        self.worker.log_signal.connect(self.log)
        self.worker.progress_signal.connect(self.update_progress)  # ✅ 连接进度信号
        self.worker.finished.connect(self.on_compare_finished)
        self.worker.start()

    def update_progress(self, value):
        self.progress_bar.setValue(value)
        if value == 100:
            self.log("✅ 比对完成，可以导出结果。")
    def on_compare_finished(self):
        if hasattr(self.worker, 'summary'):
            self.summary_data = self.worker.summary
            summary_text = (
                f"📊 比对汇总报告\n"
                f"--------------------------------\n"
                f"• 总资产编码数量（文件1）：{self.summary_data['total_file1']}\n"
                f"• 总资产编码数量（文件2）：{self.summary_data['total_file2']}\n"
                f"• 文件2中缺失的资产编码：{self.summary_data['missing_count']}\n"
                f"• 共同资产编码数量：{self.summary_data['common_count']}\n"
                f"• 列不一致的资产编码数量：{self.summary_data['diff_count']}\n"
                f"• 列一致的资产编码数量：{self.summary_data['equal_count']}\n"
                f"--------------------------------\n"
                f"• 差异数据占比：{self.summary_data['diff_ratio']:.2%}\n"
            )
            self.summary_area.setPlainText(summary_text)
            self.export_btn.setEnabled(True)

    def export_report(self):
        if not hasattr(self, 'worker') or not hasattr(self.worker, 'missing_assets') or not hasattr(self.worker,
                                                                                                    'diff_records'):
            self.log("没有可导出的数据，请先执行比对！")
            return

        from PyQt5.QtWidgets import QFileDialog
        directory = QFileDialog.getExistingDirectory(self, "选择保存路径")
        if not directory:
            self.log("导出已取消。")
            return

        import pandas as pd

        # 导出缺失资产编码
        if self.worker.missing_rows:
            pd.DataFrame(self.worker.missing_rows).to_excel(f"{directory}/文件2中缺失的资产数据.xlsx", index=False)
            self.log("✅ 已导出：缺失资产编码.xlsx")

        # 导出列不一致数据
        if self.worker.diff_full_rows:
            combined = []
            for item in self.worker.diff_full_rows:
                combined.append({
                    **{f"文件1_{k}": v for k, v in item["source"].items()},
                    **{f"文件2_{k}": v for k, v in item["target"].items()}
                })

            pd.DataFrame(combined).to_excel(f"{directory}/列不一致的完整资产数据.xlsx", index=False)
            self.log("✅ 已导出：列不一致的完整资产数据.xlsx")

            # pd.DataFrame(self.worker.diff_records) \
            #     .to_excel(f"{directory}/列不一致数据.xlsx", index=False)
            # self.log("✅ 已导出：列不一致数据.xlsx")

        self.log("📊 比对报告导出完成！")

    def log(self, message):
        self.log_area.appendPlainText(message)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    ex = ExcelComparer()
    ex.show()
    sys.exit(app.exec_())
