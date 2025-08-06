import sys
import pandas as pd
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication, QWidget, QPushButton, QFileDialog, QLabel, QVBoxLayout, QHBoxLayout, QPlainTextEdit

class ExcelComparer(QWidget):
    def __init__(self):
        super().__init__()
        #self.setWindowIcon(QIcon('path/to/your_icon.ico'))  # 添加这一行
        self.file1 = ""
        self.file2 = ""
        self.initUI()

    def initUI(self):
        self.setWindowTitle("Excel文件比较工具")
        self.resize(800, 600)

        # 主布局
        main_layout = QVBoxLayout()

        # 上部布局：左右结构
        top_layout = QHBoxLayout()

        # 左侧文件选择区域
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

        # 右侧按钮区域
        button_layout = QVBoxLayout()
        self.compare_btn = QPushButton("比较文件")
        self.compare_btn.setFixedWidth(150)
        self.compare_btn.clicked.connect(self.compare_files)
        button_layout.addStretch()
        button_layout.addWidget(self.compare_btn)
        button_layout.addStretch()

        # 将左右部分加入上部布局
        top_layout.addLayout(file_select_layout, 2)
        top_layout.addLayout(button_layout, 1)

        # 日志显示区域
        self.log_area = QPlainTextEdit()
        self.log_area.setReadOnly(True)
        self.log_area.setStyleSheet("background-color: #f0f0f0;")

        # 添加到主布局
        main_layout.addLayout(top_layout)
        main_layout.addWidget(self.log_area)

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

        try:
            df1 = pd.read_excel(self.file1, sheet_name="附表1资产卡片期初数据收集模板",
                                skiprows=lambda x: x in [0, 1, 2])
            df2 = pd.read_excel(self.file2, sheet_name="附表1资产卡片期初数据收集模板",
                                skiprows=lambda x: x in [0, 1, 2])
        except ValueError:
            self.log("发生错误：指定的页签不存在，请确认页签名是否正确！")
            return
        # 确保两个 DataFrame 的结构一致
        if not df1.columns.equals(df2.columns):
            self.log("错误：两个文件的列不一致，请检查列名或顺序是否相同！")
            return

        if len(df1) != len(df2):
            self.log(f"提示：两个文件的行数不一致（文件1有 {len(df1)} 行，文件2有 {len(df2)} 行）")
        # 比较差异
            # 2. 确保“资产编码”列存在
            if '资产编码' not in df1.columns:
                self.log("错误：列中缺少【资产编码】，请检查文件结构！")
                return

            # 3. 设置资产编码为索引（便于后续对比）
            df1.set_index('资产编码', inplace=True)
            df2.set_index('资产编码', inplace=True)

            # 4. 找出文件2中缺失的资产编码（文件1有，文件2无）
            missing_in_file2 = df1.index.difference(df2.index)
            if not missing_in_file2.empty:
                self.log(f"【文件2中缺失的资产编码】（共 {len(missing_in_file2)} 条）：")
                for code in missing_in_file2:
                    self.log(f" - {code}")

            # 5. 只保留两个文件都存在的资产编码
            common_codes = df1.index.intersection(df2.index)
            if common_codes.empty:
                self.log("警告：两个文件中没有共同的资产编码！")
                return

            df1_common = df1.loc[common_codes]
            df2_common = df2.loc[common_codes]
            # 6. 使用 compare 方法比较每一列的差异
            comparison = df1_common.compare(df2_common, align_axis=1)

            if comparison.empty:
                self.log("【共同资产编码的数据完全一致】，没有差异。")
            else:
                self.log(f"【存在差异的列】（共 {len(comparison)} 行）：")
                for asset_code, group in comparison.groupby(level=0):
                    self.log(f"\n资产编码：{asset_code}")
                    for (col, _), row in group.iterrows():
                        val1 = df1_common.loc[asset_code, col]
                        val2 = df2_common.loc[asset_code, col]
                        self.log(f" - 列 [{col}] 不一致：文件1={val1}, 文件2={val2}")
        # df1_aligned, df2_aligned = df1.align(df2, fill_value=None)
        # comparison_df = df1.compare(df2, align_axis=0)
        # if comparison_df.empty:
        #     self.log("两个 Excel 文件内容一致，没有差异。")
        # else:
        #     self.log(f"找到差异，共有 {len(comparison_df)} 行不同。")
        #     self.log("差异内容如下：")
        #     self.log(comparison_df.to_string())


    def log(self, message):
        self.log_area.appendPlainText(message)

# ✅ 主程序入口
if __name__ == "__main__":
    import sys
    app = QApplication(sys.argv)
    ex = ExcelComparer()
    ex.show()
    sys.exit(app.exec_())
