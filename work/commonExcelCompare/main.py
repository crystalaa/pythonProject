import sys
import traceback
import logging
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QApplication
from gui import ExcelComparer
from utils import resource_path

# 配置日志记录器
logging.basicConfig(
    filename="error_log.txt",
    level=logging.ERROR,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def exception_hook(exc_type, exc_value, exc_traceback):
    """全局异常钩子，防止崩溃"""
    try:
        ex = QApplication.instance().topLevelWidgets()[0]
        if hasattr(ex, "log"):
            error_message = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            logging.error(error_message)
            ex.log(f"❌ 发生异常：{exc_value}")
        else:
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
    except:
        sys.__excepthook__(exc_type, exc_value, exc_traceback)


if __name__ == "__main__":
    sys.excepthook = exception_hook

    app = QApplication(sys.argv)
    icon_path = resource_path('icon.ico')
    app.setWindowIcon(QIcon(icon_path))
    ex = ExcelComparer()
    ex.show()
    exit_code = app.exec_()

    sys.exit(exit_code)    