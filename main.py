# 这是一个示例 Python 脚本。

# coding=utf-8
import sys, sqlite3, os
from ui.mainWindow import Ui_MainWindow
from PyQt5 import QtWidgets, QtCore
from PyQt5.QtWidgets import QFileDialog, QMessageBox
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time

import requests


including_subs = True  # 是否处理子文件
db3_extension_name = 'db'  # 配合待合并的sqlite3数据库扩展名
global n
n = 0
global db3list  # 便于自定义函数中对数据库列表信息的引用和输出
db3list = []
global i  # 配合db3list便于自定义函数中对数据库列表信息的引用和输出
i = 0


def append_data(con_current_data, con_des_data, tables_current_data):
    # 把当前数据库的链接、目标数据库的链接、当前数据库的table列表同时传入
    print('\n' + db3list[i] + ' is Beginning !')
    m = 0
    cur_current_data = con_current_data.cursor()
    cur_des_data = con_des_data.cursor()
    while m < len(tables_current_data):
        if str(tables_current_data[m])[2:-3] != "sqlite_sequence":
            sql = "select * from " + str(tables_current_data[m])[2:-3]
            cur_current_data.execute(sql)
            temp_data_list = cur_current_data.fetchall()
            temp_sql = ""  # 用来存储插入记录信息，一次只能插入一个一维数组
            n = 0
            if len(temp_data_list) > 0:
                while n < len(temp_data_list[0]):  # 注意此处求取的长度是指二维数组有多少列，有它来决定？的个数
                    temp_sql += "?,"
                    n += 1
                temp_sql = "(" + temp_sql[0:-1] + ")"  # 对循环后形成的"？,"列阵做修饰，去除尾部逗号，并加上括号来适配sql语句
                cur_des_data.executemany(
                    "insert or replace into " + str(tables_current_data[m])[2:-3] + " values " + temp_sql,
                    temp_data_list)
                con_des_data.commit()
            print('\n' + db3list[i] + "-----" + str(tables_current_data[m]) + "   Finished!")
            m += 1
        else:
            m += 1
    print('\n' + db3list[i] + " All Tables Finished!")


def sql_modify(table_info_unmod):
    k = 0
    table_info_modified = "("
    # 建立新表的execute中的sql语句为 CREATE TABLE XX (字段1 类型 ,字段2 类型)
    # table_info_modified在这里将被构建成为(字段1 类型 ,字段2 类型)内容
    # PRAGMA table_info返回的信息中，这里只需要使用字段名和字段类型
    # 这两个信息存在于第二和第三位
    while k < len(table_info_unmod):
        table_info_modified = table_info_modified + table_info_unmod[k][1] + " " + table_info_unmod[k][2] + ","
        k += 1
    table_info_modified = table_info_modified[0:-1] + ")"  # 最后去掉尾部多余的逗号，并加上括号
    return (table_info_modified)


def compare_tables(tables_cur_db3, tables_des_db3, con_current_db3, con_des_db3):
    j = 0
    while j < len(tables_cur_db3):
        if (not tables_cur_db3[j] in tables_des_db3) and (str(tables_cur_db3[j])[2:-3] != 'sqlite_sequence'):
            con_current_db3_cursor = con_current_db3.cursor()
            con_current_db3_cursor.execute("PRAGMA table_info (" + str(tables_cur_db3[j])[2:-3] + ")")
            # PRAGMA table_info方法可以获取表格的字段信息，但返回值为一个列表不能直接作为sql语句
            table_info_modified = sql_modify(con_current_db3_cursor.fetchall())
            # sql_modify函数实现PRAGMA table_info得到的列表信息转换为合格的sql信息
            con_des_db3_cursor = con_des_db3.cursor()
            new_table_sql = "create table " + str(tables_cur_db3[j])[2:-3] + table_info_modified
            # 配合返回后的table_info_modified，构成了完整的用于建立工作表的sql语句
            con_des_db3_cursor.execute(new_table_sql)  # 新建table的操作貌似可以不用commit
            j += 1
        else:
            j += 1
    con_des_db3_cursor = con_des_db3.cursor()
    con_des_db3_cursor.execute(sql_inqury_tables)  # 更新目标数据库的table列表
    tables_des = con_des_db3_cursor.fetchall()
    con_des_db3.commit()


def sub_data(con_current_data, tables_current_data, datatable):
    # 把当前数据库的链接、目标数据库的链接、当前数据库的table列表同时传入
    print('\n' + db3list[i] + ' is Beginning !')
    sql = "select * from" + datatable
    m = 0
    cur_current_data = con_current_data.cursor()
    while m < len(tables_current_data):
        # if str(tables_current_data[m])[2:-3] != "sqlite_sequence" and str(tables_current_data[m])[2:-3] != "softwareversionmanage" and str(tables_current_data[m])[2:-3] != "XB" and str(tables_current_data[m])[2:-3] != "XINGB" and str(tables_current_data[m])[2:-3] != "XLLX" and str(tables_current_data[m])[2:-3] != "YYZHT" and not "SYS" in str(tables_current_data[m])[2:-3] and not "T_" in str(tables_current_data[m])[2:-3]:
        if str(tables_current_data[m])[2:-3] == datatable:
            cur_current_data.execute(sql)
            temp_data_list = cur_current_data.fetchall()
            cur_des_data.execute("select * from " + str(tables_current_data[m])[2:-3])
            return len(temp_data_list)
            m += 1
        else:
            m += 1
    print('\n' + db3list[i] + " All Tables Finished!")

class mywindow(QtWidgets.QMainWindow, Ui_MainWindow):
    def  __init__ (self):
        QtCore.QCoreApplication.setAttribute(QtCore.Qt.AA_EnableHighDpiScaling)
        super(mywindow, self).__init__()
        self.setupUi(self)
        self.pushButton.clicked.connect(self.write_folder)
        self.pushButton_2.clicked.connect(self.read_file)
        self.pushButton_4.clicked.connect(self.process)
        self.pushButton_3.clicked.connect(self.delete_file)

    def read_file(self):
        #选取文件
        foldername1 =QFileDialog.getExistingDirectory(self, "选取文件夹", "./")
        print(foldername1)
        self.textEdit_2.setText(foldername1)

    def write_folder(self):
        #选取文件夹
        foldername = QFileDialog.getExistingDirectory(self, "选取文件夹", "./")
        print(foldername)
        self.textEdit.setText(foldername)

    def delete_file(self):
        self.textEdit.setText('')
        self.textEdit_2.setText('')

    # 进行处理
    def process(self):
        try:
            self.pushButton.setEnabled(False)
            self.pushButton_2.setEnabled(False)
            self.pushButton_3.setEnabled(False)
            self.pushButton_4.setEnabled(False)
            self.progressBar.setValue(0)
            time.sleep(1)
            #获取文件路径
            file_path = self.textEdit.toPlainText()
            if len(file_path) == 0:
                msg_box = QMessageBox(QMessageBox.Warning, '提示', '请先选择需要合并的文件所在的文件夹!')
                msg_box.exec_()
                self.pushButton.setEnabled(True)
                self.pushButton_2.setEnabled(True)
                self.pushButton_3.setEnabled(True)
                self.pushButton_4.setEnabled(True)
                return
            self.progressBar.setValue(20)
            time.sleep(1)
            #获取文件夹路径
            folder_path = self.textEdit_2.toPlainText()
            if len(folder_path) == 0:
                msg_box = QMessageBox(QMessageBox.Warning, '提示', '请先选择导出目标文件夹!')
                msg_box.exec_()
                self.pushButton.setEnabled(True)
                self.pushButton_2.setEnabled(True)
                self.pushButton_3.setEnabled(True)
                self.pushButton_4.setEnabled(True)
                return
            self.progressBar.setValue(40)
            time.sleep(1)
        # 通过 target=函数名 的方式定义子线程
            pool = ThreadPoolExecutor(2)
            self.progressBar.setValue(60)
            time.sleep(1)
            pool.submit(dbReader, self, file_path, folder_path).add_done_callback(self.updateUi)
            self.progressBar.setValue(80)
            # 中间可以进行对文件的任意操作
        except:
            fail_result = r'合并失败！'
            self.pushButton.setEnabled(True)
            self.pushButton_2.setEnabled(True)
            self.pushButton_3.setEnabled(True)
            self.pushButton_4.setEnabled(True)
            self.label_3.setText(fail_result)

    def updateUi(self, res):
        self.progressBar.setValue(100)
        success_result = r'合并完成！'
        self.label_3.setText(success_result)
        self.pushButton.setEnabled(True)
        self.pushButton_2.setEnabled(True)
        self.pushButton_3.setEnabled(True)
        self.pushButton_4.setEnabled(True)

def dbReader(db3_path, file_path):
    # -----------------------------------------------------------
    # db3_path = sys.argv[1]  # unicode(sys.argv[1],'utf-8')
    # db3_path = 'E:\sqlitedata'
    for dir, dirs, files in os.walk(db3_path):
        # 由于os.walk会遍历包含子文件夹在内的全部文件
        # 采用os.listdir可只罗列当前目录的文件
        # 这里采用os.walk加辅助控制的办法
        L = 0
        # n = n + 1  # n为1的时候 正在处理首层目录，记录下其层级L
        if n == 1:
            L = len(dir.split("\\"))
        for file in files:
            try:
                if file.split(".")[1] == db3_extension_name:
                    db3list.append(os.path.join(dir, file))
            except:
                print("Got Exception")
    file_path = file_path + '/pwmis.db'
    # -----------------------------------------------------------
    # con_des = sqlite3.connect(sys.argv[2])
    con_des = sqlite3.connect(file_path)
    global tables_des, sql_inqury_tables

    # 便于自定义函数中对数据库列表信息的引用和输出
    tables_des = []
    sql_inqury_tables = "SELECT name FROM sqlite_master WHERE type='table'"

    total = 0

    # 接下来开始合并，新建一个数据库作为目标数据库
    # 之所以没有选择第一个待合并的数据库作为模板
    # 是因为数据库结构中可能存在的主键等问题，详见文后总结
    # 通过tables_des记录合并后的目标数据库内表名称，以便后续比较
    i = 0
    while i < len(db3list) and ((len(db3list[i].split("\\")) == L + 1) or (including_subs)):
        # 判别条件包括了【是否到达db3数据路径列表尾端，以及是否包含子文件夹内的db3两个，后者又分为了逻辑或连接的
        # 层级是否为主文件夹层级+1 或者 是否要求遍历全部文件夹两个条件
        # 由于采用了while循环，一旦出现子目录的文件，循环即终止
        con_current = sqlite3.connect(db3list[i])
        cur_current = con_current.cursor()
        cur_current.execute(sql_inqury_tables)
        tables_current = cur_current.fetchall()
        cur_des = con_des.cursor()
        cur_des.execute(sql_inqury_tables)
        tables_des = cur_des.fetchall()
        # 这里有一个前提假设：不同数据库文件的相同名称table具有相同的结构，避免了逐个字段判断和对表结构的调整
        compare_tables(tables_current, tables_des, con_current, con_des)
        # 经过compare_tables函数后，目标数据库的表格已经大于等于当前待合并的数据库了
        # 接下来逐个将表的信息录入目标数据库即可，因此再构建一个append_data函数
        append_data(con_current, con_des, tables_current)
        # 数据库验证 主要验证数量 需要时再取消备注
        # total_temp = sub_data(con_current, tables_current, datatable)
        # if isinstance(total_temp, int):
        #     total += sub_data(con_current, tables_current, datatable)
        con_current.close()
        i += 1
    con_des.close()
    # 数据库验证需要的参数 主要为合并之后的表内总量以及传入的需要验证的表名信息
    # datatable = sys.argv[3]
    # sql = "select * from " + datatable
    # cur_des_data = con_des.cursor()
    # cur_des_data.execute(sql)
    # temp_data_list1 = cur_des_data.fetchall()
    # nn = len(temp_data_list1)
    # # 用总量减去总插入量（插入量为每个数据库中此表内的数据总数）
    # nn = nn - total
    return ("---------All Data Finished!--------")

if __name__ == '__main__':
    import sys
    app = QtWidgets.QApplication(sys.argv)
    ui = mywindow()
    ui.show()
    sys.exit(app.exec_())