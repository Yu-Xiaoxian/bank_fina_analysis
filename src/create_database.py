import mysql.connector
import configparser
import os

# 定义 SQL 脚本文件路径
SCHEMA_SQL_FILE = 'sql/01_schema_creation.sql'

# --------------------------
# 1. 配置读取函数
# --------------------------
def get_db_config(filename='conf/settings.ini', section='DATABASE'):
    """从配置文件中读取数据库配置"""
    parser = configparser.ConfigParser()
    # 尝试读取 settings.ini 文件
    if not parser.read(filename):
        raise FileNotFoundError(f"无法找到或读取配置文件: {filename}")
    
    db_config = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db_config[item[0]] = item[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')
        
    return db_config

# --------------------------
# 2. SQL 文件加载函数
# --------------------------
def load_sql_script(filepath):
    """从文件中加载 SQL 脚本内容"""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"SQL 脚本文件未找到: {filepath}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        return f.read()

# --------------------------
# 3. 数据库连接和执行函数
# --------------------------
def execute_sql_script(sql_script, db_config):
    """连接数据库并执行SQL脚本"""
    
    # 临时连接信息 (不指定DB名，用于创建DB)
    temp_config = db_config.copy()
    db_name = temp_config.pop('database') # 移除 database key
    
    cnx = None
    cursor = None
    
    try:
        # 步骤 1: 连接到MySQL服务器 (不指定数据库，执行CREATE DATABASE)
        cnx = mysql.connector.connect(**temp_config)
        cursor = cnx.cursor()
        print("成功连接到数据库服务器。")

        # 步骤 2: 分割并执行SQL语句
        # 使用 ';' 分隔符分割SQL脚本
        sql_commands = [cmd.strip() for cmd in sql_script.split(';') if cmd.strip()]

        for command in sql_commands:
            try:
                # 针对 USE 语句的特殊处理：执行完 USE 语句后，我们重新连接到目标数据库
                if command.strip().upper().startswith("USE"):
                    cursor.execute(command)
                    
                    # 重新连接以确保所有后续操作都在新数据库上
                    cursor.close()
                    cnx.close()
                    
                    db_config['database'] = db_name # 恢复 database key
                    cnx = mysql.connector.connect(**db_config)
                    cursor = cnx.cursor()
                    print(f"✅ 成功切换到数据库: {db_name}")
                else:
                    # 执行其他 DDL 语句 (CREATE TABLE, CREATE DATABASE IF NOT EXISTS 等)
                    cursor.execute(command)
                    
                print(f"✅ 成功执行: {command[:50]}...")
            except mysql.connector.Error as err:
                # 忽略一些已存在的警告，但对其他错误抛出异常
                if err.errno in [1050, 1007]: # 1050: 表已存在, 1007: 数据库已存在
                    print(f"⚠️ 数据库或表已存在，跳过。")
                else:
                    print(f"❌ 执行失败: {command[:50]}...")
                    print(f"错误信息: {err}")
                    raise # 停止执行

        # 提交所有更改
        cnx.commit()
        print("\n🎉 数据库结构创建成功！")

    except mysql.connector.Error as err:
        if err.errno == 1045: # ER_ACCESS_DENIED_ERROR
            print("错误: 数据库连接失败。请检查 settings.ini 中的用户名和密码。")
        else:
            print(f"发生其他数据库连接或操作错误: {err}")
    except FileNotFoundError as err:
        print(f"文件错误: {err}")
    except Exception as e:
        print(f"程序错误: {e}")
        
    finally:
        if cnx and cnx.is_connected():
            if cursor:
                cursor.close()
            cnx.close()
            print("数据库连接已关闭。")

if __name__ == "__main__":
    try:
        db_config = get_db_config()
        sql_script = load_sql_script(SCHEMA_SQL_FILE)
        execute_sql_script(sql_script, db_config)
    except Exception as e:
        print(f"脚本执行终止: {e}")