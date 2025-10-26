import tushare as ts
import pandas as pd
import configparser
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine import Connection
from urllib.parse import quote_plus
import time
import sys

# 配置文件路径已修改为 'conf/settings.ini'
CONFIG_FILE = 'conf/settings.ini' 

# --------------------------
# 1. 配置和数据库连接函数
# --------------------------

def get_config(filename=CONFIG_FILE):
    """从配置文件中读取 Tushare 和数据库配置"""
    parser = configparser.ConfigParser()
    if not parser.read(filename):
        # 考虑到运行环境可能在 src/ 内部，尝试相对路径
        if not parser.read('../' + filename):
            raise FileNotFoundError(f"错误：无法找到或读取配置文件: {filename} 或 ../{filename}")
    
    db_config = dict(parser.items('DATABASE'))
    ts_token = parser.get('TSHARE', 'token')
    
    # 检查核心配置项
    if not all(k in db_config for k in ['user', 'password', 'host', 'port', 'database']):
        raise ValueError("错误：settings.ini 文件中 [DATABASE] 配置项不完整。")
    if not ts_token:
        raise ValueError("错误：settings.ini 文件中 [TSHARE] token 不能为空。")
        
    return db_config, ts_token

def create_db_engine(config: dict) -> Engine:
    """创建 SQLAlchemy 数据库连接引擎，并对密码进行 URL 编码"""
    
    # 对密码进行 URL 编码，以正确处理 @, :, / 等特殊字符
    encoded_password = quote_plus(config['password'])
    
    # 构建数据库连接 URL
    db_url = (
        f"mysql+mysqlconnector://{config['user']}:"
        f"{encoded_password}@{config['host']}:{config['port']}/"
        f"{config['database']}?charset=utf8mb4"
    )
    
    print(f"尝试连接数据库: {config['database']}@{config['host']}:{config['port']}...")
    return create_engine(db_url)

# --------------------------
# 2. 数据接口和数据库映射
# --------------------------

# Tushare 接口名称和它们在数据库中对应的表名
TABLE_MAPPING = {
    'income': 't_income',
    'balancesheet': 't_balancesheet',
    'fina_indicator': 't_fina_indicator',
    'dividend': 't_dividend'
}

TS_API_LIST = list(TABLE_MAPPING.keys())

# --------------------------
# 3. 核心数据爬取函数
# --------------------------

def get_bank_codes(engine: Engine) -> list:
    """从 banks 表中读取所有银行的股票代码 (stock_code)"""
    print("--- 1. 从数据库读取银行股票代码 ---")
    try:
        # 假设 banks 表中的股票代码字段名为 stock_code
        sql = "SELECT stock_code FROM banks;"
        df = pd.read_sql(sql, engine)
        
        if df.empty:
            print("警告：banks 表中没有找到任何股票代码。请先运行初始化脚本。")
            return []
            
        ts_codes = df['stock_code'].tolist()
        print(f"成功读取 {len(ts_codes)} 个银行股票代码。")
        return ts_codes
        
    except Exception as e:
        print(f"❌ 读取 banks 表失败。请确认表名是否存在。错误: {e}")
        return []

def mysql_insert_ignore(table, conn: Connection, keys, data_iter):
    """
    方法 A：用于 t_dividend 表。
    功能：实现 INSERT IGNORE，遇到重复的主键时，忽略该行数据。
    """
    
    full_table_name = f"{table.schema}.{table.name}" if table.schema else table.name
    placeholders = [f":{key}" for key in keys]
    
    # 构造 INSERT IGNORE INTO 语句
    sql_string = (
        f"INSERT IGNORE INTO {full_table_name} ({', '.join(keys)}) "
        f"VALUES ({', '.join(placeholders)})"
    )
    
    insert_stmt = text(sql_string)
    data_to_insert = [dict(zip(keys, row)) for row in data_iter]
    
    result = conn.execute(insert_stmt, data_to_insert)
    return result.rowcount


def mysql_insert_update(table, conn: Connection, keys, data_iter):
    
    full_table_name = f"{table.schema}.{table.name}" if table.schema else table.name
    placeholders = [f":{key}" for key in keys]
    
    has_update_flag = 'update_flag' in keys # 确定是否有 update_flag 字段
    update_cols = []
    
    for k in keys:
        if has_update_flag:
            # if k == 'update_flag':
            #     # **【关键修复点】:** 无论 update_flag 是 0 还是 1，都用新值覆盖它。
            #     # 这样做可以避免保留旧的 update_flag 值，确保逻辑的自洽性。
            #     update_cols.append(f"{k} = VALUES({k})")
            # else:
                # 其它字段：只有当新数据的 update_flag = 1 时才更新
                update_cols.append(
                    f"{k} = IF(VALUES(update_flag) = 1, VALUES({k}), {k})"
                )
        else:
            # 不含 update_flag 的表 (如 fina_indicator)，全字段覆盖
            update_cols.append(f"{k} = VALUES({k})")

    # 构造 INSERT ... ON DUPLICATE KEY UPDATE 语句
    sql_string = (
        f"INSERT INTO {full_table_name} ({', '.join(keys)}) "
        f"VALUES ({', '.join(placeholders)}) "
        f"ON DUPLICATE KEY UPDATE {', '.join(update_cols)}"
    )
    
    insert_stmt = text(sql_string)
    data_to_insert = [dict(zip(keys, row)) for row in data_iter]
    
    # 批量执行语句
    result = conn.execute(insert_stmt, data_to_insert)
    return result.rowcount

def fetch_and_save_data(pro, engine: Engine, ts_code: str, api_name: str):
    """从 Tushare 获取数据并保存到数据库"""
    
    table_name = TABLE_MAPPING[api_name]

    if api_name == 'dividend':
        write_method = mysql_insert_ignore
        print("  -> 使用 [INSERT IGNORE] 写入 (分红数据，忽略重复)。")
        
    else:
        write_method = mysql_insert_update
        if api_name in ['income', 'balancesheet']:
            # 包含 update_flag 的表
            print("  -> 使用 [ON DUPLICATE KEY UPDATE with UPDATE_FLAG] 写入 (只有修正数据才更新)。")
        else:
            # 不含 update_flag 的表 (如 fina_indicator)
            print("  -> 使用 [ON DUPLICATE KEY UPDATE] 写入 (全字段覆盖更新)。")
    
    try:
        # 获取对应的 Tushare 接口函数
        ts_func = getattr(pro, api_name)
        
        # 拉取该股票的所有历史数据
        # 财务数据接口通常使用 period 或 ann_date 过滤，这里不加日期限制，拉取全部历史数据
        df = ts_func(ts_code=ts_code, limit=0) # limit=0 相当于不限制，拉取全部
        
        if df.empty:
            # print(f"  -> {ts_code} 在 {api_name} 接口中无数据。")
            return
        
        if 'update_flag' in df.columns:
        # 将 update_flag 列转换为整数，并用 0 填充缺失值（NaN）
            df['update_flag'] = df['update_flag'].fillna(0).astype(int)
            
        # 写入数据库：使用 append 模式，依赖数据库表的主键实现去重和更新。
        # 注意：这要求目标表（t_income, t_balancesheet等）已正确设置复合主键（如 ts_code + end_date）
        df.to_sql(
            table_name, 
            engine, 
            if_exists='append', 
            index=False, 
            chunksize=5000,
            method=write_method  # <-- 关键修改：使用自定义的去重方法
        )
        print(f"  -> {api_name} 数据成功存入 {len(df)} 行 (重复行已忽略)。")

    except Exception as db_error:
        # ... (错误捕获逻辑不变) ...
        # 即使使用了 INSERT IGNORE，某些底层连接错误仍可能发生，保留捕获
        print(f"  ❌ 数据库写入失败：{ts_code} 的 {api_name} 写入 {table_name} 时出错。")
        print(f"     错误信息: {db_error.__class__.__name__}: {db_error}")

    except Exception as e:
        print(f"  ❌ 爬取 {ts_code} 的 {api_name} 数据时发生错误: {e}")
    finally:
        # 遵循 Tushare API 频率限制，每次调用后暂停
        time.sleep(0.1) 

def run_fetcher():
    """主执行函数"""
    print("--- Tushare 银行数据爬取启动 (基于 conf 目录配置) ---")
    
    try:
        # 1. 初始化配置和连接
        db_config, ts_token = get_config()
        pro = ts.pro_api(ts_token)
        engine = create_db_engine(db_config)
        
        # 测试数据库连接
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            print("✅ 数据库连接成功。")

    except Exception as e:
        print(f"初始化或连接失败，请检查配置、密码特殊字符或 MySQL 服务器状态: {e}")
        sys.exit(1)

    # 2. 获取银行代码列表
    bank_codes = get_bank_codes(engine)
    if not bank_codes:
        sys.exit(0)

    print("\n--- 3. 循环爬取并存储银行数据 ---")
    
    start_time = time.time()
    total_codes = len(bank_codes)
    
    for i, ts_code in enumerate(bank_codes):
        print(f"\n[{i+1}/{total_codes}] 开始处理银行: {ts_code}")
        
        for api_name in TS_API_LIST:
            fetch_and_save_data(pro, engine, ts_code, api_name)
            
    end_time = time.time()
    print(f"\n--- 爬取完成！总耗时: {end_time - start_time:.2f} 秒 ---")


if __name__ == "__main__":
    run_fetcher()