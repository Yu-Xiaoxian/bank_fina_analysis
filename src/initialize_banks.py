import pandas as pd
import configparser
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import sys

# 假设配置文件路径
CONFIG_FILE = 'conf/settings.ini'

# --------------------------
# 核心数据：主要上市银行的名称和代码
# --------------------------
BANK_DATA = [
    # --- 国有大型银行 ---
    {'bank_name': '工商银行', 'stock_code': '601398.SH'},
    {'bank_name': '建设银行', 'stock_code': '601939.SH'},
    {'bank_name': '农业银行', 'stock_code': '601288.SH'},
    {'bank_name': '中国银行', 'stock_code': '601988.SH'},
    {'bank_name': '交通银行', 'stock_code': '601328.SH'},
    {'bank_name': '邮储银行', 'stock_code': '601658.SH'},

    # --- 股份制银行 ---
    {'bank_name': '招商银行', 'stock_code': '600036.SH'},
    {'bank_name': '兴业银行', 'stock_code': '601166.SH'},
    {'bank_name': '平安银行', 'stock_code': '000001.SZ'},
    {'bank_name': '浦发银行', 'stock_code': '600000.SH'},
    {'bank_name': '中信银行', 'stock_code': '601998.SH'},
    {'bank_name': '民生银行', 'stock_code': '600016.SH'},
    {'bank_name': '光大银行', 'stock_code': '601818.SH'},
    {'bank_name': '华夏银行', 'stock_code': '600015.SH'},

    # --- 城市商业银行 ---
    {'bank_name': '宁波银行', 'stock_code': '002142.SZ'},
    {'bank_name': '南京银行', 'stock_code': '601009.SH'},
    {'bank_name': '江苏银行', 'stock_code': '600919.SH'},
    {'bank_name': '上海银行', 'stock_code': '601229.SH'},
    {'bank_name': '北京银行', 'stock_code': '601169.SH'},
    {'bank_name': '杭州银行', 'stock_code': '600926.SH'},
    {'bank_name': '成都银行', 'stock_code': '601838.SH'},
    {'bank_name': '厦门银行', 'stock_code': '601187.SH'},
    {'bank_name': '苏农银行', 'stock_code': '603323.SH'},
    {'bank_name': '瑞丰银行', 'stock_code': '601528.SH'},
    {'bank_name': '齐鲁银行', 'stock_code': '601665.SH'},

    # --- 农村商业银行 ---
    {'bank_name': '常熟银行', 'stock_code': '601128.SH'},
    {'bank_name': '张家港行', 'stock_code': '002839.SZ'},
    {'bank_name': '无锡银行', 'stock_code': '600908.SH'},
    {'bank_name': '江阴银行', 'stock_code': '002807.SZ'},

    # --- 其他A股上市银行 ---
    {'bank_name': '青岛银行', 'stock_code': '002948.SZ'},
    {'bank_name': '西安银行', 'stock_code': '600928.SH'},
    {'bank_name': '长沙银行', 'stock_code': '601577.SH'},
    {'bank_name': '郑州银行', 'stock_code': '002936.SZ'},
    {'bank_name': '紫金银行', 'stock_code': '601860.SH'},
    # ... (还有一些小型上市银行，此列表已覆盖主要标的)
]

def get_db_config(filename=CONFIG_FILE):
    """从配置文件中读取数据库配置"""
    parser = configparser.ConfigParser()
    if not parser.read(filename):
        raise FileNotFoundError(f"错误：无法找到或读取配置文件: {filename}")
    
    return dict(parser.items('DATABASE'))

def create_db_engine(config):
    """创建 SQLAlchemy 数据库连接引擎，并对密码进行 URL 编码"""
    encoded_password = quote_plus(config['password'])
    db_url = (
        f"mysql+mysqlconnector://{config['user']}:"
        f"{encoded_password}@{config['host']}:{config['port']}/"
        f"{config['database']}?charset=utf8mb4"
    )
    return create_engine(db_url)


def initialize_banks_table():
    """将银行数据插入到 banks 表中，并确保 stock_code 是主键"""
    print("--- 银行数据初始化脚本启动 ---")
    try:
        db_config = get_db_config()
        engine = create_db_engine(db_config)
    except Exception as e:
        print(f"初始化失败，请检查配置文件和数据库连接: {e}")
        sys.exit(1)

    df = pd.DataFrame(BANK_DATA)
    table_name = 'banks' # 目标表名

    print(f"正在将 {len(df)} 条银行数据写入数据库表 '{table_name}'...")
    
    try:
        # **步骤 1: 使用 if_exists='replace' 创建或替换表结构**
        # 注意：这里可能会再次触发外键约束错误，因为 DROP TABLE 被拒绝。
        # 我们使用 try-except 来捕获这个错误，然后直接进入下一步。
        try:
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"表 '{table_name}' 已成功创建或替换。")
        except Exception as e:
            # 捕获到外键删除错误，表示表已存在，我们继续。
            if "Cannot drop table" in str(e):
                print(f"表 '{table_name}' 存在外键约束，跳过 DROP TABLE 步骤。")
            else:
                raise e # 抛出其他非外键相关的错误
        
        
        # **步骤 2: 确保 'stock_code' 是主键**
        # 即使表已存在，我们也尝试执行 ALTER TABLE，确保主键存在。
        # 如果主键已存在，MySQL 会友好地报错，我们用 try-except 忽略它。
        primary_key_sql = text(f"ALTER TABLE {table_name} ADD PRIMARY KEY (stock_code);")
        
        with engine.connect() as connection:
            try:
                connection.execute(primary_key_sql)
                connection.commit()
                print("✅ 已将 'stock_code' 设置为主键。")
            except Exception as e:
                # 捕获主键已存在的错误 (如 Duplicate key name 'PRIMARY')，忽略
                if "Duplicate key name" not in str(e):
                    print(f"警告: 设置主键时发生意外错误: {e}")
                
        # **步骤 3: 使用 ON DUPLICATE KEY UPDATE 方式插入/更新数据**
        # 这是最安全的方式：如果 stock_code 已存在，则更新 bank_name；否则插入。
        
        # 构建 MySQL 的 ON DUPLICATE KEY UPDATE 语句
        values_clause = ", ".join([f"('{row['bank_name']}', '{row['stock_code']}')" for _, row in df.iterrows()])
        
        insert_update_sql = text(f"""
            INSERT INTO {table_name} (bank_name, stock_code) VALUES {values_clause}
            ON DUPLICATE KEY UPDATE
                bank_name = VALUES(bank_name);
        """)
        
        with engine.connect() as connection:
            connection.execute(insert_update_sql)
            connection.commit()
            print(f"✅ 银行数据已使用 INSERT...ON DUPLICATE KEY UPDATE 方式成功更新/插入 {len(df)} 条。")


        print("\n🎉 银行表初始化完毕！")
        print("您现在可以安全地运行数据爬取脚本了: python src/tushare_data_fetcher.py")

    except Exception as e:
        print(f"\n❌ 数据写入失败。致命错误: {e}")

if __name__ == "__main__":
    initialize_banks_table()