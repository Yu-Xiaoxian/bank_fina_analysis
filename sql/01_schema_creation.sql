-- 尝试创建数据库（如果不存在）。
CREATE DATABASE IF NOT EXISTS bank_analysis_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- 切换到新创建的数据库
USE bank_analysis_db;

-- ---------------------------------
-- 1. 银行信息表 (banks)
-- ---------------------------------
CREATE TABLE IF NOT EXISTS banks (
    bank_id INT PRIMARY KEY AUTO_INCREMENT COMMENT '银行唯一ID',
    bank_name VARCHAR(100) NOT NULL COMMENT '银行名称 (e.g., 招商银行)',
    stock_code VARCHAR(20) NOT NULL UNIQUE COMMENT '股票代码 (e.g., 600036.SH)',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='分析银行的基本信息';

-- ---------------------------------
-- 2. 财务指标定义表 (financial_indicators)
-- ---------------------------------
CREATE TABLE IF NOT EXISTS financial_indicators (
    indicator_id INT PRIMARY KEY AUTO_INCREMENT COMMENT '指标唯一ID',
    indicator_code VARCHAR(50) NOT NULL UNIQUE COMMENT '指标的机器可读代码 (e.g., REVENUE, NET_PROFIT)',
    indicator_name VARCHAR(100) NOT NULL COMMENT '指标的中文名称 (e.g., 营业收入, 净利润)',
    description TEXT COMMENT '指标的详细描述'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='所有追踪财务指标的定义';

-- ---------------------------------
-- 3. 财报元数据表 (financial_reports)
-- ---------------------------------
CREATE TABLE IF NOT EXISTS financial_reports (
    report_id INT PRIMARY KEY AUTO_INCREMENT COMMENT '财报的唯一ID',
    bank_id INT NOT NULL COMMENT '关联的银行ID',
    year INT NOT NULL COMMENT '财报所属年份 (e.g., 2024)',
    period_type ENUM('Q1', 'H1', 'Q1_Q3', 'FY') NOT NULL COMMENT '报告周期类型 (Q1, H1, Q1_Q3, FY)',
    report_date DATE NOT NULL COMMENT '财报发布日期',
    source_url VARCHAR(255) COMMENT '财报源链接',
    currency VARCHAR(10) DEFAULT 'CNY' COMMENT '货币单位',
    
    FOREIGN KEY (bank_id) REFERENCES banks(bank_id) ON DELETE CASCADE,
    UNIQUE KEY idx_bank_year_period (bank_id, year, period_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='每份财报的基本信息和周期管理';

-- ---------------------------------
-- 4. 原始财报数据表 (reported_data)
-- ---------------------------------
CREATE TABLE IF NOT EXISTS reported_data (
    data_id INT PRIMARY KEY AUTO_INCREMENT COMMENT '数据唯一ID',
    report_id INT NOT NULL COMMENT '关联的财报ID',
    indicator_id INT NOT NULL COMMENT '关联的指标ID',
    indicator_value DECIMAL(20, 4) NOT NULL COMMENT '指标的数值',
    
    FOREIGN KEY (report_id) REFERENCES financial_reports(report_id) ON DELETE CASCADE,
    FOREIGN KEY (indicator_id) REFERENCES financial_indicators(indicator_id) ON DELETE RESTRICT,
    UNIQUE KEY idx_report_indicator (report_id, indicator_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='从财报中直接录入的原始数值';