set hive.exec.mode.local.auto=true;

create database if not exists day0328;

use day0328;


-- 商品基础信息表（product_base）
drop table product_base;
CREATE TABLE product_base (
  product_id BIGINT COMMENT '商品ID', -- 使用业务ID而非自增ID
  product_name string COMMENT '商品名称',
  category_id INT COMMENT '类目ID', -- 关联类目维度表
  price DECIMAL(16,2) COMMENT '销售价格',
  status string COMMENT '商品状态',
  create_time string COMMENT '上架时间',
  update_time string COMMENT '最后修改时间'
) COMMENT'商品基础信息表';

INSERT INTO product_base VALUES
(1001, '男士真皮商务腰带', 301, 399.00, 'on_sale', '2023-09-05 09:30:00', '2023-10-03 14:20:00'),
(1002, '无线降噪蓝牙耳机', 205, 899.00, 'off_shelf', '2023-08-20 11:15:00', '2023-10-02 16:45:00'),
(1003, '304不锈钢保温杯', 112, 159.00, 'on_sale', '2023-09-25 14:00:00', '2023-10-03 10:10:00');





-- 商品每日指标表（product_daily_metrics）
drop table product_daily_metrics;
CREATE TABLE product_daily_metrics (
  stat_date string COMMENT '统计日期', -- 使用日期类型而非datetime
  product_id BIGINT COMMENT '商品ID',
  -- 流量维度
  pv INT COMMENT '当日浏览量',
  uv INT COMMENT '当日访客数',
  avg_duration DECIMAL(16,2) COMMENT '平均停留时长(秒)',
  -- 行为维度
  favorites INT COMMENT '收藏量',
  cart_adds INT COMMENT '加购量',
  -- 销售维度
  sales_volume INT COMMENT '销售件数',
  sales_amount DECIMAL(16,2) COMMENT '销售金额',
  update_time string COMMENT '更新时间'

)  COMMENT'商品每日指标表';

INSERT INTO product_daily_metrics VALUES
('2023-10-01', 1001, 1200, 650, 48.5, 85, 60, 45, 17955.00, '2023-10-01 23:59:59'),
('2023-10-02', 1001, 1050, 580, 42.3, 70, 45, 38, 14232.00, '2023-10-02 23:59:59'),
('2023-10-03', 1003, 850, 480, 55.2, 95, 70, 55, 8745.00, '2023-10-03 23:59:59');






-- 库存健康表（inventory_health）
drop table inventory_health;
CREATE TABLE inventory_health (
  product_id BIGINT,
  current_stock INT COMMENT '当前库存',
  stock_days INT COMMENT '库存周转天数',
  stock_status string comment 'normal:正常,overstock:积压,shortage:短缺',
  update_time string
) COMMENT'库存健康表';

INSERT INTO inventory_health VALUES
(1001, 120, 7, 'normal', '2023-10-03 08:00:00'),
(1002, 1500, 365, 'overstock', '2023-10-03 08:00:00'),
(1003, 8, 2, 'shortage', '2023-10-03 08:00:00');



-- 异常监控表（abnormal_monitor）
drop table abnormal_monitor;
CREATE TABLE abnormal_monitor (
  detect_id BIGINT,
  product_id BIGINT,
  detect_date string COMMENT '检测日期',
  abnormal_type string COMMENT '异常类型',
  confidence bigint COMMENT '异常置信度(0-100)',
  processed bigint COMMENT '是否已处理'
) COMMENT'异常监控表';

INSERT INTO abnormal_monitor VALUES
(90001, 1001, '2023-10-02', 'sales_fluctuation', 88, 0),
(90002, 1003, '2023-10-03', 'stock_alert', 95, 1);


select * from product_base;
select * from product_daily_metrics;
select * from inventory_health;
select * from abnormal_monitor;
