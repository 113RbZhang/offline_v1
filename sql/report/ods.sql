set hive.exec.mode.local.auto=true;
-- set hive.exec.dynamic.partition=true; 动态分区
-- set hive.exec.dynamic.partition.mode=nonstrict;
create database if not exists dev_offline_report_v1_fine_1;
use dev_offline_report_v1_fine_1;


-----------------log------------------------
drop table if exists ods_log;
CREATE EXTERNAL TABLE if not exists ods_log (`line` string)
    partitioned by (dt string)
 -- 按照时间创建分区
LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_log';  -- 指定数据在hdfs上的存储位置
load data inpath '/2207A/gmall/data/log/app.2025-03-23.log' into table ods_log partition (dt='20250323');
load data inpath '/2207A/gmall/data/log/app.2025-03-24.log' into table ods_log partition (dt='20250324');
load data inpath '/2207A/gmall/data/log/app.2025-03-25.log' into table ods_log partition (dt='20250325');
select *from ods_log;
-----------业务------------------------------
-- 1 活动信息表
DROP TABLE IF EXISTS ods_activity_info;
CREATE EXTERNAL TABLE if not exists ods_activity_info(
    `id` STRING COMMENT '编号',
    `activity_name` STRING  COMMENT '活动名称',
    `activity_type` STRING  COMMENT '活动类型',
    `start_time` STRING  COMMENT '开始时间',
    `end_time` STRING  COMMENT '结束时间',
    `create_time` STRING  COMMENT '创建时间'
) COMMENT '活动信息表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_activity_info/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/activity_info' into table ods_activity_info ;
select *from ods_activity_info;
-- 2 活动规则表
DROP TABLE IF EXISTS ods_activity_rule;
CREATE EXTERNAL TABLE if not exists ods_activity_rule(
    `id` STRING COMMENT '编号',
    `activity_id` STRING  COMMENT '活动ID',
    `activity_type` STRING COMMENT '活动类型',
    `condition_amount` DECIMAL(16,2) COMMENT '满减金额',
    `condition_num` BIGINT COMMENT '满减件数',
    `benefit_amount` DECIMAL(16,2) COMMENT '优惠金额',
    `benefit_discount` DECIMAL(16,2) COMMENT '优惠折扣',
    `benefit_level` STRING COMMENT '优惠级别'
) COMMENT '活动规则表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_activity_rule/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/activity_rule' into table ods_activity_rule ;
select *from ods_activity_rule;
-- 3 一级品类表
DROP TABLE IF EXISTS ods_base_category1;
CREATE EXTERNAL TABLE if not exists ods_base_category1(
    `id` STRING COMMENT 'id',
    `name` STRING COMMENT '名称'
) COMMENT '商品一级分类表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_base_category1/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/base_category1' into table ods_base_category1 ;
select *from ods_base_category1;
-- 4 二级品类表
DROP TABLE IF EXISTS ods_base_category2;
CREATE EXTERNAL TABLE if not exists ods_base_category2(
    `id` STRING COMMENT ' id',
    `name` STRING COMMENT '名称',
    `category1_id` STRING COMMENT '一级品类id'
) COMMENT '商品二级分类表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_base_category2/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/base_category2' into table ods_base_category2 ;
select *from ods_base_category2;
-- 5 三级品类表
DROP TABLE IF EXISTS ods_base_category3;
CREATE EXTERNAL TABLE if not exists ods_base_category3(
    `id` STRING COMMENT ' id',
    `name` STRING COMMENT '名称',
    `category2_id` STRING COMMENT '二级品类id'
) COMMENT '商品三级分类表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_base_category3/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/base_category3' into table ods_base_category3 ;
select *from ods_base_category3;
-- 6 编码字典表
DROP TABLE IF EXISTS ods_base_dic;
CREATE EXTERNAL TABLE if not exists ods_base_dic(
    `dic_code` STRING COMMENT '编号',
    `dic_name` STRING COMMENT '编码名称',
    `parent_code` STRING COMMENT '父编码',
    `create_time` STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '操作日期'
) COMMENT '编码字典表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_base_dic/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/base_dic' into table ods_base_dic ;
select *from ods_base_dic;
-- 7 省份表
DROP TABLE IF EXISTS ods_base_province;
CREATE EXTERNAL TABLE if not exists ods_base_province (
    `id` STRING COMMENT '编号',
    `name` STRING COMMENT '省份名称',
    `region_id` STRING COMMENT '地区ID',
    `area_code` STRING COMMENT '地区编码',
    `iso_code` STRING COMMENT 'ISO-3166编码，供可视化使用',
    `iso_3166_2` STRING COMMENT 'IOS-3166-2编码，供可视化使用'
)  COMMENT '省份表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_base_province/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/base_province' into table ods_base_province;
select *from ods_base_province;
-- 8 地区表
DROP TABLE IF EXISTS ods_base_region;
CREATE EXTERNAL TABLE if not exists ods_base_region (
    `id` STRING COMMENT '编号',
    `region_name` STRING COMMENT '地区名称'
)  COMMENT '地区表'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_base_region/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/base_region' into table ods_base_region;
select *from ods_base_region;
-- 9 品牌表
DROP TABLE IF EXISTS ods_base_trademark;
CREATE EXTERNAL TABLE if not exists ods_base_trademark (
    `id` STRING COMMENT '编号',
    `tm_name` STRING COMMENT '品牌名称'
)  COMMENT '品牌表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_base_trademark/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/base_trademark' into table ods_base_trademark ;
select *from ods_base_trademark;
-- 10 购物车表
DROP TABLE IF EXISTS ods_cart_info;
CREATE EXTERNAL TABLE if not exists ods_cart_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户id',
    `sku_id` STRING COMMENT 'skuid',
    `cart_price` DECIMAL(16,2)  COMMENT '放入购物车时价格',
    `sku_num` BIGINT COMMENT '数量',
    `url` string COMMENT 'url',
    `sku_name` STRING COMMENT 'sku名称 (冗余)',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `is_ordered` STRING COMMENT '是否已经下单',
    `order_time` STRING COMMENT '下单时间',
    `source_type` STRING COMMENT '来源类型',
    `source_id` STRING COMMENT '来源编号'
) COMMENT '加购表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_cart_info/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/cart_info' into table ods_cart_info ;
select *from ods_cart_info;
-- 11 评论表
DROP TABLE IF EXISTS ods_comment_info;
CREATE EXTERNAL TABLE if not exists ods_comment_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `nic` STRING COMMENT '用户昵称',
    `head` STRING COMMENT '用户头像',
    `sku_id` STRING COMMENT '商品sku',
    `spu_id` STRING COMMENT '商品spu',
    `order_id` STRING COMMENT '订单ID',
    `appraise` STRING COMMENT '评价',
    `appraise_nr` STRING COMMENT '评价内容',
    `create_time` STRING COMMENT '评价时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '商品评论表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_comment_info/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/comment_info' into table ods_comment_info ;
select *from ods_comment_info;
-- 12 优惠券信息表
DROP TABLE IF EXISTS ods_coupon_info;
CREATE EXTERNAL TABLE if not exists ods_coupon_info(
    `id` STRING COMMENT '购物券编号',
    `coupon_name` STRING COMMENT '购物券名称',
    `coupon_type` STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` DECIMAL(16,2) COMMENT '满额数',
    `condition_num` BIGINT COMMENT '满件数',
    `activity_id` STRING COMMENT '活动编号',
    `benefit_amount` DECIMAL(16,2) COMMENT '减金额',
    `benefit_discount` DECIMAL(16,2) COMMENT '折扣',
    `create_time` STRING COMMENT '创建时间',
    `range_type` STRING COMMENT '范围类型 1、商品 2、品类 3、品牌',
    `limit_num` BIGINT COMMENT '最多领用次数',
    `taken_count` BIGINT COMMENT '已领用次数',
    `start_time` STRING COMMENT '开始领取时间',
    `end_time` STRING COMMENT '结束领取时间',
    `operate_time` STRING COMMENT '修改时间',
    `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_coupon_info/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/coupon_info' into table ods_coupon_info ;
select * from ods_coupon_info;
-- 13 优惠券领用表
DROP TABLE IF EXISTS ods_coupon_use;
CREATE EXTERNAL TABLE if not exists ods_coupon_use(
    `id` STRING COMMENT '编号',
    `coupon_id` STRING  COMMENT '优惠券ID',
    `user_id` STRING  COMMENT 'skuid',
    `order_id` STRING  COMMENT 'spuid',
    `coupon_status` STRING  COMMENT '优惠券状态',
    `get_time` STRING  COMMENT '领取时间',
    `using_time` STRING  COMMENT '使用时间(下单)',
    `used_time` STRING  COMMENT '使用时间(支付)',
    `expire_time` STRING COMMENT '过期时间'
) COMMENT '优惠券领用表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_coupon_use/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/coupon_use' into table ods_coupon_use ;
select * from ods_coupon_use;
-- 14 收藏表
DROP TABLE IF EXISTS ods_favor_info;
CREATE EXTERNAL TABLE if not exists ods_favor_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户id',
    `sku_id` STRING COMMENT 'skuid',
    `spu_id` STRING COMMENT 'spuid',
    `is_cancel` STRING COMMENT '是否取消',
    `create_time` STRING COMMENT '收藏时间',
    `cancel_time` STRING COMMENT '取消时间'
) COMMENT '商品收藏表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_favor_info/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/favor_info' into table ods_favor_info ;
select *from ods_favor_info;
-- 15 订单明细表
DROP TABLE IF EXISTS ods_order_detail;
CREATE EXTERNAL TABLE if not exists ods_order_detail(
    `id` STRING COMMENT '编号',
    `order_id` STRING  COMMENT '订单号',
    `sku_id` STRING COMMENT '商品id',
    `sku_name` STRING COMMENT '商品名称',
	 img_url  STRING COMMENT '路径',
    `order_price` DECIMAL(16,2) COMMENT '商品价格',
    `sku_num` BIGINT COMMENT '商品数量',
    `create_time` STRING COMMENT '创建时间',
    `source_type` STRING COMMENT '来源类型',
    `source_id` STRING COMMENT '来源编号',
    `split_total_amount` DECIMAL(16,2) COMMENT '分摊最终金额',
    `split_activity_amount` DECIMAL(16,2) COMMENT '分摊活动优惠',
    `split_coupon_amount` DECIMAL(16,2) COMMENT '分摊优惠券优惠'
) COMMENT '订单详情表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_order_detail/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/order_detail' into table ods_order_detail ;
select *from ods_order_detail;

-- 16 订单明细活动关联表
DROP TABLE IF EXISTS ods_order_detail_activity;
CREATE EXTERNAL TABLE if not exists ods_order_detail_activity(
    `id` STRING COMMENT '编号',
    `order_id` STRING  COMMENT '订单号',
    `order_detail_id` STRING COMMENT '订单明细id',
    `activity_id` STRING COMMENT '活动id',
    `activity_rule_id` STRING COMMENT '活动规则id',
    `sku_id` BIGINT COMMENT '商品id',
    `create_time` STRING COMMENT '创建时间'
) COMMENT '订单详情活动关联表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_order_detail_activity/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/order_detail_activity' into table ods_order_detail_activity ;
select *from ods_order_detail_activity;
-- 17 订单明细优惠券关联表
DROP TABLE IF EXISTS ods_order_detail_coupon;
CREATE EXTERNAL TABLE if not exists ods_order_detail_coupon(
    `id` STRING COMMENT '编号',
    `order_id` STRING  COMMENT '订单号',
    `order_detail_id` STRING COMMENT '订单明细id',
    `coupon_id` STRING COMMENT '优惠券id',
    `coupon_use_id` STRING COMMENT '优惠券领用记录id',
    `sku_id` STRING COMMENT '商品id',
    `create_time` STRING COMMENT '创建时间'
) COMMENT '订单详情活动关联表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_order_detail_coupon/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/order_detail_coupon' into table ods_order_detail_coupon ;
select *from ods_order_detail_coupon;
-- 18 订单表
drop table ods_order_info;
CREATE EXTERNAL TABLE if not exists ods_order_info (
    `id` STRING COMMENT '订单号',
	 consignee STRING,
	 consignee_tel STRING,
    `total_amount` DECIMAL(16,2) COMMENT '订单最终金额',
    `order_status` STRING COMMENT '订单状态',
    `user_id` STRING COMMENT '用户id',
    `payment_way` STRING COMMENT '支付方式',
    `delivery_address` STRING COMMENT '送货地址',
	 order_comment STRING,
    `out_trade_no` STRING COMMENT '支付流水号',
	 trade_body STRING,
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `expire_time` STRING COMMENT '过期时间',
	 process_status STRING,
    `tracking_no` STRING COMMENT '物流单编号',
	 parent_order_id BIGINT,
	 img_url STRING,
    `province_id` STRING COMMENT '省份ID',
    `activity_reduce_amount` DECIMAL(16,2) COMMENT '活动减免金额',
    `coupon_reduce_amount` DECIMAL(16,2) COMMENT '优惠券减免金额',
    `original_total_amount` DECIMAL(16,2)  COMMENT '订单原价金额',
    `feight_fee` DECIMAL(16,2)  COMMENT '运费',
    `feight_fee_reduce` DECIMAL(16,2)  COMMENT '运费减免',
	 refundable_time STRING
) COMMENT '订单表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_order_info/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/order_info' into table ods_order_info ;
select * from ods_order_info;

-- 19 退单表
DROP TABLE IF EXISTS ods_order_refund_info;
CREATE EXTERNAL TABLE if not exists ods_order_refund_info(
    `id` STRING COMMENT '编号',
    `user_id` STRING COMMENT '用户ID',
    `order_id` STRING COMMENT '订单ID',
    `sku_id` STRING COMMENT '商品ID',
    `refund_type` STRING COMMENT '退单类型',
    `refund_num` BIGINT COMMENT '退单件数',
    `refund_amount` DECIMAL(16,2) COMMENT '退单金额',
    `refund_reason_type` STRING COMMENT '退单原因类型',
    `refund_reason_txt` STRING COMMENT '退单文本',
    `refund_status` STRING COMMENT '退单状态',--退单状态应包含买家申请、卖家审核、卖家收货、退款完成等状态。此处未涉及到，故该表按增量处理
    `create_time` STRING COMMENT '退单时间'
) COMMENT '退单表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_order_refund_info/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/order_refund_info' into table ods_order_refund_info ;
select *from ods_order_refund_info;
-- 20 订单状态日志表
DROP TABLE IF EXISTS ods_order_status_log;
CREATE EXTERNAL TABLE if not exists ods_order_status_log (
    `id` STRING COMMENT '编号',
    `order_id` STRING COMMENT '订单ID',
    `order_status` STRING COMMENT '订单状态',
    `operate_time` STRING COMMENT '修改时间'
)  COMMENT '订单状态表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_order_status_log/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/order_status_log' into table ods_order_status_log ;
select *from ods_order_status_log;
-- 21 支付表
DROP TABLE IF EXISTS ods_payment_info;
CREATE EXTERNAL TABLE if not exists ods_payment_info(
    `id` STRING COMMENT '编号',
    `out_trade_no` STRING COMMENT '对外业务编号',
    `order_id` STRING COMMENT '订单编号',
    `user_id` STRING COMMENT '用户编号',
    `payment_type` STRING COMMENT '支付类型',
    `trade_no` STRING COMMENT '交易编号',
    `payment_amount` DECIMAL(16,2) COMMENT '支付金额',
    `subject` STRING COMMENT '交易内容',
    `payment_status` STRING COMMENT '支付状态',
    `create_time` STRING COMMENT '创建时间',
    `callback_time` STRING COMMENT '回调时间'
)  COMMENT '支付流水表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_payment_info/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/payment_info' into table ods_payment_info ;
select *from ods_payment_info;
-- 22 退款表
DROP TABLE IF EXISTS ods_refund_payment;
CREATE EXTERNAL TABLE if not exists ods_refund_payment(
    `id` STRING COMMENT '编号',
    `out_trade_no` STRING COMMENT '对外业务编号',
    `order_id` STRING COMMENT '订单编号',
    `sku_id` STRING COMMENT 'SKU编号',
    `payment_type` STRING COMMENT '支付类型',
    `trade_no` STRING COMMENT '交易编号',
    `refund_amount` DECIMAL(16,2) COMMENT '支付金额',
    `subject` STRING COMMENT '交易内容',
    `refund_status` STRING COMMENT '支付状态',
    `create_time` STRING COMMENT '创建时间',
    `callback_time` STRING COMMENT '回调时间'
)  COMMENT '支付流水表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_refund_payment/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/refund_payment' into table ods_refund_payment ;
select *from ods_refund_payment;
-- 23 商品平台属性表
DROP TABLE IF EXISTS ods_sku_attr_value;
CREATE EXTERNAL TABLE if not exists ods_sku_attr_value(
    `id` STRING COMMENT '编号',
    `attr_id` STRING COMMENT '平台属性ID',
    `value_id` STRING COMMENT '平台属性值ID',
    `sku_id` STRING COMMENT '商品ID',
    `attr_name` STRING COMMENT '平台属性名称',
    `value_name` STRING COMMENT '平台属性值名称'
) COMMENT 'sku平台属性表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_sku_attr_value/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/sku_attr_value' into table ods_sku_attr_value ;
select *from ods_sku_attr_value;
-- 24 商品（SKU）表
DROP TABLE IF EXISTS ods_sku_info;
CREATE EXTERNAL TABLE if not exists ods_sku_info(
    `id` STRING COMMENT 'skuId',
    `spu_id` STRING COMMENT 'spuid',
    `price` DECIMAL(16,2) COMMENT '价格',
    `sku_name` STRING COMMENT '商品名称',
    `sku_desc` STRING COMMENT '商品描述',
    `weight` DECIMAL(16,2) COMMENT '重量',
    `tm_id` STRING COMMENT '品牌id',
    `category3_id` STRING COMMENT '品类id',
    `url` STRING COMMENT 'url',
    `is_sale` STRING COMMENT '是否在售',
    `create_time` STRING COMMENT '创建时间'
) COMMENT 'SKU商品表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_sku_info/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/sku_info'overwrite into table ods_sku_info ;
select *from ods_sku_info;
-- 25 商品销售属性表
DROP TABLE IF EXISTS ods_sku_sale_attr_value;
CREATE EXTERNAL TABLE if not exists ods_sku_sale_attr_value(
    `id` STRING COMMENT '编号',
    `sku_id` STRING COMMENT 'sku_id',
    `spu_id` STRING COMMENT 'spu_id',
    `sale_attr_value_id` STRING COMMENT '销售属性值id',
    `sale_attr_id` STRING COMMENT '销售属性id',
    `sale_attr_name` STRING COMMENT '销售属性名称',
    `sale_attr_value_name` STRING COMMENT '销售属性值名称'
) COMMENT 'sku销售属性名称'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_sku_sale_attr_value/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/sku_sale_attr_value' into table ods_sku_sale_attr_value ;
select *from ods_sku_sale_attr_value;
-- 26 商品（SPU）表
DROP TABLE IF EXISTS ods_spu_info;
CREATE EXTERNAL TABLE if not exists ods_spu_info(
    `id` STRING COMMENT 'spuid',
    `spu_name` STRING COMMENT 'spu名称',
    `category3_id` STRING COMMENT '品类id',
    `tm_id` STRING COMMENT '品牌id'
) COMMENT 'SPU商品表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_spu_info/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/spu_info'into table ods_spu_info ;
select *from ods_spu_info;
-- 27 用户表
DROP TABLE IF EXISTS ods_user_info;
CREATE EXTERNAL TABLE if not exists ods_user_info(
    `id` STRING COMMENT '用户id',
    `login_name` STRING COMMENT '用户名称',
    `nick_name` STRING COMMENT '用户昵称',
    `passwd` STRING COMMENT '密码',
    `name` STRING COMMENT '用户姓名',
    `phone_num` STRING COMMENT '手机号码',
    `email` STRING COMMENT '邮箱',
    `head` STRING COMMENT '头像',
    `user_level` STRING COMMENT '用户等级',
    `birthday` STRING COMMENT '生日',
    `gender` STRING COMMENT '性别',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `status` STRING COMMENT '状态'
) COMMENT '用户表'

ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'

LOCATION '/2207A/runbo_zhang/report/gmall/ods/ods_user_info/'
TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/user_info'into table ods_user_info ;

select *from ods_user_info;
