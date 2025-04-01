set hive.exec.mode.local.auto=true;
create database if not exists dev_realtime_defence_v1_runbo_zhang;
-- select sum(order_price),oi.truett from (select *from ods_order_info
--     lateral view explode(array(1,7,30))tmp as `ttt`) oi
-- left join ods_order_detail od on oi.id=od.order_id
--         group by oi.ttt;




drop table if exists ods_order_info;
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
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

    LOCATION '/2207A/gmall/data/ods/ods_order_info/'
    TBLPROPERTIES ("orc.compress"="lzop");

load data inpath '/2207A/gmall/data/yewu/order_info' into table ods_order_info;
select * from ods_order_info;




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
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

    LOCATION '/2207A/gmall/data/ods/ods_order_detail/'
    TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/order_detail' into table ods_order_detail;
select *from ods_order_detail;


DROP TABLE IF EXISTS ods_user_info;
CREATE EXTERNAL TABLE if not exists ods_user_info(
                                       `id` STRING COMMENT '用户id',
                                       `login_name` STRING COMMENT '用户名称',
                                       `nick_name` STRING COMMENT '用户昵称',
                                       `password` STRING COMMENT '密码',
                                       `name` STRING COMMENT '用户姓名',
                                       `phone_num` STRING COMMENT '手机号码',
                                       `email` STRING COMMENT '邮箱',
                                       `head_img` STRING COMMENT '头像',
                                       `user_level` STRING COMMENT '用户等级',
                                       `birthday` STRING COMMENT '生日',
                                       `gender` STRING COMMENT '性别',
                                       `create_time` STRING COMMENT '创建时间',
                                       `operate_time` STRING COMMENT '操作时间',
                                       `status` STRING COMMENT '状态'
) COMMENT '用户表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'

    LOCATION '/2207A/gmall/data/ods/ods_user_info/'
    TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/user_info'into table ods_user_info;
select *from ods_user_info;



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
                                      `sku_default_img` STRING COMMENT '默认显示',
                                      `is_sale` STRING COMMENT '是否在售',
                                      `create_time` STRING COMMENT '创建时间'
) COMMENT 'SKU商品表'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
    LOCATION '/2207A/gmall/data/ods/ods_sku_info/'
    TBLPROPERTIES ("orc.compress"="lzop");
load data inpath '/2207A/gmall/data/yewu/sku_info' into table ods_sku_info;
select * from ods_sku_info;
