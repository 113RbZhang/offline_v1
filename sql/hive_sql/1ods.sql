set hive.exec.mode.local.auto  =true;

create database if not exists dev_realtime_v1_runbo_zhang;
use dev_realtime_v1_runbo_zhang;

-- 1.创建电商业务相关的表：

--todo ods_order_info、
-- drop table if exists ods_order_info;
create table if not exists ods_order_info(
`id` bigint    COMMENT '编号',
`consignee` string   COMMENT '收货人',
`consignee_tel` string   COMMENT '收件人电话',
`final_total_amount` decimal(16,2)   COMMENT '总金额',
`order_status` string   COMMENT '订单状态',
`user_id` bigint   COMMENT '用户id',
`delivery_address` string  COMMENT '送货地址',
`order_comment` string   COMMENT '订单备注',
`out_trade_no` string   COMMENT '订单交易编号（第三方支付用)',
`trade_body` string   COMMENT '订单描述(第三方支付用)',
`create_time` string   COMMENT '创建时间',
`operate_time` string   COMMENT '操作时间',
`expire_time` string   COMMENT '失效时间',
`tracking_no` string   COMMENT '物流单编号',
`parent_order_id` bigint   COMMENT '父订单编号',
`img_url` string   COMMENT '图片路径',
`province_id` int   COMMENT '地区',
`benefit_reduce_amount` decimal(16,2)   COMMENT '优惠金额',
`original_total_amount` decimal(16,2)   COMMENT '原价金额',
`feight_fee`decimal(16,2) COMMENT '运费'
)partitioned by (dt string)
    row format delimited fields terminated by '\t'
location '/2207A/runbo_zhang/gmall/ods/ods_order_info'
    ;


-- drop table if exists ods_order_info_tem;
create temporary table if not exists ods_order_info_tem(
    `id` bigint    COMMENT '编号',
    `consignee` string   COMMENT '收货人',
    `consignee_tel` string   COMMENT '收件人电话',
    `final_total_amount` decimal(16,2)   COMMENT '总金额',
    `order_status` string   COMMENT '订单状态',
    `user_id` bigint   COMMENT '用户id',
    `delivery_address` string  COMMENT '送货地址',
    `order_comment` string   COMMENT '订单备注',
    `out_trade_no` string   COMMENT '订单交易编号（第三方支付用)',
    `trade_body` string   COMMENT '订单描述(第三方支付用)',
    `create_time` string   COMMENT '创建时间',
    `operate_time` string   COMMENT '操作时间',
    `expire_time` string   COMMENT '失效时间',
    `tracking_no` string   COMMENT '物流单编号',
    `parent_order_id` bigint   COMMENT '父订单编号',
    `img_url` string   COMMENT '图片路径',
    `province_id` int   COMMENT '地区',
    `benefit_reduce_amount` decimal(16,2)   COMMENT '优惠金额',
    `original_total_amount` decimal(16,2)   COMMENT '原价金额',
    `feight_fee`decimal(16,2) COMMENT '运费'
)
    row format delimited fields terminated by '\t'
    location '/2207A/runbo_zhang/gmall/ods/ods_order_info_tem'
    ;


-- load data inpath '/2207A/runbo_zhang/gmall/yewu/order_info' overwrite into table ods_order_info_tem;
select * from ods_order_info_tem;

insert into ods_order_info
select *,create_time from ods_order_info_tem;
select *from ods_order_info;



--todo ods_order_detail、

-- drop table if exists ods_order_detail;
create table if not exists ods_order_detail(
   `id` bigint   COMMENT '编号',
   `order_id` bigint  COMMENT '订单编号',
   `sku_id` bigint  COMMENT 'sku_id',
   `sku_name` string  COMMENT 'sku名称（冗余)',
   `img_url` string  COMMENT '图片名称（冗余)',
   `order_price` decimal(10,2)  COMMENT '购买价格(下单时sku价格）',
   `sku_num` string  COMMENT '购买个数',
   `create_time` string  COMMENT '创建时间',
   `source_type` string  COMMENT '来源类型',
   `source_id` bigint  COMMENT '来源编号'
)partitioned by (dt string)
row format delimited fields terminated by '\t'
location '/2207A/runbo_zhang/gmall/ods/ods_order_detail'
    ;



-- drop table if exists ods_order_detail_tem;
create temporary table if not exists ods_order_detail_tem(
 `id` bigint   COMMENT '编号',
 `order_id` bigint  COMMENT '订单编号',
 `sku_id` bigint  COMMENT 'sku_id',
 `sku_name` string  COMMENT 'sku名称（冗余)',
 `img_url` string  COMMENT '图片名称（冗余)',
 `order_price` decimal(10,2)  COMMENT '购买价格(下单时sku价格）',
 `sku_num` string  COMMENT '购买个数',
 `create_time` string  COMMENT '创建时间',
 `source_type` string  COMMENT '来源类型',
 `source_id` bigint  COMMENT '来源编号'
)
row format delimited fields terminated by '\t'
location '/2207A/runbo_zhang/gmall/ods/ods_order_detail_tem'
    ;

-- load data inpath '/2207A/runbo_zhang/gmall/yewu/order_detail' overwrite into table ods_order_detail_tem;
-- select *from ods_order_detail_tem;

insert into ods_order_detail
select *,create_time from ods_order_detail_tem;
select *from ods_order_detail;
--todo ods_sku_info、

-- drop table if exists ods_sku_info;
create table if not exists ods_sku_info(
                                           `id` bigint   COMMENT 'skuid(itemID)',
                                           `spu_id` bigint  COMMENT 'spuid',
                                           `price` decimal(10,0)  COMMENT '价格',
                                           `sku_name` string  COMMENT 'sku名称',
                                           `sku_desc` string  COMMENT '商品规格描述',
                                           `weight` decimal(10,2)  COMMENT '重量',
                                           `tm_id` bigint  COMMENT '品牌(冗余)',
                                           `category3_id` bigint  COMMENT '三级分类id（冗余)',
                                           `sku_default_img` string  COMMENT '默认显示图片(冗余)',
                                           `create_time` string  COMMENT '创建时间'
)partitioned by (dt string)
    row format delimited fields terminated by '\t'
    location '/2207A/runbo_zhang/gmall/ods/ods_sku_info'
    ;



-- drop table if exists ods_sku_info_tem;
create temporary table if not exists ods_sku_info_tem(
 `id` bigint   COMMENT 'skuid(itemID)',
 `spu_id` bigint  COMMENT 'spuid',
 `price` decimal(10,0)  COMMENT '价格',
 `sku_name` string  COMMENT 'sku名称',
 `sku_desc` string  COMMENT '商品规格描述',
 `weight` decimal(10,2)  COMMENT '重量',
 `tm_id` bigint  COMMENT '品牌(冗余)',
 `category3_id` bigint  COMMENT '三级分类id（冗余)',
 `sku_default_img` string  COMMENT '默认显示图片(冗余)',
 `create_time` string  COMMENT '创建时间'
)
    row format delimited fields terminated by '\t'
    location '/2207A/runbo_zhang/gmall/ods/ods_sku_info_tem'
    ;

-- load data inpath '/2207A/runbo_zhang/gmall/yewu/sku_info' overwrite into table ods_sku_info_tem;
select *from ods_sku_info_tem;

insert into ods_sku_info
select *,create_time from ods_sku_info_tem;
select *from ods_sku_info;




--todo ods_user_info。
-- drop table if exists ods_user_info;
create table if not exists ods_user_info(
`id` bigint   COMMENT '编号',
`login_name` string   COMMENT '用户名称',
`nick_name` string   COMMENT '用户昵称',
`passwd` string   COMMENT '用户密码',
`name` string   COMMENT '用户姓名',
`phone_num` string   COMMENT '手机号',
`email` string   COMMENT '邮箱',
`head_img` string   COMMENT '头像',
`user_level` string   COMMENT '用户级别',
`birthday` string  COMMENT '用户生日',
`gender` string  COMMENT '性别 M男,F女',
`create_time` string  COMMENT '创建时间',
`operate_time` string  COMMENT '修改时间'
)partitioned by (dt string)
    row format delimited fields terminated by '\t'
    location '/2207A/runbo_zhang/gmall/ods/ods_user_info'
    ;



-- -- drop table if exists ods_user_info_tem;
create temporary table if not exists ods_user_info_tem(
`id` bigint   COMMENT '编号',
`login_name` string   COMMENT '用户名称',
`nick_name` string   COMMENT '用户昵称',
`passwd` string   COMMENT '用户密码',
`name` string   COMMENT '用户姓名',
`phone_num` string   COMMENT '手机号',
`email` string   COMMENT '邮箱',
`head_img` string   COMMENT '头像',
`user_level` string   COMMENT '用户级别',
`birthday` string  COMMENT '用户生日',
`gender` string  COMMENT '性别 M男,F女',
`create_time` string  COMMENT '创建时间',
`operate_time` string  COMMENT '修改时间'
)
    row format delimited fields terminated by '\t'
    location '/2207A/runbo_zhang/gmall/ods/ods_user_info_tem'
    ;

load data inpath '/2207A/runbo_zhang/gmall/yewu/user_info' overwrite into table ods_user_info_tem;
select *from ods_user_info_tem;

insert into ods_user_info
select *,create_time from ods_user_info_tem;
select *from ods_user_info;



--         要求每张表：
-- 1. 需要按天分区 （3分）
-- 2. 数表过程自主使用数据存储格式、压缩格式。（3分）
-- 3.  列的分隔使用’\t’。（3分）
-- 4.  指定存储位置为 ‘/2207A/runbo_zhang/gmall/ods/表名’ (3分)
-- 5.数据加载到ODS层表（5分）
-- 6. 数据加载完整（3分）
select *from ods_order_info;
select *from ods_order_detail;
select *from ods_sku_info;
select *from ods_user_info;

-- 7.数据格式符合要求（2分）
