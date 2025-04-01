set hive.exec.mode.local.auto=true;

use dev_realtime_defence_v1_runbo_zhang;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

create external table if not exists dwd_order_detail(
                                               `id` bigint   COMMENT '编号',
                                               `order_id` bigint  COMMENT '订单编号',
                                               `sku_id` bigint  COMMENT 'sku_id',
                                               `sku_name` string  COMMENT 'sku名称（冗余)',
                                               `img_url` string  COMMENT '图片名称（冗余)',
                                               `order_price` decimal(10,2)  COMMENT '购买价格(下单时sku价格）',
                                               `sku_num` string  COMMENT '购买个数',
                                               `create_time` string  COMMENT '创建时间',
                                               `source_type` string  COMMENT '来源类型',
                                               `source_id` bigint  COMMENT '来源编号',
                                               amount decimal(10,2) comment '单价*个数'

)partitioned by (dt string)
    row format delimited fields terminated by '\t'
    location '/2207A/runbo_zhang/gmall/dwd/dwd_order_detail';

insert into table dwd_order_detail partition (dt)
select
    id,
    order_id,
    sku_id     ,
    sku_name   ,
    img_url    ,
    order_price,
    sku_num    ,
    create_time,
    source_type,
    source_id  ,
    order_price*sku_num,
    create_time as dt

from ods_order_detail;



select *from dwd_order_detail;




--todo dwd_order_info、
create external  table if not exists dwd_order_info(
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
                                             `feight_fee`decimal(16,2) COMMENT '运费',
                                             amount decimal(16,2) comment '除去运费的金额'


)partitioned by (dt string)
    row format delimited fields terminated by '\t'
    location '/2207A/runbo_zhang/gmall/dwd/dwd_order_info';
insert into dwd_order_info  partition (dt)
select
    id,
    consignee,
    consignee_tel        ,
    total_amount   ,
    order_status         ,
    user_id              ,
    delivery_address     ,
    order_comment        ,
    out_trade_no         ,
    trade_body           ,
    create_time          ,
    operate_time         ,
    expire_time          ,
    tracking_no          ,
    parent_order_id      ,
    img_url              ,
    province_id          ,
    coupon_reduce_amount,
    original_total_amount,
    feight_fee           ,
    total_amount-ods_order_info.feight_fee,
    create_time as dt

from ods_order_info;
select *from dwd_order_info;

--todo dwd_user_info、

-- drop table if exists dwd_user_info;
create external  table if not exists dwd_user_info(

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
                                            `operate_time` string  COMMENT '修改时间',
                                            `order_id`string comment '订单id',
                                            `order_status` string   COMMENT '订单状态',-- 漏斗分析
                                            `order_amount` decimal(16,2)   COMMENT '下单金额',
                                            `order_type` string   COMMENT '下单品牌'-- 复购率


)partitioned by (dt string)
    row format delimited fields terminated by '\t'
    location '/2207A/runbo_zhang/gmall/dwd/dwd_user_info';

insert into dwd_user_info  partition (dt)
select
    oui.id,
    oui.login_name,
    oui.nick_name   ,
    oui.password      ,
    oui.name        ,
    oui.phone_num   ,
    oui.email       ,
    oui.head_img    ,
    oui.user_level  ,
    oui.birthday    ,
    oui.gender      ,
    oui.create_time ,
    oui.operate_time,
    ooi.id,
    ooi.order_status,
    ooi.total_amount,
    ood.sku_id,


    ooi.create_time as dt
from ods_order_info ooi
         left join ods_user_info oui
                   on ooi.user_id=oui.id
         left  join ods_order_detail ood
                    on ood.order_id=ooi.id;

select *from dwd_user_info;



--todo dwd_sku_info 表,要求如下:

create external  table if not exists dwd_sku_info(

                                           `id` bigint   COMMENT 'skuid(itemID)',
                                           `spu_id` bigint  COMMENT 'spuid',
                                           `price` decimal(10,0)  COMMENT '价格',
                                           `sku_name` string  COMMENT 'sku名称',
                                           `sku_desc` string  COMMENT '商品规格描述',
                                           `weight` decimal(10,2)  COMMENT '重量',
                                           `tm_id` bigint  COMMENT '品牌(冗余)',
                                           `category3_id` bigint  COMMENT '三级分类id（冗余)',
                                           `sku_default_img` string  COMMENT '默认显示图片(冗余)',
                                           `create_time` string  COMMENT '创建时间',
                                           type string comment '分类'


)partitioned by (dt string)
    row format delimited fields terminated by '\t'
    location '/2207A/runbo_zhang/gmall/dwd/dwd_sku_info';
insert into dwd_sku_info partition (dt)
select
    id             ,
    spu_id         ,
    price          ,
    sku_name       ,
    sku_desc       ,
    weight         ,
    tm_id          ,
    category3_id   ,
    sku_default_img,
    create_time    ,
    category3_id,
    create_time as dt
from ods_sku_info;



select *from dwd_sku_info;