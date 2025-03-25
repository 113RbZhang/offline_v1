set hive.exec.mode.local.auto  =true;

use dev_realtime_v1_runbo_zhang;
--①　GMV成交总额 （gmv=销售额+取消订单金额+拒收订单金额+退货订单金额。）  （8分）
drop table if exists ads_gmv;
create table if not exists ads_gmv(
    amount decimal(16,2)

)    row format delimited fields terminated by '\t'
    
    location '/2207A/runbo_zhang/gmall/ads/ads_gmv';



insert into table ads_gmv  select *from dws_gmv;
select *from ads_gmv;
-- ②　转化率（用户行为漏斗分析）（8分）
drop table if exists ads_zhl;
create table if not exists ads_zhl(
                                      t1 decimal(16,2) comment '1001',
                                      t2 decimal(16,2) comment '1002',
                                      t3 decimal(16,2) comment '1003',
                                      t4 decimal(16,2) comment '1004',
                                      t5 decimal(16,2) comment '1005'
)row format delimited fields terminated by '\t'
    
    location '/2207A/runbo_zhang/gmall/ads/ads_zhl';
insert into ads_zhl
select
    t2/t1,
    t3/t2,
    t4/t3,
    t5/t4,
    t6/t5
    from dws_zhl;
select *from ads_zhl;


-- ③　品牌复购率（复购率计算分析）（8分）
create table if not exists ads_fgl(
                                      id string,
                                      fgl bigint comment '复购率'
)row format delimited fields terminated by '\t'
    
    location '/2207A/runbo_zhang/gmall/ads/ads_fgl';
insert into ads_fgl
select *from dws_fgl;
select *from ads_fgl;
-- ④　数据存储到Hive的ADS层表。（5分）


select *from ads_fgl  ;
select *from ads_zhl  ;
select *from ads_gmv  ;