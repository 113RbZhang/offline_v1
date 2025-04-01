



drop table if exists dws_gmv;
create table if not exists dws_gmv(
    amount decimal(16,2)

)    row format delimited fields terminated by '\t'

    location '/2207A/runbo_zhang/gmall/dws/dws_gmv';



insert into table dws_gmv  select sum(amount)from dwd_order_info group by ('a');
select *from dws_gmv;


-- ②　转化率（用户行为漏斗分析）（8分）
create table if not exists dws_zhl(
                                      t1 bigint comment '1001',
                                      t2 bigint comment '1002',
                                      t3 bigint comment '1003',
                                      t4 bigint comment '1004',
                                      t5 bigint comment '1005',
                                      t6 bigint comment '1006'
)row format delimited fields terminated by '\t'

    location '/2207A/runbo_zhang/gmall/dws/dws_zhl';
insert into dws_zhl
select
    sum(`if`(order_status=1001,1,0)),
    sum(`if`(order_status=1002,1,0)),
    sum(`if`(order_status=1003,1,0)),
    sum(`if`(order_status=1004,1,0)),
    sum(`if`(order_status=1005,1,0)),
    sum(`if`(order_status=1006,1,0))


from dwd_user_info;
select *from dws_zhl;

-- ③　品牌复购率（复购率计算分析）（8分）
create table if not exists dws_fgl(
                                      id string,
                                      fgl bigint comment '1001'
)row format delimited fields terminated by '\t'

    location '/2207A/runbo_zhang/gmall/dws/dws_fgl';
insert into dws_fgl
select order_type,sum(`if`(num>2,1,0))/count(*)  from (
                                                          select id,order_type,count(*)num
                                                          from dwd_user_info group by order_type, id
                                                      )a
group by order_type;

select *from dws_fgl;

-- （2）DWS层的数据存储格式为orc列式存储lzo压缩格式。（3分）

-- （3）.  依据下单次数和下单金额等，编写SQL语句进行数据装载。（3分）

select *from dws_fgl;
select *from dws_zhl;
select *from dws_gmv;
