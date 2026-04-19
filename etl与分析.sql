create table myhive.stu(id int,
name string
);
insert into myhive.stu values (1,'zhou'),(2,'lin');
select * from myhive.stu;
create table myhive.stu2(id int, name string) row format delimited fields terminated by '\t';
insert into myhive.stu2 values (1,'zhou'),(2,'lin');
set hive.enforce.bucketing=true;
show functions ;
desc function extended lcase;
create database db_msg;
use db_msg;
-- 如果表已存在就删除
drop table if exists db_msg.tb_msg_source ;

-- 建表
create table db_msg.tb_msg_source(
                                     msg_time string comment "消息发送时间",
                                     sender_name string comment "发送人昵称",
                                     sender_account string comment "发送人账号",
                                     sender_sex string comment "发送人性别",
                                     sender_ip string comment "发送人ip地址",
                                     sender_os string comment "发送人操作系统",
                                     sender_phonetype string comment "发送人手机型号",
                                     sender_network string comment "发送人网络类型",
                                     sender_gps string comment "发送人的GPS定位",
                                     receiver_name string comment "接收人昵称",
                                     receiver_ip string comment "接收人IP",
                                     receiver_account string comment "接收人账号",
                                     receiver_os string comment "接收人操作系统",
                                     receiver_phonetype string comment "接收人手机型号",
                                     receiver_network string comment "接收人网络类型",
                                     receiver_gps string comment "接收人的GPS定位",
                                     receiver_sex string comment "接收人性别",
                                     msg_type string comment "消息类型",
                                     distance string comment "双方距离",
                                     message string comment "消息内容"
);

load data inpath '/chatdemo/data/chat_data-30W.csv' into table tb_msg_source;

select * from tb_msg_source tablesample ( 100 rows );

select count(*) from tb_msg_source;
insert overwrite table db_msg.tb_msg_etl
select *,date(msg_time) as msg_day,hour(msg_time) as mag_hour,split(sender_gps,',')[0] as sender_lng,split(sender_gps,',')[1] as sender_lat
     from tb_msg_source where length(sender_gps)>0;

drop table if exists db_msg.tb_msg_etl;
create table db_msg.tb_msg_etl(
                                      msg_time string comment "消息发送时间",
                                      sender_name string comment "发送人昵称",
                                      sender_account string comment "发送人账号",
                                      sender_sex string comment "发送人性别",
                                      sender_ip string comment "发送人ip地址",
                                      sender_os string comment "发送人操作系统",
                                      sender_phonetype string comment "发送人手机型号",
                                      sender_network string comment "发送人网络类型",
                                      sender_gps string comment "发送人的GPS定位",
                                      receiver_name string comment "接收人昵称",
                                      receiver_ip string comment "接收人IP",
                                      receiver_account string comment "接收人账号",
                                      receiver_os string comment "接收人操作系统",
                                      receiver_phonetype string comment "接收人手机型号",
                                      receiver_network string comment "接收人网络类型",
receiver_gps string comment "接收人的GPS定位",
receiver_sex string comment "接收人性别",
msg_type string comment "消息类型",
distance string comment "双方距离",
message string comment "消息内容",
msg_day string comment "消息日",
msg_hour string comment "消息小时",
    sender_lng double comment "经度",
    sender_lat double comment "纬度"

);

drop table if exists db_msg.tb_rs_total_msg_cnt;
create table db_msg.tb_rs_total_msg_cnt comment '每日信息总量' as
select msg_day,count(*) as total_msg_cnt
from tb_msg_etl group by msg_day;

drop table if exists db_msg.tb_rs_hour_msg_cnt;
create table db_msg.tb_rs_hour_msg_cnt comment '每小时消息趋势' as
select
    msg_hour,
    count(*) as total_msg_cnt,
    count(distinct sender_account) as sender_user_cnt,
    count(distinct receiver_account) as receiver_user_cnt
    from db_msg.tb_msg_etl group by msg_hour;

drop table if exists db_msg.tb_rs_loc_cnt;
create table db_msg.tb_rs_loc_cnt comment '每日地区发送消息总量' as
select
    msg_day,sender_lng,sender_lat,count(*) as total_msg_cnt
from db_msg.tb_msg_etl
group by msg_day,sender_lng,sender_lat;

drop table if exists db_msg.tb_rs_user_cnt;
create table db_msg.tb_rs_user_cnt comment '收发信息用户总数' as
select
    msg_day,
    count(distinct sender_account) as sender_user_cnt,
    count(distinct receiver_account) as receiver_user_cnt
from db_msg.tb_msg_etl
group by msg_day;

drop table if exists tb_rs_s_user_top10;
create table db_msg.tb_rs_s_user_top10 comment '发送消息最多的10个用户' as
select
    sender_name,
    count(*) as sender_msg_cnt
from db_msg.tb_msg_source group by sender_name
order by sender_msg_cnt desc limit 10;

drop table if exists db_msg.tb_rs_r_user_top10;
create table db_msg.tb_rs_r_user_top10 comment '接收消息最多的10个用户' as
select
    receiver_name,
    count(*) as recceiver_msg_cnt
from db_msg.tb_msg_source group by receiver_name
order by recceiver_msg_cnt desc limit 10;

drop table if exists db_msg.tb_rs_sender_phone;
create table db_msg.tb_rs_sender_phone comment '发送人的手机型号分布' as
select
    sender_phonetype,
    count(*) as cnt
from db_msg.tb_msg_etl group by sender_phonetype;

drop table if exists db_msg.tb_rs_sender_os;
create table db_msg.tb_rs_sender_os comment '发送人的os分布' as
select
    sender_os,
    count(*) as cnt
from db_msg.tb_msg_etl group by sender_os;