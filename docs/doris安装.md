## 下载解压安装包 并重命名
tar -zxvf apache-doris-2.0.0-bin-x64.tar.gz -C /data/soft
mv apache-doris-2.0.0-bin-x64 apache-doris-2.0.0
## 修改fe配置
cd apache-doris-2.0.0/fe
sudo vi conf/fe.conf

priority_networks=192.168.58.0/24
meta_dir=/path/your/doris-meta     //可以默认
## 启动关闭fe
./bin/start_fe.sh --daemon
./bin/stop_fe.sh
通过web浏览器访问http://cdh03:8030
## mysql链接fe
mysql -uroot -P9030 -h cdh03
-- 进入Mysql命令行后，执行下面命令查看FE运行状态
mysql>show frontends\G;
检查isMaster join 和alive是否true;


## be

cd apache-doris-2.0.0/be
vi conf/be.conf

priority_networks=192.168.58.0/24
#配置BE数据存储目录
storage_root_path=/path/your/data_dir
# 由于从 1.2 版本开始支持 Java UDF 函数，BE 依赖于 Java 环境。所以要预先配置 `JAVA_HOME` 环境变量
JAVA_HOME=/usr/java/jdk1.8.0_181-cloudera

## 启动be
./bin/start_be.sh --daemon
./bin/stop_be.sh

报错可能需要修改
### 1
vi /etc/security/limits.conf
* soft nofile 65536
* hard nofile 65536 
### 2
vi /etc/sysctl.conf
vm.max_map_count=2000000
sysctl -p

添加be
mysql -uroot -P9030 -h cdh03
然后
ALTER SYSTEM ADD BACKEND "cdh03:9050";
