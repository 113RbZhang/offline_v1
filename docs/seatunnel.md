## 下载安装包
进入SeaTunnel下载页面 下载最新版本的二进制安装包seatunnel-<version>-bin.tar.gz
然后解压 
ar -xzvf "apache-seatunnel-${version}-bin.tar.gz"
## 下载连接器插件
sh bin/install-plugin.sh -- 直接安装
sh bin/install-plugin.sh 2.3.9  --指定版本安装
指定下载插件
你可以修改plugin_config配置文件，如下所示：

--seatunnel-connectors--
connector-fake
connector-console
--end--
在/etc/profile中配置环境变量
![img_1.png](img_1.png)

export SEATUNNEL HOME=/opt/bigdata/seatunnel-2.3.3/backend/apache-seatunnel-2.3.3
export PATH $SEATUNNEL HOME/bin:$PATH


# web配置
https://www.apache.org/dyn/closer.lua/seatunnel/seatunnel-web/1.0.0/apache-seatunnel-web-1.0.0-bin.tar.gz
下载并解压
tar -zxvf apache-seatunnel-web-1.0.0-bin.tar.gz


`在seatunnel的本机下载web，不进行分布式，不需要额外配置`

vim apache-seatunnel-web-1.0.0-bin/script/seatunnel_server_env.sh
修改mysql库配置
export HOSTNAME="localhost"
export PORT="3306"
export USERNAME="root"
export PASSWORD="123456"



origin/main
