for i in activity_info base_category3\
                                base_dic\
                                cart_info\
                                order_detail\
                                order_info\
                                sku_info\
                                spu_info\
                                user_info\

do
sqoop import --connect jdbc:mysql://192.168.58.1/gmall \
      --username root --password root \
      --table $i \
      --target-dir /2207A/runbo_zhang/gmall/yewu/$i \
      -m 1 -z --compression-codec lzop \
      --delete-target-dir \
      --null-string '\\N' --null-non-string '\\N' \
      --fields-terminated-by '\t' --lines-terminated-by '\n'
done







