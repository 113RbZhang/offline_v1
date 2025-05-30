for i in activity_info   \
         activity_rule   \
         activity_sku   \
         base_attr_info   \
         base_attr_value   \
         base_category1   \
         base_category2   \
         base_category3   \
         base_category_view   \
         base_dic   \
         base_frontend_param   \
         base_province   \
         base_region   \
         base_sale_attr   \
         base_trademark   \
         cart_info   \
         cms_banner   \
         comment_info   \
         coupon_info   \
         coupon_range   \
         coupon_use   \
         favor_info   \
         financial_sku_cost   \
         order_detail   \
         order_detail_activity   \
         order_detail_coupon   \
         order_info   \
         order_refund_info   \
         order_status_log   \
         payment_info   \
         refund_payment   \
         seckill_goods   \
         sku_attr_value   \
         sku_image   \
         sku_info   \
         sku_sale_attr_value   \
         spu_image   \
         spu_info   \
         spu_poster   \
         spu_sale_attr   \
         spu_sale_attr_value   \
         user_address   \
         user_info   \
         ware_info   \
         ware_order_task   \
         ware_order_task_detail   \
         ware_sku
do
  sqoop import --connect jdbc:mysql://cdh03:3306/gmall \
        --username root --password root \
        --table $i \
        --target-dir /2207A/gmall/data/yewu/$i \
        -m 1 -z --compression-codec lzop  \
        --delete-target-dir \
       --null-string '\\N' --null-non-string '\\N' \
        --fields-terminated-by '\t' --lines-terminated-by '\n'
done

