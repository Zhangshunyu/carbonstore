SELECT gen_harmonized_1_item.`gen_tag_62877`, gen_harmonized_1_item.`prod_size`, sum(store_sales.`ss_sales_price`) AS `sum(ss_sales_price)`, gen_harmonized_1_item.`prod_name`, sum(store_sales.`ss_sales_price`) AS `sum(ss_sales_price)` 
FROM
  store_sales
  LEFT OUTER JOIN (SELECT 1 AS `gen_tag_62877`, item.`i_item_id`, first(item.`i_product_name`, false) AS `prod_name`, first(item.`i_size`, false) AS `prod_size` 
  FROM
    item
  GROUP BY item.`i_item_id`) gen_harmonized_1_item  ON (store_sales.`ss_item_sk` = CAST(gen_harmonized_1_item.`i_item_id` AS INT))
GROUP BY gen_harmonized_1_item.`gen_tag_62877`, gen_harmonized_1_item.`prod_size`, gen_harmonized_1_item.`prod_name`;

