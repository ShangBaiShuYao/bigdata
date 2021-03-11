-- 需求：各区域点击量Top3 商品
CREATE TABLE `area_count_info` (
  `task_id` text COMMENT '任务id',
  `area` VARCHAR(20) COMMENT '地区',
  `product_name` VARCHAR(20) COMMENT '商品名称',
  `product_count` BIGINT COMMENT '点击次数',
  `city_click_ratio` VARCHAR(200) COMMENT '城市备注'
) ENGINE=InnoDB DEFAULT CHARSET=utf8