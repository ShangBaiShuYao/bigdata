
-- 需求一：热门品类Top10
CREATE TABLE `category_top10` (
  `taskId` text COMMENT '任务标记',
  `category_id` text COMMENT '品类id',
  `click_count` bigint(20) DEFAULT NULL COMMENT '点击次数',
  `order_count` bigint(20) DEFAULT NULL COMMENT '下单次数',
  `pay_count` bigint(20) DEFAULT NULL   COMMENT '支付次数'
) ENGINE=InnoDB DEFAULT CHARSET=utf8