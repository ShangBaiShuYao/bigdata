-- 需求二：热门品类中活跃Session
CREATE TABLE `category_session_top10` (
  `taskId` text COMMENT '任务标记',
  `category_id` text COMMENT '品类id',
  `session_id` varchar(200) DEFAULT NULL COMMENT '会话id',
  `click_count` bigint(20) DEFAULT NULL COMMENT '点击次数'
) ENGINE=InnoDB DEFAULT CHARSET=utf8