-- 需求三：页面单跳转化率
CREATE TABLE `jump_page_ratio` (
  `task_id` text COMMENT '任务id',
  `page_jump` VARCHAR(20) COMMENT '页面跳转',
  `ratio` DECIMAL(10,3) COMMENT '比例'
) ENGINE=InnoDB DEFAULT CHARSET=utf8