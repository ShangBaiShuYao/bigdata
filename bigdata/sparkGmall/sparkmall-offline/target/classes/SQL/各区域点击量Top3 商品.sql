//需求四:各区域点击量Top3 商品
需求分析: 按大区计算商品被点击的次数前3名，并计算出前2名占比及其他城市综合占比，结果数据展示：

地区	商品名称 	点击次数		 城市备注
华北	商品A	    100000		 北京21.2%，天津13.2%，其他65.6%
华北	商品P	    80200		 北京63.0%，太原10%，其他27.0%
华北	商品M	    40000		 北京63.0%，太原10%，其他27.0%



-------------------------------------------离线数据生成完成表的部分展示-----------------------------------------------------------------------
保存：city_info完成
+-------+---------+----+
|city_id|city_name|area|
+-------+---------+----+
|      7|     天津|  华北|
|      8|     成都|  西南|


保存：product_info完成
+----------+------------+-----------+
|product_id|product_name|extend_info|
+----------+------------+-----------+
|         1|        商品_1|      自营|
|         2|        商品_2|    第三方|
|         3|        商品_3|      自营|


保存：user_info完成
+-------+--------+--------+---+------------+------+
|user_id|username|    name|age|professional|gender|
+-------+--------+--------+---+------------+------+
|      1|  user_1|  name_1| 25|      程序员|     女|
|      2|  user_2|  name_2| 54|        学生|     男|
|      3|  user_3|  name_3| 46|        经理|     女|


保存：user_visit_action完成
+----------+-------+--------------------+-------+-------------------+--------------+-----------------+----------------+------------------+-----------------+----------------+---------------+-------+
|      date|user_id|          session_id|page_id|        action_time|search_keyword|click_category_id|click_product_id|order_category_ids|order_product_ids|pay_category_ids|pay_product_ids|city_id|
+----------+-------+--------------------+-------+-------------------+--------------+-----------------+----------------+------------------+-----------------+----------------+---------------+-------+
|2019-11-26|     68|afc49de6-5d18-431...|     25|2019-11-26 00:00:00|            苹果|               -1|              -1|              null|             null|            null|           null|     21|
|2019-11-26|     68|afc49de6-5d18-431...|     25|2019-11-26 00:00:02|          null|               18|              45|              null|             null|            null|           null|     14|

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


需求分析SQL步骤

//①这样写没有错,但是click_product_id这里面有许多等于-1的值,所以我们得过滤
select  
	area, 
	product_name,
	city_name
from
	user_visit_action uv                   //这里面保留的保留,条件过滤的过滤即过滤掉-1的: select click_product_id,city_id from user_visit_action where click_product_id>0 uv
join
    product_info pi
on
    uv.click_product_id = pi.product_id
join
    city_info ci
on  
    uv.city_id = ci.city_id


//②过滤掉-1 的
select  
	area, 
	product_name,
	city_name
from
	(select click_product_id,city_id from user_visit_action where click_product_id>0) uv                   //这里面保留的保留,条件过滤的过滤即过滤掉-1的: select click_product_id,city_id from user_visit_action where click_product_id>0 uv
join
    product_info pi
on
    uv.click_product_id = pi.product_id
join
    city_info ci
on  
    uv.city_id = ci.city_id ; t1 //将三表查出来的area(地区),product_name(产品名称),city_name(城市名称) 的表叫做t1表,这是全量信息



//假如我自定义一个函数a, 我这个a在③这个sql里面添加进去, 我不管他排名情况,把所有商品的城市排行都算出来.因为只有②是按照大区和商品进行分组的.

//③计算点击次数
select  
	area, 
	product_name,
	count(*) ct
from
    t1
group by
    area,product_name;t2   //到了这一步为止,我们就能将数据集算出来了,但是我们还没有取前三名



//④排序
select 
    area,
    product_name,
    ct,
    rank() over(partition by area order by ct desc) rk
from 
    t2; t3


//⑤获取前三名
select 
    area,
    product_name,
    ct
from 
    t3
where
    rk <= 3;











