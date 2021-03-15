package com.shangbaishuyao.hive;
/**
 * Desc:
 * create by shangbaishuyao on 2021/3/4
 * @Author: 上白书妖
 * @Date: 17:41 2021/3/4
 *
 * 清洗数据:
 *   1. 判断切割后的数据长度是否大于等于9 
 *   2. 将视频类型字段中的空格去掉
 *   3. 将关联视频通过&符号拼接到一起
 */
public class ETLUtils {
	/**
	 * 一行数据:
	 * 
	 * fQShwYqGqsw	lonelygirl15	736	People & Blogs	133	151763	3.01	666	765	fQShwYqGqsw	LfAaY1p_2Is	5LELNIVyMqo	vW6ZpqXjCE4	vPUAf43vc-Q	ZllfQZCc2_M	it2d7LaU_TA	KGRx8TgZEeU	aQWdqI1vd6o	kzwa8NBlUeo	X3ctuFCCF5k	Ble9N2kDiGc	R24FONE2CDs	IAY5q60CmYY	mUd0hcEnHiU	6OUcp6UJ2bA	dv0Y_uoHrLc	8YoxhsUMlgA	h59nXANN-oo	113yn3sv0eo
	 */
	public static String  etlData(String line ) {
		//1. 通过 \t 切割数据
		String [] splits =  line.split("\t");
		
		//2. 判断长度
		if(splits.length < 9 ) {
			return null ; 
		}
		
		//3.将视频类型字段中的空格去掉
		splits[3]=splits[3].replaceAll(" ", "");
		
		//4. 将关联视频通过&符号拼接到一起
		StringBuffer sbs = new StringBuffer();
		
		for(int i= 0 ; i < splits.length ;i ++) {
			//假设没有关联视频的情况
			if(i < 9 ) {
				if(i == splits.length-1) {
					sbs.append(splits[i]);
				}else {
					sbs.append(splits[i]).append("\t");
				}
				//有关联视频的情况
			}else {
				if(i == splits.length-1) {
					sbs.append(splits[i]);
				}else {
					sbs.append(splits[i]).append("&");
				}
			}
		}
		
		
		return  sbs.toString();
	}
	
	
	public static void main(String[] args) {
		
		String str = etlData("fQShwYqGqsw	lonelygirl15	736	People & Blogs	133	151763	3.01	666");
		
		System.out.println(str);
		
	}
}
