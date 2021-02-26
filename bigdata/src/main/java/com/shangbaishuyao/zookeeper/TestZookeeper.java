package com.shangbaishuyao.zookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
/*
 * Desc: 测试 <br/>
 * create by shangbaishuyao on 2021/2/26
 * @Author: 上白书妖
 * @Date: 12:53 2021/2/26
 */
public class TestZookeeper {
	
	private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
	
	/**
	 * minSessionTimeout=4000
	   maxSessionTimeout=40000
	 */
	private int sessionTimeout = 10000 ;
	
	
	private ZooKeeper zkClient  ; 
	
	/** 
	 * 获取Znode的数据 
	 * 修改Znode的数据
	 * 
	 * 删除Znode
	 */
	@Test
	public void testZnodeData() throws Exception {
//		byte[] datas = zkClient.getData("/shangbaishuyao", false, null);
//		System.out.println(new String(datas));
		
	//	zkClient.setData("/shangbaishuyao", "sgg".getBytes(), -1);
		
		zkClient.delete("/0615", 1);
	}
	
	
	/**
	 * 判断子节点是否存在
	 */
	@Test
	public void testZondeIsExists() throws Exception {
		Stat stat = zkClient.exists("/shangbaishuyao", false);
		if(stat == null) {
			System.out.println("Znode不存在");
		}else {
			System.out.println("Znode存在");
		}
	}
	
	/**
	 * 获取子节点 并监听
	 */
	@Test
	public void testLsNodeAndListener() throws Exception {
		
		List<String> nodes = zkClient.getChildren("/", true);
		System.out.println("nodes: " + nodes );
		
		
		Thread.sleep(Long.MAX_VALUE);
	}
	
	/**
	 * 创建子节点
	 */
	@Test
	public void testCreateZnode() throws Exception {
		
		String path = 
				zkClient.create("/shangbaishuyao","shangguigu".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT	) ;
		System.out.println(path + " is Created!!!!");
	}
	
	
	
	/**
	 * 获取Zookeeper客户端对象
	 */
	@Before
	public void testZookeeperClient()  throws Exception{
		
		   zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			public void process(WatchedEvent event) {
				System.out.println("========================");
			}
		} );
		
		//System.out.println("zkClient:" + zkClient);
	}
}
