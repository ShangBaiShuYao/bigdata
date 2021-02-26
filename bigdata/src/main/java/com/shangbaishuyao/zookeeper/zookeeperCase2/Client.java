package com.shangbaishuyao.zookeeper.zookeeperCase2;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

/**
 * Desc: <p>表示客户端， 实时的获取服务器的节点信息，并且监听变化</p>
 * create by shangbaishuyao on 2021/2/26
 * @Author: 上白书妖
 * @Date: 10:36 2021/2/26
 */
public class Client {
	private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
	private int sessionTimeout = 10000 ;
	private ZooKeeper zkClient ;
	private String  parentPath = "/servers";
	
	
	public static void main(String[] args) throws Exception {
		
		Client client = new Client();
		
		//1. 获取到Zookeeper的客户端对象
		client.getZookeeperClient();
		
		//2. 获取当前正在线上的服务器节点，并监听
		
		client.getOnlineNodeAndListener();
		
		//3. 保持与Zookeeper的连接状态
		Thread.sleep(Long.MAX_VALUE);
		
	}
	
	private  void  getOnlineNodeAndListener() throws Exception {
		List<String> nodes = zkClient.getChildren(parentPath, new Watcher() {
			public void process(WatchedEvent event) {
				try {
					getOnlineNodeAndListener();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
		System.out.println("当前可用的服务器节点为:  " + nodes );
	}

	private void getZookeeperClient() throws Exception {
		
		zkClient  = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
			}
		});
	}
}
