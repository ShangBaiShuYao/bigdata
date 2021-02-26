package com.shangbaishuyao.zookeeper.TestZookeeperAPI;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * Desc:
 * create by shangbaishuyao on 2021/2/26
 * @Author: 上白书妖
 * @Date: 12:53 2021/2/26
 *
 *  表示服务器的节点
 *  
 *  	/servers 
 *  		/server1
 *  	    /server2
 *
 */
public class Server {
	private String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
	private int sessionTimeout = 10000 ;
	private ZooKeeper zkClient ;
	
	private String  parentPath = "/servers";
	
	public static void main(String[] args) throws Exception {
		
		Server server = new Server();
		
		//1. 获取到Zookeeper的客户端对象
		server.getZookeeperClient();
		
		//2. 判断父节点是否存在
		server.checkParentNodeIsExists();
		
		//3. 将当前的节点信息注册到Zookeeper中
		server.registServer(args[0]);
		
		//4. 保持与Zookeeper的连接
		Thread.sleep(Long.MAX_VALUE);
	}

	private void registServer(String data) throws Exception {
		String path = 
				zkClient.create(parentPath+"/server", data.getBytes() , Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		
		System.out.println(path + " is on line***************" + data);
	}

	private void checkParentNodeIsExists() throws Exception {
		Stat stat = zkClient.exists(parentPath, false);
		if(stat == null) {
			//创建父节点
			zkClient.create(parentPath, "servers".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
	}

	private void getZookeeperClient() throws Exception {
		
		zkClient  = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				
			}
		});
	}
}
