package zktest;

import java.io.IOException;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Master implements Watcher{
	ZooKeeper zk;
	String hostPort;
	Random random=new Random();
	String serverId= Integer.toHexString(random.nextInt());
	static boolean isLeader=false;
	
	Master(String hostPort){
		this.hostPort=hostPort;
	}
	void startZk() {
		try {
			zk=new ZooKeeper(hostPort, 15000, this);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void process(WatchedEvent e) {
		System.out.println(e);
	}
	public void stopZk() throws InterruptedException {
		zk.close();
	}
//	public void rumForMaster() throws KeeperException, InterruptedException {
//		zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//	}
	public boolean checkMaster() throws KeeperException, InterruptedException {
		while(true) {
			try {
				Stat stat=new Stat();
				byte data[]=zk.getData("master", false, stat);
				isLeader=new String(data).equals(serverId);
				return true;
			}catch(NoNodeException e) {
				return false;
			}catch(ConnectionLossException e) {
				
			}
		}
	}
	public void runForMaster() throws KeeperException, InterruptedException {
		while(true) {
			try {
				zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
				isLeader=true;
				break;
			}catch(NodeExistsException e) {
				
			}catch(ConnectionLossException e) {
				
			}
			if(checkMaster())break;
			
		}
	}
	public static void main(String[] args) throws InterruptedException, KeeperException {
		Master master=new Master("127.0.0.1:2181");
		master.startZk();
		master.runForMaster();
		if(isLeader) {
			System.out.println("i'm leader");
			Thread.sleep(60000);
		}else {
			System.out.println("someone else is leader");
		}
		
		master.stopZk();
	}
}
