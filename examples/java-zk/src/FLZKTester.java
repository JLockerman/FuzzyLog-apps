import client.ProxyClient;

import java.util.List;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

public class FLZKTester
{
	public static void main(String[] args) throws Exception
	{
		if(args.length<1)
		{
			System.out.println("Usage: java FLZK [proxyport]");
			System.exit(0);
		}
		ProxyClient client = new ProxyClient(Integer.parseInt(args[0]));
		IZooKeeper zk = new FLZK(client, null);
		
		System.out.println("hello world!");

		if(zk.exists("/abcd",  true)==null)
			zk.create("/abcd", "AAA".getBytes(), null, CreateMode.PERSISTENT);
		zk.setData("/abcd", "ABCD".getBytes(), -1);
		System.out.println(zk);
		Thread.sleep(1000);
		int numops = 1;
		//synchronous testing
		for(int i=0;i<100;i++)
			zk.create("/abcd/" + i, "AAA".getBytes(), null, CreateMode.PERSISTENT);
		System.out.println(zk);
			
		for(int i=0;i<100;i++)
			zk.setData("/abcd/" + i, "BBB".getBytes(), -1);
		System.out.println(zk);
			
		for(int i=0;i<100;i++)
			zk.exists("/abcd/" + i, false);
		System.out.println(zk);
			
		for(int i=0;i<100;i++)
			zk.delete("/abcd/" + i, -1);

			
//		zk.setData("/abcd", "ABCD".getBytes(), -1);
		System.out.println(zk);
		System.out.println("Test done");
		System.exit(0);
	}	
}
