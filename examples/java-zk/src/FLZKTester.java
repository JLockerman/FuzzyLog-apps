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
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

public class FLZKTester
{
	public static void main(String[] args) throws Exception
	{
		if(args.length<2)
		{
			System.out.println("Usage: java FLZK [hostname] [proxyport]");
			System.exit(0);
		}
		ProxyClient client = new ProxyClient(args[0], Integer.parseInt(args[1]));
		IZooKeeper zk = new FLZK(client, null);
		
		System.out.println("hello world!");

		if(zk.exists("/abcd",  true)==null)
			zk.create("/abcd", "AAA".getBytes(), null, CreateMode.PERSISTENT);
		zk.setData("/abcd", "ABCD".getBytes(), -1);
		System.out.println(zk);
		Thread.sleep(1000);
		int numops = 1;

		long stoptime = 0;
		long starttime = 0;

		//synchronous testing
		int numbatches = 100;
		int batchsize = 1000;
		for(int j=0;j<numbatches;j++)
		{
			starttime = System.currentTimeMillis();
			for(int i=0;i<batchsize;i++)
			{
				try
				{
					zk.create("/abcd/" + i, "AAA".getBytes(), null, CreateMode.PERSISTENT);
				}
				catch(Exception e)
				{
					//fail silently
				}
			}
			stoptime = System.currentTimeMillis();
			System.out.println("Batch [" + j + "/" + numbatches + "] Create done in " + (stoptime-starttime) + ": " + ((double)(stoptime-starttime)/(double)batchsize) + " ms per op.");
		}
	//		System.out.println(zk);
			
		starttime = System.currentTimeMillis();			
		for(int i=0;i<1000;i++)
			zk.setData("/abcd/" + i, "BBB".getBytes(), -1);
		stoptime = System.currentTimeMillis();			
		System.out.println("SetData done in " + (stoptime-starttime));			
//		System.out.println(zk);
		
		starttime = System.currentTimeMillis();
		for(int i=0;i<1000;i++)
			zk.exists("/abcd/" + i, false);
		stoptime = System.currentTimeMillis();		
		System.out.println("Exists done in " + (stoptime-starttime));			
//		System.out.println(zk);
		
		starttime = System.currentTimeMillis();
		for(int i=0;i<1000;i++)
			zk.delete("/abcd/" + i, -1);
		stoptime = System.currentTimeMillis();
//		System.out.println("Delete done in " + (stoptime-starttime));						

			
//		zk.setData("/abcd", "ABCD".getBytes(), -1);
		System.out.println(zk);
		System.out.println("Test done");
		System.exit(0);
	}	
}
