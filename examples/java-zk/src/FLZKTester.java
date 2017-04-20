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
		if(args.length<3)
		{
			System.out.println("Usage: java FLZK [hostname] [proxyport] [testtype] [color] [othercolor]");
			System.exit(0);
		}
		ProxyClient appendclient = new ProxyClient(args[0], Integer.parseInt(args[1]));
//		ProxyClient client2 = new ProxyClient(args[0], Integer.parseInt(args[1])+1);
		ProxyClient playbackclient = appendclient;
		int testtype = Integer.parseInt(args[2]);
		int color = Integer.parseInt(args[3]);
		int secondcolor = Integer.parseInt(args[4]);
		
		IZooKeeper zk=null;
		
		if(testtype==0)
		{
			zk = new FLZK(appendclient, playbackclient, color, null);
		}
		else if(testtype==1)
		{
			int othercolors[] = new int[1]; othercolors[0] = secondcolor;
			zk = new FLZKCausal(appendclient, playbackclient, color, null, othercolors);
		}
		else if(testtype==2)
		{
			zk = new FLZKCAP(appendclient, playbackclient, color, null, secondcolor);
		}
		
			
		System.out.println("hello world!");

		if(zk.exists("/abcd",  true)==null)
			zk.create("/abcd", "AAA".getBytes(), null, CreateMode.PERSISTENT);
		zk.setData("/abcd", "ABCD".getBytes(), -1);
//		System.out.println(zk);
		Thread.sleep(1000);
		
		if(testtype==2)
		{
		
			Thread.sleep(1000);
			for(int i=0;i<100;i++)
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
			System.out.println("Network partitions...");
			((FLZKCAP)zk).disconnect();
			Thread.sleep(1000);
			for(int i=0;i<200;i++)
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
			System.out.println("Network heals!");
			Thread.sleep(500);
			//how do we know that we're capturing all prior appends to secondary
			((FLZKCAP)zk).reconnect();
			Thread.sleep(500);
			for(int i=0;i<200;i++)
			{
				try
				{
					zk.delete("/abcd/" + i, -1);
				}
				catch(Exception e)
				{
					//fail silently
				}
			}
			
		}
		


		System.out.println("starting synchronous test!");

		long stoptime = 0;
		long starttime = 0;

		//synchronous testing
		int numbatches = 1;
		int batchsize = 100;
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
					//fail silently? catch nodeexists keeperexception
				}
			}
			stoptime = System.currentTimeMillis();
			System.out.println("Batch [" + j + "/" + numbatches + "] Create done in " + (stoptime-starttime) + ": " + ((double)(stoptime-starttime)/(double)batchsize) + " ms per op.");
		}
	//		System.out.println(zk);
			
		starttime = System.currentTimeMillis();			
		for(int i=0;i<100;i++)
			zk.setData("/abcd/" + i, "BBB".getBytes(), -1);
		stoptime = System.currentTimeMillis();			
		System.out.println("SetData done in " + (stoptime-starttime));			
//		System.out.println(zk);
		
		starttime = System.currentTimeMillis();
		for(int i=0;i<100;i++)
			zk.exists("/abcd/" + i, false);
		stoptime = System.currentTimeMillis();		
		System.out.println("Exists done in " + (stoptime-starttime));			
//		System.out.println(zk);
		
		starttime = System.currentTimeMillis();
		for(int i=0;i<100;i++)
			zk.delete("/abcd/" + i, -1);
		stoptime = System.currentTimeMillis();
//		System.out.println("Delete done in " + (stoptime-starttime));						

			
//		zk.setData("/abcd", "ABCD".getBytes(), -1);
		System.out.println(zk);
		System.out.println("Synchronous Test done");
		
		
		//asynchronous testing
		TesterCB dcb = new TesterCB();
		int numtests = 10000;
		long testtime = 0;
		dcb.start(numtests);
		for(int i=0;i<numtests;i++)
		{
			zk.create("/abcd/" + i, "AAA".getBytes(), null, CreateMode.PERSISTENT, dcb, null);
		}
		testtime = dcb.waitforcompletion();
		System.out.println("Throughput (create): " + (1000.0*(double)numtests/(double)testtime));
		
		dcb.start(numtests);
		for(int i=0;i<numtests;i++)
		{
			zk.setData("/abcd/" + i, "BBB".getBytes(), -1, dcb, null);
		}
		testtime = dcb.waitforcompletion();
		System.out.println("Throughput (setdata): " + (1000.0*(double)numtests/(double)testtime));		

		dcb.start(numtests);
		for(int i=0;i<numtests;i++)
		{
			zk.exists("/abcd/" + i, false, dcb, null);
		}
		testtime = dcb.waitforcompletion();
		System.out.println("Throughput (exists): " + (1000.0*(double)numtests/(double)testtime));		

		dcb.start(numtests);
		for(int i=0;i<numtests;i++)
		{
			zk.getData("/abcd/" + i, false, dcb, null);
		}
		testtime = dcb.waitforcompletion();
		System.out.println("Throughput (getdata): " + (1000.0*(double)numtests/(double)testtime));		
		
		dcb.start(numtests);		
		for(int i=0;i<numtests;i++)
		{
			zk.getChildren("/abcd/" + i, false, dcb, null);
		}
		testtime = dcb.waitforcompletion();
		System.out.println("Throughput (getchildren): " + (1000.0*(double)numtests/(double)testtime));		

		dcb.start(numtests);
		for(int i=0;i<numtests;i++)
		{
			zk.delete("/abcd/" + i, -1, dcb, null);
		}
		testtime = dcb.waitforcompletion();
		System.out.println("Throughput (delete): " + (1000.0*(double)numtests/(double)testtime));		
		
		
		System.exit(0);
	}	
}

class TesterCB implements AsyncCallback.StringCallback, AsyncCallback.VoidCallback, AsyncCallback.StatCallback, AsyncCallback.DataCallback, AsyncCallback.ChildrenCallback
{
	int numtowaitfor;
	AtomicInteger numdone;
	long starttime;
	long stoptime;
	boolean debugprints = false;
	
	public TesterCB()
	{
		numdone = new AtomicInteger();
	}
	
	public void start(int t)
	{
		starttime = System.currentTimeMillis();	
		numtowaitfor = t;
	}
	
	public synchronized boolean done()
	{
		return numdone.get()>=numtowaitfor;
	}
	
	public long waitforcompletion()
	{
		synchronized(this)
		{
			while(!this.done())
			{
				try
				{
					this.wait();
				}
				catch(InterruptedException e)
				{
					//continue;
				}
			}
			stoptime = System.currentTimeMillis();
			System.out.println("Done in " + (stoptime-starttime) + " milliseconds.");
		}
		return stoptime-starttime;
	}
	
	
	@Override
	public void processResult(int rc, String path, Object ctx, String name) 
	{
		if(debugprints) System.out.println(Code.get(rc) + " create " + path + " " + numdone);
		bump();
	}


	@Override
	public void processResult(int rc, String path, Object ctx)
	{	
		if(debugprints) System.out.println(Code.get(rc) + " delete: " + path + " " + numdone);
		bump();
	}


	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat) 
	{
		if(debugprints) System.out.println(Code.get(rc) + " setdata/exists: " + path + " " + numdone);
		bump();		
	}


	@Override
	public void processResult(int rc, String path, Object ctx, byte[] data,
			Stat stat) 
	{
		if(debugprints) System.out.println(Code.get(rc) + " getdata: " + path + " " + numdone);
		bump();
	}


	@Override
	public void processResult(int rc, String path, Object ctx, List<String> children) 
	{
		if(debugprints) System.out.println(Code.get(rc) + " getchildren: " + path + " " + numdone);
		bump();
		
	}
	
	void bump()
	{
		if(numdone.incrementAndGet()>=numtowaitfor)
		{
			synchronized(this)
			{
				this.notify();
			}
		}	
	}
	
	
}
