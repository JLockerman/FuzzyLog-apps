
import java.util.List;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import fuzzy_log.ProxyHandle;

public class FLZKTester
{
	public static void main(String[] args) throws Exception
	{
		// System.in.read();
		if(args.length<4)
		{
			System.out.println("Usage: java FLZK [server addr] [testtype] [client num] [num clients]");
			System.exit(0);
		}
		int testtype = Integer.parseInt(args[1]);
		int client_num = Integer.parseInt(args[2]);
		long num_clients = Integer.parseInt(args[3]);
		int color = client_num+1;
		//FuzzyLog appendclient = new FuzzyLog(new String[]{args[0]}, new int[]{color});
//		ProxyClient client2 = new ProxyClient(args[0], Integer.parseInt(args[1])+1);
		//FuzzyLog playbackclient = appendclient;

		System.out.println("make handle: server addr " + args[0] +  ", color " + color + ", wait for " + num_clients);
		final ProxyHandle client;
		if(testtype == 0) client = new ProxyHandle(args[0], 13337, color);
		else if(testtype == 1) client = new ProxyHandle(args[0], 13337, num_clients, color);
		else throw new RuntimeException("Unknown test type " + testtype);

		// System.out.println("start testing");

		final FLZK zk = new FLZK(client, color, null);

		if(testtype==0) {}
		else if(testtype==1)
		{
			// System.out.println("create test");
			runCreateTest(zk, client_num);
			System.exit(0);
		}
		else {
			throw new RuntimeException();
		}


		System.out.println("hello world!");

		if(zk.exists("/abcd",  true)==null)
			zk.create("/abcd", "AAA".getBytes(), null, CreateMode.PERSISTENT);
		System.out.println("created abcd!");
		/*zk.setData("/abcd", "ABCD".getBytes(), -1);
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
			// ((FLZKCAP)zk).disconnect();
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
			// ((FLZKCAP)zk).reconnect();
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

		}*/



		/* System.out.println("starting synchronous test!");

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
*/
		int numtests = 10_000;
		long testtime = 0;
		System.out.println("local tests.");
		{
			((FLZK) zk).localCreate("/abcQ", "AAA".getBytes(), null, CreateMode.PERSISTENT);
			long start = System.nanoTime();
			for(int i = 0; i < numtests; i++) {
				((FLZK) zk).localCreate("/abcQ/" + i, "AAA".getBytes(), null, CreateMode.PERSISTENT);
			}
			testtime = (System.nanoTime() - start)/1_000_000;
			System.out.println("Done in " + testtime + " milliseconds.");
			System.out.println("Throughput (local create): " + (1000.0*(double)numtests/(double)testtime));
		}

		System.out.println("start async tests.");
		//asynchronous testing
		TesterCB dcb = new TesterCB();

		// int numtests = 10;

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

	private final static void runCreateTest(final FLZK zk, int clientNum) {
		// System.out.println("create scaling test start");
		final String dirname = "/foo" + clientNum;
		final String dir = dirname + "/";
		try { zk.create(dir, "AAA".getBytes(), null, CreateMode.PERSISTENT); }
		catch(KeeperException | InterruptedException e) { throw new RuntimeException(e); }

		AtomicBoolean done = new AtomicBoolean(false);

		CountingCB cb = new CountingCB();

		Thread createThread = new Thread(() -> {
			int fileNum = 0;
			while(!done.get()) {
				for(int i = 0; i < 20; i++) {
					zk.create(dir + i, "AAA".getBytes(), null, CreateMode.PERSISTENT, cb, null);
					fileNum++;
				}
			}
		});
		createThread.start();
		final int numRounds = 10;
		final int[] complete = new int[numRounds];
		for(int round = 0; round < numRounds; round++) {
			try { Thread.sleep(1000); }
			catch(InterruptedException e) { throw new RuntimeException(e); }
			complete[round] = cb.take();
		}
		try { Thread.sleep(1000); }
		catch(InterruptedException e) { }
		done.set(true);
		try { Thread.sleep(1000); }
		catch(InterruptedException e) { }

		long total = 0;
		// System.out.println(Arrays.toString(complete));
		for(int i = 0; i < complete.length; i++) total += complete[i];
		long avg = total / complete.length;
		System.out.printf("%d: %6d Hz\n", clientNum, avg);
		//System.out.println(clientNum + ": " + avg + " Hz");
	}
}

class CountingCB implements AsyncCallback.StringCallback, AsyncCallback.VoidCallback, AsyncCallback.StatCallback, AsyncCallback.DataCallback, AsyncCallback.ChildrenCallback {
	private final AtomicInteger numDone;

	public CountingCB()
	{
		numDone = new AtomicInteger();
	}

	public final int take() {
		return numDone.getAndSet(0);
	}

	@Override
	public final void processResult(int rc, String path, Object ctx, String name)
	{
		bump();
	}


	@Override
	public final void processResult(int rc, String path, Object ctx)
	{
		bump();
	}


	@Override
	public final void processResult(int rc, String path, Object ctx, Stat stat)
	{
		bump();
	}


	@Override
	public final void processResult(int rc, String path, Object ctx, byte[] data,
			Stat stat)
	{
		bump();
	}


	@Override
	public final void processResult(int rc, String path, Object ctx, List<String> children)
	{
		bump();

	}

	private final void bump()
	{
		int done = numDone.incrementAndGet();
	}
}

class TesterCB implements AsyncCallback.StringCallback, AsyncCallback.VoidCallback, AsyncCallback.StatCallback, AsyncCallback.DataCallback, AsyncCallback.ChildrenCallback
{
	private int numtowaitfor;
	private AtomicInteger numdone;
	private long starttime;
	private long stoptime;
	private final boolean debugprints = false;

	public TesterCB()
	{
		numdone = new AtomicInteger();
	}

	public void start(int t)
	{
		starttime = System.nanoTime();
		numtowaitfor = t;
		numdone = new AtomicInteger();
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
			stoptime = System.nanoTime();
			System.out.println("Done in " + (stoptime-starttime)/1_000_000 + " milliseconds.");
		}
		return (stoptime-starttime)/1_000_000;
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
