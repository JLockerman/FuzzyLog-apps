
import java.util.List;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
		FLZKOp.client = client_num;
		//FuzzyLog appendclient = new FuzzyLog(new String[]{args[0]}, new int[]{color});
//		ProxyClient client2 = new ProxyClient(args[0], Integer.parseInt(args[1])+1);
		//FuzzyLog playbackclient = appendclient;

		String serverAddrs = args[0];
		System.out.println("make handle: server addrs " + serverAddrs +  ", color " + color + ", wait for " + num_clients);
		final HashMap<String,Integer> partitions = new HashMap<>();
		final String myRoot;
		final ProxyHandle<FLZKOp> client;
		String renameRoot = "";
		// if(testtype == 0) {
		// 	partitions.put("/abcd", color);
		// 	partitions.put("/efgh", color + 1);
		// 	myRoot = "/abcd";
		// 	client = new ProxyHandle(serverAddrs, 13337, color);
		// }
		// else if(testtype == 1) {
		// 	for(int i = 0; i < num_clients; i++) {
		// 		partitions.put("/foo" + i, i + 1);
		// 	}
		// 	myRoot = "/foo" + client_num;
		// 	client = new ProxyHandle(serverAddrs, 13337, num_clients, color);
		// }
		// else throw new RuntimeException("Unknown test type " + testtype);
		switch (testtype) {
			case 0:
			case 2: {
				partitions.put("/abcd", color);
				partitions.put("/efgh", color + 1);
				myRoot = "/abcd";
				client = new ProxyHandle(serverAddrs, 13337, color);
				break;
			}
			case 1: {
				for(int i = 0; i < num_clients; i++) {
					partitions.put("/foo" + i, i + 1);
				}
				myRoot = "/foo" + client_num;
				renameRoot = "/foo" + (client_num + 1) % num_clients;
				client = new ProxyHandle(serverAddrs, 13337, num_clients, color);
				break;
			}
			default:
				throw new RuntimeException("Unknown test type " + testtype);
		}

		// System.out.println("start testing");

		final FLZK zk = new FLZK(client, color, myRoot, partitions, null);

		switch (testtype) {
			case 0: break;
			case 1:
				runCreateTest(zk, myRoot, renameRoot, client_num, (int)num_clients);
				System.exit(0);
			case 2:
				runRegressionTest(zk, myRoot, client_num);
				System.exit(0);
			default:
				throw new RuntimeException("Unknown test type " + testtype);
		}


		System.out.println("hello world!");

		if(zk.exists("/abcd",  true)==null) {
			System.out.println("needs create");
			zk.create("/abcd", "AAA".getBytes(), null, CreateMode.PERSISTENT);
		}

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
		/*System.out.println("local tests.");
		{
			((FLZK) zk).localCreate("/abcQ", "AAA".getBytes(), null, CreateMode.PERSISTENT);
			long start = System.nanoTime();
			for(int i = 0; i < numtests; i++) {
				((FLZK) zk).localCreate("/abcQ/" + i, "AAA".getBytes(), null, CreateMode.PERSISTENT);
			}
			testtime = (System.nanoTime() - start)/1_000_000;
			System.out.println("Done in " + testtime + " milliseconds.");
			System.out.println("Throughput (local create): " + (1000.0*(double)numtests/(double)testtime));
		}*/

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

		{
			long starttime = System.nanoTime();
			for(int i=0;i<numtests;i++)
			{
				zk.rename("/abcd/" + i, "/abcd/r" + i, null);
			}
			while(zk.exists("/abcd/r" + (numtests - 1), false) == null);
			long stoptime = System.nanoTime();
			testtime = (stoptime-starttime)/1_000_000;
			System.out.println("Throughput (rename): " + (1000.0*(double)numtests/(double)testtime));
		}

		dcb.start(numtests);
		for(int i=0;i<numtests;i++)
		{
			zk.delete("/abcd/r" + i, -1, dcb, null);
		}
		testtime = dcb.waitforcompletion();
		System.out.println("Throughput (delete): " + (1000.0*(double)numtests/(double)testtime));


		System.exit(0);
	}

	private final static void runCreateTest(final FLZK zk, final String dirname, final String renameDirname, int clientNum, int numClients) {
		System.out.println("create scaling test start");
		final String dir = dirname + "/";
		// final String renameDir = renameDirname + "/";
		// try { zk.create(dir, "AAA".getBytes(), null, CreateMode.PERSISTENT); }
		// catch(KeeperException | InterruptedException e) {
		// 	System.out.println("could not create " + dirname);
		// 	throw new RuntimeException(e);
		// }

		AtomicBoolean done = new AtomicBoolean(false);
		AtomicBoolean stopped_send = new AtomicBoolean(false);
		AtomicInteger numSent = new AtomicInteger(0);

		CountingCB2 cb = new CountingCB2();

		Thread createThread = new Thread(() -> {
			Random rand = new Random(clientNum + 1111);
			int fileNum = 0;
			int renameNum = 0;
			int window = 0;
			while(!done.get()) {
				//most efficent @ 1000: 12kHz, 2000: 29kHz, 5000: 30kHz!?
				//freezes @ 500, 750, 1000!?
				//slow, no freeze @ 100
				while(window - cb.localCount() > 5000) { Thread.yield(); }
				// for(int i = 0; i < 500; i++) {
					if(rand.nextInt(100) < 1 && renameNum < cb.localCount()) {
						int renameDir = rand.nextInt(numClients);
						zk.rename(dir + renameNum, "/foo" + renameDir + "/r" + clientNum + "_"  + renameNum, cb);
						renameNum++;
					} else {
						zk.create(dir + fileNum, "AAA".getBytes(), null, CreateMode.PERSISTENT, cb, null);
						fileNum++;
					}
					window++;
					numSent.getAndIncrement();
				// }
			}
			// numSent.set(fileNum + renameNum);
			stopped_send.set(true);
		});
		createThread.start();
		final int numRounds = 10;
		int total_completed = 0;
		int total_sent = 0;
		final int[] complete = new int[numRounds];
		final int[] sent = new int[numRounds];
		final int[] outstanding = new int[numRounds];
		for(int round = 0; round < numRounds; round++) {
			try { Thread.sleep(3000); }
			catch(InterruptedException e) { throw new RuntimeException(e); }
			complete[round] = cb.take();
			sent[round] = numSent.getAndSet(0);
			total_completed += complete[round];
			total_sent += sent[round];
			outstanding[round] = total_sent - total_completed;
		}
		try { Thread.sleep(3000); }
		catch(InterruptedException e) { }
		done.set(true);
		try { Thread.sleep(3000); }
		catch(InterruptedException e) { }

		long total = 0;
		// System.out.println(Arrays.toString(complete));
		for(int i = 0; i < complete.length; i++) total += (complete[i] / 3);
		long avg = total / complete.length;
		System.out.printf("> %d: %6d Hz\n", clientNum, avg);
		System.out.println(
			"" + clientNum + ": " + Arrays.toString(complete) + "\n" +
			clientNum + ": " + Arrays.toString(sent) + "\n" +
			clientNum + ": " + Arrays.toString(outstanding)
		);
		while(!stopped_send.get()) { Thread.yield(); }
		try { Thread.sleep(30); }
		catch(InterruptedException e) { }
		//System.out.println(clientNum + ": " + avg + " Hz");
		// while(!stopped_send.get()) { Thread.yield(); }
		// int fileNum = numSent.get();
		// while(total_completed < fileNum) {
		// 	total_completed += cb.take();
		// 	Thread.yield();
		// }
	}

	private final static void do_assert(boolean condition) {
		if(!condition) {
			throw new RuntimeException();
		}
	}

	private final static void do_assert(boolean condition, long iter) {
		if(!condition) {
			throw new RuntimeException("@ " + iter);
		}
	}

	private final static void runRegressionTest(final FLZK zk, final String dirname, int clientNum) {
		System.out.println("Start regression test.");
		try {
			for(int i = 0; i < 100; i++) {
				String path = zk.create("/abcd/" + i, ("AAA" + i).getBytes(), null, CreateMode.PERSISTENT);
				do_assert(path.equals("/abcd/" + i));
				do_assert(zk.exists("/abcd/" + i, false) != null);
				byte[] data = zk.getData("/abcd/" + i, false, null);
				assert(Arrays.equals(data, ("AAA" + i).getBytes()));
				if(i % 2 == 0) {
					zk.setData("/abcd/" + i, ("BBB" + i).getBytes(), -1);
					byte[] data2 = zk.getData("/abcd/" + i, false, null);
					do_assert(Arrays.equals(data2, ("BBB" + i).getBytes()));
				}
				if(i % 10 == 0) {
					zk.rename("/abcd/" + i, "/abcd/" + i + "_" + i, null);
					//TODO this don't sync
					// Stat s = zk.exists("/abcd/" + i, false);
					// if(s != null) {
					// 	throw new RuntimeException("" + s);
					// }
					// do_assert(zk.exists("/abcd/" + i, false) == null);
				}
				if(i > 11 && i % 11 == 0) zk.rename("/abcd/" + i, "/abcd/" + (i - 11), null);
				if(i > 0 && i % 11 == 0) zk.rename("/abcd/Q" + i, "/abcd/P" + i, null);
			}
			for(int i = 0; i < 100; i++) {
				if(i % 10 != 0)  {
					do_assert(zk.exists("/abcd/" + i, false) != null, i);
					byte[] data = zk.getData("/abcd/" + i, false, null);
					if(i % 2 == 0) {
						do_assert(Arrays.equals(data, ("BBB" + i).getBytes()), i);
					} else {
						if(!Arrays.equals(data, ("AAA" + i).getBytes())) {
							String is = new String(data);
							throw new RuntimeException(
								"\n     got " + is + "\n" +
								"expected " + ("AAA" + i)
							);
						}
						do_assert(Arrays.equals(data, ("AAA" + i).getBytes()), i);
					}
				} else {
					do_assert(zk.exists("/abcd/" + i, false) == null, i);
					do_assert(zk.exists("/abcd/" + i + "_" + i, false) != null, i);
					byte[] data = zk.getData("/abcd/" + i + "_" + i, false, null);
					if(i % 2 == 0) {
						do_assert(Arrays.equals(data, ("BBB" + i).getBytes()), i);
					} else {
						do_assert(Arrays.equals(data, ("AAA" + i).getBytes()), i);
					}
				}
				if(i > 1 && i % 11 == 0) {
					do_assert(zk.exists("/abcd/Q" + i, false) == null, i);
					do_assert(zk.exists("/abcd/P" + i, false) == null, i);
				}
			}
			System.out.println("test done.");
		} catch(KeeperException | InterruptedException e) {
			throw new RuntimeException(e);
		}
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


class CountingCB2 implements AsyncCallback.StringCallback, AsyncCallback.VoidCallback, AsyncCallback.StatCallback, AsyncCallback.DataCallback, AsyncCallback.ChildrenCallback {
	private final AtomicInteger numDone;
	private int localNumDone;

	public CountingCB2()
	{
		numDone = new AtomicInteger();
		localNumDone = 0;
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
		numDone.incrementAndGet();
		localNumDone += 1;
	}

	public final int localCount() {
		return this.localNumDone;
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
		numdone = new AtomicInteger();
		numtowaitfor = t;
		starttime = System.nanoTime();
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
