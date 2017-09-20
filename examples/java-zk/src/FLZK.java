import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;
import org.apache.zookeeper.AsyncCallback;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import fuzzy_log.*;
import fuzzy_log.FuzzyLog.ReadHandleAndWriteHandle;
import fuzzy_log.ReadHandle.*;
import c_link.write_id;

final class Pair<A,B>
{
	public final A first;
	public final B second;
	Pair(A first, B second)
	{
		this.first = first;
		this.second = second;
	}
}

final class SetOp extends FLZKOp implements Serializable
{
	String path;
	byte[] data;
	int version;
	AsyncCallback.StatCallback cb;
	Object ctx;
	public SetOp(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx)
	{
		this.path = path;
		this.data = data;
		this.version = version;
		this.cb = cb;
		this.ctx = ctx;
	}

	public void callback(KeeperException ke, Object O)
	{
		if(ke!=null)
			cb.processResult(ke.code().intValue(), path, ctx, (Stat)O);
		else
			cb.processResult(Code.OK.intValue(), path, ctx, (Stat)O);

	}
	private void writeObject(java.io.ObjectOutputStream out) throws IOException
	{
		out.writeObject(data);
		out.writeObject(new Integer(version));
		out.writeObject(path);
	}
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
	{
		data = (byte[])in.readObject();
		version = ((Integer)in.readObject()).intValue();
		path = (String)in.readObject();
	}

	public boolean hasCallback() {
		return cb != null;
	}

	/*public final ByteBuffer writeBytes(ByteBuffer out) {
		if(out.remaining() < size()) {
			ByteBuffer newOut = ByteBuffer.allocateDirect(out.limit() + size());
			out.flip();
			newOut.put(out);
			out = newOut;

		}
		out.put((byte)1);
		super.writeBytes(out)
			.put(path.getBytes(Charset.UTF_8))
			.put(data)
			.putInt(version);
	}

	public final SetOp readBytes(ByteBuffer in) {
		int id = super.readId(in);
		byte[] path_b = in.getByteArray();
		String path = new String(path_b, Charset.UTF_8);
		byte[] data = in.getByteArray();
		int version = in.getInt();
		return new SetOp(id, path, data, version, null, null);
	}

	private final int size() {
		return 4 + 1 + 4 + this.path.length() * 4 + 4 + data.length + 4;
	}*/
}


final class CreateOp extends FLZKOp implements Serializable
{
	CreateMode cm;
	byte[] data;
	List<ACL> acl;
	String path;
	AsyncCallback.StringCallback cb;
	Object ctxt;
	public CreateOp(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctxt)
	{
		this.path = path;
		this.data = data;
		this.acl = acl;
		this.cm = createMode;
		this.cb = cb;
		this.ctxt = ctxt;
	}
	public void callback(KeeperException ke, Object O)
	{
		if(ke!=null)
			cb.processResult(ke.code().intValue(), path, ctxt, (String)O);
		else
			cb.processResult(Code.OK.intValue(), path, ctxt, (String)O);
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException
	{
		out.writeObject(cm);
		out.writeObject(data);
		out.writeObject(acl);
		out.writeObject(path);
	}
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
	{
		cm = (CreateMode)in.readObject();
		data = (byte[])in.readObject();
		acl = (List<ACL>)in.readObject();
		path = (String)in.readObject();
	}

	public boolean hasCallback() {
		return cb != null;
	}
}

final class GetChildrenOp extends FLZKOp
{
	String path; boolean watch; AsyncCallback.ChildrenCallback cb; Object ctx;
	public GetChildrenOp(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx)
	{
		this.path = path;
		this.watch = watch;
		this.cb = cb;
		this.ctx = ctx;
	}

	@Override public void callback(KeeperException ke, Object O)
	{
		if(ke!=null)
			cb.processResult(ke.code().intValue(), path, ctx, (List<String>)O); //what about stat?
		else
			cb.processResult(Code.OK.intValue(), path, ctx, (List<String>)O); //what about stat?
	}

	public boolean hasCallback() {
		return cb != null;
	}
}

final class GetOp extends FLZKOp
{
	String path;
	boolean watch;
	AsyncCallback.DataCallback cb;
	Object ctx;

	public GetOp(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx)
	{
		this.path = path;
		this.watch = watch;
		this.cb = cb;
		this.ctx = ctx;
	}


	@Override
	public void callback(KeeperException ke, Object O)
	{
		Pair<byte[], Stat> P = (Pair<byte[], Stat>)O;
		if(ke!=null)
		{
			//P is going to be null if an exception was thrown
			cb.processResult(ke.code().intValue(), path, ctx, null, null); //what about stat?
		}
		else
		{
			cb.processResult(Code.OK.intValue(), path, ctx, P.first, P.second);
		}
	}

	public boolean hasCallback() {
		return cb != null;
	}
}


final class ExistsOp extends FLZKOp
{
	String path;
	boolean watch;
	AsyncCallback.StatCallback cb;
	Object ctx;

	public ExistsOp(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx)
	{
		this.path = path;
		this.watch = watch;
		this.cb = cb;
		this.ctx = ctx;
	}


	@Override
	public void callback(KeeperException ke, Object O)
	{
		if(ke!=null)
			cb.processResult(ke.code().intValue(), path, ctx, (Stat)O);
		else
			cb.processResult(Code.OK.intValue(), path, ctx, (Stat)O);
	}

	public boolean hasCallback() {
		return cb != null;
	}
}

final class DeleteOp extends FLZKOp implements Serializable
{
	String path;
	int version;
	AsyncCallback.VoidCallback cb;
	Object ctxt;
	public DeleteOp(String path, int version, AsyncCallback.VoidCallback tcb, Object tctxt)
	{
		this.path = path;
		this.version = version;
		this.cb = tcb;
		this.ctxt = tctxt;
	}
	public void callback(KeeperException ke, Object O)
	{
		if(ke!=null)
			cb.processResult(ke.code().intValue(), path, ctxt);
		else
			cb.processResult(Code.OK.intValue(), path, ctxt);

	}
	private void writeObject(java.io.ObjectOutputStream out) throws IOException
	{
		out.writeObject(path);
		out.writeObject(new Integer(version));
	}
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
	{
		path = (String)in.readObject();
		version = ((Integer)in.readObject()).intValue();
	}

	public boolean hasCallback() {
		return cb != null;
	}
}

final class MarkerOp extends FLZKOp
{

	public static final MarkerOp Instance = new MarkerOp();

	public MarkerOp() {}

	public void callback(KeeperException ke, Object O) {}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {}

	public boolean hasCallback() {
		return false;
	}
}

final class WakeOp extends FLZKOp
{

	public static final WakeOp Instance = new WakeOp();

	public WakeOp() {}

	public void callback(KeeperException ke, Object O) {}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {}

	public boolean hasCallback() {
		return false;
	}
}


final class Sequenced<T> {
	public final long sequenceNumber;
	public final T t;

	public Sequenced(long sequenceNumber, T t) {
		this.sequenceNumber = sequenceNumber;
		this.t = t;
	}

	@Override
	public final String toString() {
		return "(" + sequenceNumber + ", " + t + ")";
	}
}

public final class FLZK implements IZooKeeper, Runnable
{
	protected final ProxyHandle client;
	private final HashMap<File, Node> map;

	private final ConcurrentHashMap<Object, FLZKOp> pendinglist;  //mutating operations, must sunchronize on appendclient
	private final LinkedBlockingQueue<FLZKOp> passivelist; //non-mutating operations

	private final LinkedBlockingQueue<Object> callbacklist;
	private final Watcher defaultwatcher;
	private final Map<String, Set<Watcher>> existswatches;

	private final ArrayList<LinkedBlockingQueue<FLZKOp>> toSer;
	private final LinkedBlockingQueue<byte[]> fromSer;
	private int nextSer = 0;

	private final ArrayList<LinkedBlockingQueue<Sequenced<byte[]>>> toDe;
	private final LinkedBlockingQueue<Sequenced<FLZKOp>> fromDe;
	private int nextDe = 0;

	private final int serDeThreads = 4;

	private byte[] writeBuffer;

	private long numentries=0;


	private int mycolor = 0;

	private final boolean debugprints = false;

	public FLZK(ProxyHandle client, int tmycolor, Watcher W) throws Exception, KeeperException
	{
		this.client = client;

		defaultwatcher = W;
		pendinglist = new ConcurrentHashMap<Object, FLZKOp>();
		passivelist = new LinkedBlockingQueue<FLZKOp>();
		callbacklist = new LinkedBlockingQueue<Object>();
		map = new HashMap<File, Node>();
		map.put(new File("/"), new Node("foobar".getBytes(), "/"));
		existswatches = new HashMap<String, Set<Watcher>>();
		mycolor = tmycolor;

		writeBuffer = new byte[512];

		//IO Thread
		(new Thread(this)).start();

		//Callback Thread
		new Thread(new Runnable(){
			public void run()
			{
				Object O = null;
				while(true)
				{
					O = null;
					try
					{
						O = callbacklist.take();
					}
					catch (InterruptedException e)
					{
						//do nothing, keep going
					}
					if(O!=null)
					{
						Pair P = (Pair)O;
						//todo -- are the instanceofs inefficient?
						if(P.first instanceof Pair)
						{
							Pair<Pair<FLZKOp, KeeperException>, Object> P2 = (Pair<Pair<FLZKOp, KeeperException>, Object>)P;
							P2.first.first.callback(P2.first.second, P2.second);
						}
						else if(P.first instanceof Watcher)
						{
							Pair<Watcher, WatchedEvent> P2 = (Pair<Watcher, WatchedEvent>)P;
							P2.first.process(P2.second);
						}
						else
							throw new RuntimeException("callback is of unidentified type!");
					}
				}

			}
		}).start();

		fromSer = new LinkedBlockingQueue<>();
		fromDe = new LinkedBlockingQueue<>();

		new Thread(() -> {
			while(true) {
				try {
					byte[] op = fromSer.take();
					client.append(mycolor, op);
					passivelist.add(WakeOp.Instance);
				} catch(InterruptedException e) { }
			}
		}).start();

		new Thread(() -> {
			long nextSequnceNumber = 0;
			// PriorityQueue<Sequenced<FLZKOp>> pendingEvents = new PriorityQueue<>((a, b) -> {
			// 	if(a.sequenceNumber < b.sequenceNumber) return -1;
			// 	else if(b.sequenceNumber < a.sequenceNumber) return 1;
			// 	else return 0;
			// });
			HashMap<Long, FLZKOp> pendingEvents = new HashMap<>();
			while(true) {
				try {
					Sequenced<FLZKOp> szop = fromDe.take();
					// System.out.println("n " + nextSequnceNumber + " c " + szop.sequenceNumber);
					// System.out.println(pendingEvents);
					if(szop.sequenceNumber == nextSequnceNumber) {
						nextSequnceNumber += 1;
						FLZKOp zop = szop.t;
						Object ret = null;
						try
						{
							if(debugprints) System.out.println("applying op" + zop);
							ret = this.apply(zop);
							if(zop.hasCallback()) schedulecallback(zop, null, ret);
								//mycop.callback(null, ret);
						}
						catch(KeeperException ke)
						{
							if(zop.hasCallback()) schedulecallback(zop, ke, ret);
								//mycop.callback(ke, ret);
						}
						//while(!pendingEvents.isEmpty() && pendingEvents.peek().sequenceNumber == nextSequnceNumber) {
						while(pendingEvents.containsKey(nextSequnceNumber)) {
							//zop = pendingEvents.remove().t;
							zop = pendingEvents.remove(nextSequnceNumber);
							nextSequnceNumber += 1;
							try
							{
								if(debugprints) System.out.println("applying op" + zop);
								ret = this.apply(zop);
								if(zop.hasCallback()) schedulecallback(zop, null, ret);
									//mycop.callback(null, ret);
							}
							catch(KeeperException ke)
							{
								if(zop.hasCallback()) schedulecallback(zop, ke, ret);
									//mycop.callback(ke, ret);
							}
						}
					} else {
						// pendingEvents.add(szop);
						pendingEvents.put(szop.sequenceNumber, szop.t);
						// System.out.println(pendingEvents.size());
					}


				} catch(InterruptedException e) { }
			}
		}).start();

		toSer = new ArrayList<>(serDeThreads);
		toDe = new ArrayList<>(serDeThreads);
		for(int i = 0; i < serDeThreads; i++) {
			final int threadNum = i;
			toSer.add(new LinkedBlockingQueue<>());
			new Thread(() -> {
				while(true) {
					try {
						FLZKOp flop = toSer.get(threadNum).take();
						byte[] out = ObjectToBytes(flop);
						pendinglist.put(flop.id, flop);
						fromSer.offer(out);
					} catch(InterruptedException e) { }
				}
			}).start();

			toDe.add(new LinkedBlockingQueue<>());
			new Thread(() -> {
				while(true) {
					try {
						Sequenced<byte[]> sbytes = toDe.get(threadNum).take();
						byte[] bytes = sbytes.t;
						long sequenceNumber = sbytes.sequenceNumber;
						ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
						FLZKOp zop =(FLZKOp)ois.readObject();
						//is this a pending operation?
						FLZKOp mycop = pendinglist.remove(zop.id);

						if(debugprints)
						{
							if(mycop!=null) System.out.println("Found pending op!" + zop.id);
							else System.out.println("Didn't find pending op!" + zop.id);
						}
						if(mycop != null) {
							fromDe.offer(new Sequenced<>(sequenceNumber, mycop));
						} else {
							fromDe.offer(new Sequenced<>(sequenceNumber, zop));
						}
					} catch(InterruptedException e) {
					} catch(ClassNotFoundException | IOException e) {
						throw new RuntimeException(e);
					}
				}
			}).start();
		}
	}

	public synchronized String toString()
	{
		String x = map.toString();
		return x;
	}


	void processAsync(FLZKOp flop, boolean mutate)
	{
		//signal to learning thread
		// if(debugprints) System.out.println("process");
		if(mutate)
		{
			toSer.get(nextSer % serDeThreads).offer(flop);
			nextSer += 1;
			// client.append(mycolor, ObjectToBytes(flop));
			// CacheArrayOutputStream out = ObjectToBytes(flop, this.writeBuffer);
			// pendinglist.put(flop.id, flop);
			// client.append(mycolor, out.buffer(), out.length());
			// passivelist.add(WakeOp.Instance);
			// this.writeBuffer = out.buffer();
			//we don't have to wait for the append to finish;
			//we just wait for it to appear in the learner thread
		}
		else
		{
			passivelist.add(flop);
		}

	}

	@Override
	public void run()
	{
		while(true)
		{
			try
			{
				this.sync();
			}
			catch(InterruptedException e)
			{
				//do nothing, just try again
			}
			catch(IOException e)
			{
				throw new RuntimeException(e);
			}
		}

	}

	private void sync() throws IOException, InterruptedException
	{
		long seq = 0;
		while(true) {
			FLZKOp pop;
			if (pendinglist.isEmpty()) {
				pop = passivelist.take();
			} else {
				pop = passivelist.poll();
			}
			if(pendinglist.isEmpty() && pop == MarkerOp.Instance) {
				pop = passivelist.take();
			}

			// if(debugprints) System.out.println("sync");
			// int objectsFound = 0;
			// new passive events must wait for the next snapshot
			passivelist.offer(MarkerOp.Instance);
			// snapshot mycolor
			ProxyHandle.Bytes events = client.snapshot_and_get();
			for(byte[] event: events) {
				// objectsFound += 1;
				numentries++;
				if(debugprints) System.out.println("Tail not reached");
				toDe.get(nextDe % serDeThreads).offer(new Sequenced<>(seq, event));
				nextDe += 1;
				seq += 1;
				// long fetchTime = System.nanoTime() - startTime;
				// System.out.println("loop time: " + fetchTime + "ns ");
				// startTime = System.nanoTime();
			}
			// long fetchTime = System.nanoTime() - startTime;
			// double hz = 1_000_000_000.0 * (double)objectsFound / (double)fetchTime;
			// /*if(objectsFound != 0)*/ System.out.println("fetch time: " + fetchTime + "ns " + objectsFound + " events " + hz + "hz");

			handle_passive: for(; pop != null && pop != MarkerOp.Instance; pop = passivelist.poll()) {
				if (pop == WakeOp.Instance) { continue handle_passive; }
				if (pop == MarkerOp.Instance) { break handle_passive; }
				//TODO we don't need passive ops to be totally ordered
				fromDe.offer(new Sequenced<>(seq, pop));
				seq += 1;
			}

			// if(debugprints) System.out.println("finished sync: " + passivelist.size() + ", " + pendinglist.size());
		}
	}

	public void schedulecallback(FLZKOp cop, KeeperException ke, Object ret)
	{
		callbacklist.offer(new Pair<Pair<FLZKOp, KeeperException>, Object>(new Pair<FLZKOp, KeeperException>(cop, ke), ret));
	}

	public Object apply(FLZKOp cop) throws KeeperException
	{
		//inefficient -- fix later
		if(cop instanceof CreateOp) return apply((CreateOp)cop);
		else if(cop instanceof DeleteOp) return apply((DeleteOp)cop);
		else if(cop instanceof ExistsOp) return apply((ExistsOp)cop);
		else if(cop instanceof SetOp) return apply((SetOp)cop);
		else if(cop instanceof GetOp) return apply((GetOp)cop);
		else if(cop instanceof GetChildrenOp) return apply((GetChildrenOp)cop);
		else
		{
			System.out.println(cop.getClass());
			throw new RuntimeException("This should never get called");
		}

	}

	public Object apply(CreateOp cop) throws KeeperException
	{
		String path = cop.path;
		File f = new File(path);
		if(map.containsKey(f))
			throw new KeeperException.NodeExistsException();

		if(!map.containsKey(f.getParentFile()))
			throw new KeeperException.NoNodeException();

		Node N = map.get(f.getParentFile());

		if(cop.cm.isEphemeral()) throw new RuntimeException("not yet supported!");
		if(N.isEphemeral())
			throw new KeeperException.NoChildrenForEphemeralsException();


		if(cop.cm.isSequential())
		{
			AtomicInteger I = N.sequentialcounters.get(f);
			if(I==null)
			{
				I = new AtomicInteger(0);
				N.sequentialcounters.put(f,  I);
			}
			int x = I.getAndIncrement();
			String y = String.format("%010d", x);
			path = path + y;
			f = new File(path);
		}



//		N.lock();
		Node newnode = new Node(cop.data, path);
		newnode.stat.setCzxid(numentries); //copy stat either here or on read
		//TODO: is it okay to use numentries here? will this be problematic if different clients see nodes in different orders?
		N.children.add(newnode);
		//existswatches is synchronized on 'this'
		triggerwatches(existswatches.get(newnode.path), new WatchedEvent(Watcher.Event.EventType.NodeCreated, KeeperState.SyncConnected,newnode.path));
		triggerwatches(N.childrenwatches, new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, KeeperState.SyncConnected, N.path));
//		N.unlock();
		map.put(f, newnode); //map is synchronized on 'this'
		return path;
	}

	public Object apply(DeleteOp dop) throws KeeperException
	{
		File F = new File(dop.path);
		if(!map.containsKey(F))
			throw new KeeperException.NoNodeException();
		Node N = map.get(F);
//		N.lock();
		if(dop.version!=-1 && N.stat.getVersion()!=dop.version)
			throw new KeeperException.BadVersionException();
		if(N.children.size()>0)
			throw new KeeperException.NotEmptyException();
		Node parent = map.get(F.getParentFile());
//		parent.lock();
		parent.children.remove(N);
		triggerwatches(parent.childrenwatches, new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, KeeperState.SyncConnected, parent.path));
//		parent.unlock();
		triggerwatches(N.datawatches, new WatchedEvent(Watcher.Event.EventType.NodeDeleted, KeeperState.SyncConnected, N.path));
//		N.unlock();
		map.remove(F);
		return null;
	}



	public Object apply(ExistsOp eop) throws KeeperException
	{
		File F = new File(eop.path);
		if(!map.containsKey(F))
		{
			if(eop.watch && defaultwatcher!=null)
			{
				Set<Watcher> S = existswatches.get(eop.path);
				if(S==null)
				{
					S = new HashSet<Watcher>();
					existswatches.put(eop.path, S);
				}
				S.add(defaultwatcher);
			}
			return null;
		}
		Node N = map.get(F);
//		N.lock();
		Stat x = N.stat;
		if(eop.watch && defaultwatcher!=null) N.datawatches.add(defaultwatcher);
//		N.unlock();
		return x; //todo -- return copy of stat?
	}

	public Object apply(SetOp sop) throws KeeperException
	{
		File f = new File(sop.path);
		if(!map.containsKey(f))
			throw new KeeperException.NoNodeException();
		Node N = map.get(f);
//		N.lock();
		if(sop.version!=-1 && sop.version!=N.stat.getVersion())
			throw new KeeperException.BadVersionException();
		N.data = sop.data;
		N.stat.setMzxid(numentries); //todo: can this cause problems since we don't use curpos?
		Stat x = N.stat;
		//mb: triggers both 'exists' and 'getdata' watches
		triggerwatches(N.datawatches, new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, KeeperState.SyncConnected, N.path));
//		N.unlock();
		return x; //handle stat copying either here or at modification

	}

		public Object apply(GetOp gop) throws KeeperException
	{
		File F = new File(gop.path);
		if(!map.containsKey(F))
			throw new KeeperException.NoNodeException();
		Node N = map.get(F);
//		N.lock();
		Stat x = N.stat;
		byte[] y = N.data;
		if(gop.watch && defaultwatcher!=null) N.datawatches.add(defaultwatcher);
//		N.unlock();
		return new Pair<byte[], Stat>(y, x); //todo -- copy data and stat

	}

	public Object apply(GetChildrenOp gcop) throws KeeperException
	{
		File F = new File(gcop.path);
		if(!map.containsKey(F))
				throw new KeeperException.NoNodeException();
		Node N = map.get(F);
//		N.lock();
		LinkedList<String> children = new LinkedList<String>();
		Iterator<Node> it = N.children.iterator();
		while(it.hasNext())
		{
			children.add(it.next().path);
		}
		if(gcop.watch && defaultwatcher!=null) N.childrenwatches.add(defaultwatcher);
//		N.unlock();
		return children;
	}




	//not thread-safe; lock S before calling
	public void triggerwatches(Set<Watcher> S, WatchedEvent WE)
	{
		if(S!=null && S.size()>0)
		{
			Iterator<Watcher> it = S.iterator();
			while(it.hasNext())
			{
				schedulewatchcallback(it.next(), WE);
				it.remove();
			}
		}
	}

	public void schedulewatchcallback(Watcher W, WatchedEvent WE)
	{
		callbacklist.offer(new Pair<Watcher, WatchedEvent>(W, WE));
	}



	byte[] ObjectToBytes(FLZKOp O)
	{
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(O);
			byte[] b = baos.toByteArray();
			return b;
		}
		catch(IOException ioe)
		{
			throw new RuntimeException(ioe);
		}
	}

	static ByteBuffer ObjectToBytes(FLZKOp O, ByteBuffer buffer)
	{
		try
		{
			buffer.clear();
			ByteBufferOutputStream baos = new ByteBufferOutputStream(buffer);
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(O);
			buffer.flip();
			return baos.buffer;
		}
		catch(IOException ioe)
		{
			throw new RuntimeException(ioe);
		}
	}

	static CacheArrayOutputStream ObjectToBytes(FLZKOp O, byte[] buffer)
	{
		try
		{
			CacheArrayOutputStream baos = new CacheArrayOutputStream(buffer);
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(O);
			return baos;
		}
		catch(IOException ioe)
		{
			throw new RuntimeException(ioe);
		}
	}

	private final static class ByteBufferOutputStream extends java.io.OutputStream {
		private ByteBuffer buffer;

		ByteBufferOutputStream(ByteBuffer b) {
			buffer = b;
		}

		public final void write(byte[] b) {
			write(ByteBuffer.wrap(b));
		}

		public final void write(byte[] b, int off, int len) {
			write(ByteBuffer.wrap(b, off, len));
		}

		public final void write(ByteBuffer b) {
			if(buffer.remaining() < b.remaining()) {
				int newCap = buffer.capacity() * 2;
				if (newCap - buffer.remaining() < b.remaining()) {
					newCap = buffer.capacity() + b.remaining();
				}
				ByteBuffer newBuffer = ByteBuffer.allocateDirect(newCap);
				this.buffer.flip();
				newBuffer.put(this.buffer);
				this.buffer = newBuffer;
			}

			this.buffer.put(b);
		}

		public final void write(int b) {
			this.buffer.put((byte) b);
		}
	}

	private final static class CacheArrayOutputStream extends java.io.OutputStream {
		private ByteBuffer buffer;

		CacheArrayOutputStream(byte[] b) {
			buffer = ByteBuffer.wrap(b);
		}

		public final void write(byte[] b) {
			write(ByteBuffer.wrap(b));
		}

		public final void write(byte[] b, int off, int len) {
			write(ByteBuffer.wrap(b, off, len));
		}

		public final void write(ByteBuffer b) {
			if(buffer.remaining() < b.remaining()) {
				int newCap = buffer.capacity() * 2;
				if (newCap - buffer.remaining() < b.remaining()) {
					newCap = buffer.capacity() + b.remaining();
				}
				ByteBuffer newBuffer = ByteBuffer.allocate(newCap);
				this.buffer.flip();
				newBuffer.put(this.buffer);
				this.buffer = newBuffer;
			}

			this.buffer.put(b);
		}

		public final void write(int b) {
			this.buffer.put((byte) b);
		}

		public byte[] buffer() {
			return this.buffer.array();
		}

		public int length() {
			return this.buffer.position();
		}
	}

	public void waitForResults(Object[] results) throws KeeperException
	{
		synchronized(results)
		{
			while(results[0]==null)
			{
				try
				{
					results.wait();
				}
				catch(InterruptedException ie)
				{
					continue;
				}
			}
		}
		int x = ((Integer)results[0]).intValue();
		if(x!=0)
			throw KeeperException.create(Code.get(x));
	}

	//synchronous methods -- create, exists, setdata:
	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException
	{
		Object[] results = new Object[2];
		//call asynchronous version
		this.create(path, data, acl, createMode, new AsyncToSync(), results);
		waitForResults(results);
		return (String)results[1];
	}

	public Object localCreate(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException
	{
		Object[] results = new Object[2];
		//call asynchronous version
		CreateOp cop = new CreateOp(path, data, acl, createMode, new AsyncToSync(), null);
		return this.apply(cop);
	}

	public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException
	{
		Object[] results = new Object[2];
		this.setData(path, data, version, new AsyncToSync(), results);
		waitForResults(results);
		return (Stat)results[1];
	}

	public Stat exists(String path, boolean watcher) throws KeeperException, InterruptedException
	{
		Object[] results = new Object[2];
		this.exists(path, watcher, new AsyncToSync(), results);
		waitForResults(results);
		return (Stat)results[1];
	}

	public void delete(String path, int version) throws KeeperException
	{
		Object[] results = new Object[1];
		this.delete(path, version, new AsyncToSync(), results);
		waitForResults(results);
	}

	public byte[] getData(String path, boolean watcher, Stat stat) throws KeeperException, InterruptedException
	{
		//todo -- what about stat?
		Object[] results = new Object[2];
		this.getData(path, watcher, new AsyncToSync(), results);
		waitForResults(results);
		return (byte[])results[1];
	}

	public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException
	{
		Object[] results = new Object[2];
		this.getChildren(path, watch, new AsyncToSync(), results);
		waitForResults(results);
		return (List<String>)results[1];
	}


	//mutating async ops: create, delete, setdata
	public void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctxt)
	{
		CreateOp cop = new CreateOp(path, data, acl, createMode, cb, ctxt);
		processAsync(cop, true);
	}

	public void delete(String path, int version, AsyncCallback.VoidCallback cb, Object ctx)
	{
		DeleteOp dop = new DeleteOp(path, version, cb, ctx);
		processAsync(dop, true);
	}

	public void setData(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx)
	{
		SetOp sop = new SetOp(path, data, version, cb, ctx);
		processAsync(sop, true);
	}

	//non-mutating async

	public void exists(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx)
	{
		ExistsOp eop = new ExistsOp(path, watch, cb, ctx);
		processAsync(eop, false);
	}

	public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx)
	{
		GetOp gop = new GetOp(path, watch, cb, ctx);
		processAsync(gop, false);
	}

	public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx)
	{
		GetChildrenOp gcop = new GetChildrenOp(path, watch, cb, ctx);
		processAsync(gcop, false);
	}

}

class AsyncToSync implements AsyncCallback.StringCallback, AsyncCallback.StatCallback, AsyncCallback.VoidCallback, AsyncCallback.DataCallback, AsyncCallback.ChildrenCallback
{

	@Override
	public void processResult(int rc, String path, Object ctx, String name)
	{
		Object[] results = (Object[])ctx;
		synchronized(results)
		{
			results[0] = new Integer(rc);
			results[1] = name;
			results.notify();
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat)
	{
		Object[] results = (Object[])ctx;
		synchronized(results)
		{
			results[0] = new Integer(rc);
			results[1] = stat;
			results.notify();
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx)
	{
		Object[] results = (Object[])ctx;
		synchronized(results)
		{
			results[0] = new Integer(rc);
			results.notify();
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, byte[] data,
			Stat stat)
	{
		Object[] results = (Object[])ctx;
		synchronized(results)
		{
			results[0] = new Integer(rc);
			results[1] = data;
			results.notify();
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, List<String> children)
	{
		Object[] results = (Object[])ctx;
		synchronized(results)
		{
			results[0] = new Integer(rc);
			results[1] = children;
			results.notify();
		}
	}
}

class Node implements Serializable
{
	transient Stat stat;
	Lock L;
	byte data[];
	boolean ephemeral;
	List<Node> children;
	Map<File, AtomicInteger> sequentialcounters;
	String path;
	Set<Watcher> datawatches;
	Set<Watcher> childrenwatches;
	public Node(byte data[], String tpath)
	{
		ephemeral = false;
		this.data = data;
		children = new ArrayList<Node>();
		L = new ReentrantLock();
		stat = new Stat();
		path = tpath;
		datawatches = new HashSet<Watcher>();
		childrenwatches = new HashSet<Watcher>();
		sequentialcounters = new HashMap<File, AtomicInteger>();
	}
	public void lock()
	{
		L.lock();
	}
	public void unlock()
	{
		L.unlock();
	}
	public byte[] getData()
	{
		return data;
	}
	public boolean isEphemeral()
	{
		return ephemeral;
	}
	public String toString()
	{
		return path;
	}
}
