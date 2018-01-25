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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
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

	public SetOp(FLZKOp flop)
	{
		super(flop.id, flop.kind);
	}

	public static final byte kind = 1;
	public final byte kind() { return this.kind; }

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

	@Override
	public final void writeData(DataOutputStream out) throws IOException {
		super.writeData(out);
		out.writeInt(data.length);
		out.write(data);
		out.writeInt(version);
		out.writeUTF(path);
	}
	public void readFields(DataInputStream in) throws IOException {
		int dataLen = in.readInt();
		this.data = new byte[dataLen];
		in.readFully(data);
		this.version = in.readInt();
		this.path = in.readUTF();
	}

	@Override
	public final SetOp readData(DataInputStream in) throws IOException {
		FLZKOp sup = (FLZKOp) FLZKOp.INSTANCE.readData(in);
		throw new NotImplementedException();
	}

	public boolean hasCallback() {
		return cb != null;
	}
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

	public CreateOp(FLZKOp flop)
	{
		super(flop.id, flop.kind);
	}

	public void callback(KeeperException ke, Object O)
	{
		if(ke!=null)
			cb.processResult(ke.code().intValue(), path, ctxt, (String)O);
		else
			cb.processResult(Code.OK.intValue(), path, ctxt, (String)O);
	}

	public static final byte kind = 2;
	public final byte kind() { return this.kind; }

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

	@Override
	public final void writeData(DataOutputStream out) throws IOException {
		super.writeData(out);
		out.writeInt(cm.toFlag());
		out.writeInt(data.length);
		out.write(data);
		if(acl != null) {
            int len = acl.size();
            out.writeInt(len);
            for(ACL a: acl) a.write(out);
        } else {
            out.writeInt(0);
        }
		out.writeUTF(path);
	}
	public void readFields(DataInputStream in) throws IOException {
		try {
			this.cm = CreateMode.fromFlag(in.readInt());
		} catch(KeeperException e) {
			throw new RuntimeException(e);
		}

        int dataLen = in.readInt();
        this.data = new byte[dataLen];
        in.readFully(data);
		int aclLen = in.readInt();
		if(aclLen <= 0) {
			this.acl = Collections.emptyList();
		} else {
			this.acl = new ArrayList<>(aclLen);
			for(int i = 0; i < aclLen; i++) {
				ACL a = new ACL();
				a.readFields(in);
				acl.add(a);
			}
		}
        this.path = in.readUTF();
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

	public static final byte kind = 3;
	public final byte kind() { return this.kind; }

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

	public static final byte kind = 4;
	public final byte kind() { return this.kind; }
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

	public static final byte kind = 5;
	public final byte kind() { return this.kind; }

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

	public static final byte kind = 6;
	public final byte kind() { return this.kind; }

	public DeleteOp(String path, int version, AsyncCallback.VoidCallback tcb, Object tctxt)
	{
		this.path = path;
		this.version = version;
		this.cb = tcb;
		this.ctxt = tctxt;
	}

	public DeleteOp(FLZKOp flop)
	{
		super(flop.id, flop.kind);
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

	@Override
	public final void writeData(DataOutputStream out) throws IOException {
		super.writeData(out);
		out.writeInt(version);
		out.writeUTF(path);
	}
	public void readFields(DataInputStream in) throws IOException {
		this.version = in.readInt();
		this.path = in.readUTF();
	}

	public boolean hasCallback() {
		return cb != null;
	}
}

final class OpData implements ProxyHandle.Data {

	public static final OpData BUILDER = new OpData();

	public final FLZKOp op;

	private OpData() {
		op = null;
	}

	public OpData(FLZKOp flop) {
		this.op = flop;
	}

	@Override
	public void writeData(DataOutputStream out) throws IOException {
		op.writeData(out);
	}

	@Override
	public OpData readData(DataInputStream in) throws IOException {
		FLZKOp flop = FLZKOp.INSTANCE.readData(in);
		switch(flop.kind) {
			case SetOp.kind:
				SetOp sop = new SetOp(flop);
				sop.readFields(in);
				return new OpData(sop);

			case CreateOp.kind:
				CreateOp cop = new CreateOp(flop);
				cop.readFields(in);
				return new OpData(cop);

			case DeleteOp.kind:
				DeleteOp dop = new DeleteOp(flop);
				dop.readFields(in);
				return new OpData(dop);

			default: throw new IOException("unrecoginzed kind " + flop.kind);
		}
	}
}

final class MarkerOp extends FLZKOp
{

	public static final MarkerOp Instance = new MarkerOp();

	public static final byte kind = 7;
	public final byte kind() { return this.kind; }

	public MarkerOp() {}

	public void callback(KeeperException ke, Object O) {}

	public boolean hasCallback() {
		return false;
	}
}

final class WakeOp extends FLZKOp
{

	public static final WakeOp Instance = new WakeOp();

	public static final byte kind = 8;
	public final byte kind() { return this.kind; }

	public WakeOp() {}

	public void callback(KeeperException ke, Object O) {}

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
	private final Map<File, Node> map;

	private final ConcurrentHashMap<Object, FLZKOp> pendinglist;  //mutating operations, must sunchronize on appendclient
	private final LinkedBlockingQueue<FLZKOp> passivelist; //non-mutating operations

	private final LinkedBlockingQueue<Object> callbacklist;
	private final Watcher defaultwatcher;
	private final Map<String, Set<Watcher>> existswatches;

	private final LinkedBlockingQueue<FLZKOp> fromDe;

	private byte[] writeBuffer;

	private long numentries=0;


	private final int mycolor;
	private final int[] bothColors;

	private final boolean debugprints = false;

	private final boolean justSend;

	public FLZK(ProxyHandle client, int tmycolor, int othercolor, Watcher W) throws Exception, KeeperException
	{
		this(client, tmycolor, othercolor, W, false);
	}

	public FLZK(ProxyHandle client, int tmycolor, int othercolor, Watcher W, boolean justSend) throws Exception, KeeperException
	{
		this.client = client;

		defaultwatcher = W;
		pendinglist = new ConcurrentHashMap<Object, FLZKOp>();
		passivelist = new LinkedBlockingQueue<FLZKOp>();
		callbacklist = new LinkedBlockingQueue<Object>();
		// map = new TreeMap<File, Node>();
		map = new HashMap<File, Node>(10_000_000);
		map.put(new File("/"), new Node("foobar".getBytes(), "/"));
		existswatches = new HashMap<String, Set<Watcher>>();
		mycolor = tmycolor;
		bothColors = new int[] {mycolor, othercolor};
		this.justSend = justSend;

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

		fromDe = new LinkedBlockingQueue<>();

		new Thread(() -> {
			// int objectsApplied = 0;
			while(true) {
				try {
					FLZKOp zop = fromDe.take();
					FLZKOp mycop = pendinglist.remove(zop.id);
					if(mycop != null) zop = mycop;
					Object ret = null;
					try
					{
						// if(debugprints) System.out.println("applying op" + zop);
						ret = this.apply(zop);
						//if(zop.hasCallback()) {
							// objectsApplied += 1;
						//	schedulecallback(zop, null, ret);
							//mycop.callback(null, ret);
							// System.out.println("objects applied " + objectsApplied);
						//}
						if((zop.hasCallback())) {
							zop.callback(null, ret);
						}
					}
					catch(KeeperException ke)
					{
						if(zop.hasCallback()) zop.callback(ke, ret);
						//if(zop.hasCallback()) schedulecallback(zop, ke, ret);
							//mycop.callback(ke, ret);
					}

				} catch(InterruptedException e) { }
			}
		}).start();
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
			// client.append(mycolor, ObjectToBytes(flop));
			// CacheArrayOutputStream out = ObjectToBytes(flop, this.writeBuffer);
			// pendinglist.put(flop.id, flop);
			// client.append(mycolor, out.buffer(), out.length());
			// passivelist.add(WakeOp.Instance);
			// this.writeBuffer = out.buffer();
			//we don't have to wait for the append to finish;
			//we just wait for it to appear in the learner thread
			// if(debugprints) System.out.println("process " + flop);
			pendinglist.put(flop.id, flop);
			if(flop instanceof CreateOp) {
				// System.out.println("> " + Arrays.toString(bothColors));
				client.append(bothColors, flop);
			} else {
				// System.out.println("> " + mycolor);
				client.append(mycolor, flop);
			}

			if(!justSend) passivelist.add(WakeOp.Instance);
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
		while(true) {
			FLZKOp pop;
			if(pendinglist.isEmpty()) {
				pop = passivelist.take();
			} else {
				pop = passivelist.poll();
			}
			if(pendinglist.isEmpty() && pop == MarkerOp.Instance) {
				pop = passivelist.take();
			}

			// if(debugprints) System.out.println("sync");
			// int objectsFound = 0;
			// long startTime = System.nanoTime();
			// new passive events must wait for the next snapshot
			passivelist.offer(MarkerOp.Instance);
			// snapshot mycolor
			ProxyHandle.DataStream<OpData> events = client.snapshot_and_get_data(OpData.BUILDER);
			for(OpData event: events) {
				// objectsFound += 1;
				numentries++;
				// if(debugprints) System.out.println("Tail not reached");
				fromDe.offer(event.op);
				// long fetchTime = System.nanoTime() - startTime;
				// System.out.println("loop time: " + fetchTime + "ns ");
				// startTime = System.nanoTime();
			}
			// long fetchTime = System.nanoTime() - startTime;
			// double hz = 1_000_000_000.0 * (double)objectsFound / (double)fetchTime;
			// if(objectsFound != 0) System.out.println("> fetch time: " + fetchTime + "ns " + objectsFound + " events " + hz + "hz");
			// System.out.println("> fetch time: " + fetchTime + "ns " + objectsFound + " events " + hz + "hz");


			handle_passive: for(; pop != MarkerOp.Instance; pop = passivelist.poll()) {
				if (pop == null || pop == WakeOp.Instance) continue handle_passive;
				if (pop == MarkerOp.Instance) break handle_passive;
				//TODO we don't need passive ops to be totally ordered
				fromDe.offer(pop);
			}
			// if(debugprints && objectsFound != 0) System.out.println("finished sync: " + numentries);
			// if(debugprints) System.out.println("finished sync: " + passivelist.size() + ", " + pendinglist.size());
		}
	}

	public long getNumEvents() {
		return numentries;
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
			// System.out.println(cop.getClass());
			throw new RuntimeException("This should never get called");
		}
	}

	public Object apply(CreateOp cop) throws KeeperException
	{
		String path = cop.path;
		File f = new File(path);
		path = f.getPath();

		Node N = map.get(f.getParentFile());
		if(N == null) throw new KeeperException.NoNodeException();

		if(cop.cm.isEphemeral()) throw new RuntimeException("not yet supported!");
		if(N.isEphemeral()) throw new KeeperException.NoChildrenForEphemeralsException();


		if(cop.cm.isSequential())
		{
			NodeAux aux = N.aux.getOrInit();
			AtomicInteger I = aux.sequentialcounters.get(f);
			if(I==null)
			{
				I = new AtomicInteger(0);
				aux.sequentialcounters.put(f,  I);
			}
			int x = I.getAndIncrement();
			String y = String.format("%010d", x);
			path = path + y;
			f = new File(path);
			path = f.getPath();
		}

		Node newnode = new Node(cop.data, path);
		Node old = map.put(f, newnode); //map is synchronized on 'this'
		if(old != null) {
			map.put(f, old);
			throw new KeeperException.NodeExistsException();
		}

//		N.lock();
		newnode.stat.setCzxid(numentries); //copy stat either here or on read
		//TODO: is it okay to use numentries here? will this be problematic if different clients see nodes in different orders?
		N.aux.getOrInit().children.add(newnode);
		//existswatches is synchronized on 'this'
		if(existswatches.isEmpty()) return path;
		Set<Watcher> watches = existswatches.get(newnode.path);
		if(watches != null && !watches.isEmpty())
			triggerwatches(watches, new WatchedEvent(Watcher.Event.EventType.NodeCreated, KeeperState.SyncConnected,newnode.path));
		if(watches != null && !N.aux.get().childrenwatches.isEmpty())
			triggerwatches(N.aux.get().childrenwatches, new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, KeeperState.SyncConnected, N.path));
//		N.unlock();

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
		NodeAux aux = N.aux.get();
		if(aux != null && aux.children.size()>0)
			throw new KeeperException.NotEmptyException();
		Node parent = map.get(F.getParentFile());
//		parent.lock();
		parent.aux.get().children.remove(N);
		triggerwatches(parent.aux.get().childrenwatches, new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, KeeperState.SyncConnected, parent.path));
//		parent.unlock();
		if(aux != null && aux.datawatches != null && !aux.datawatches.isEmpty())
			triggerwatches(aux.datawatches, new WatchedEvent(Watcher.Event.EventType.NodeDeleted, KeeperState.SyncConnected, N.path));
//		N.unlock();
		map.remove(F);
		return null;
	}

	public Object apply(ExistsOp eop) throws KeeperException
	{
		File F = new File(eop.path);
		Node N = map.get(F);
		if(N == null)
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
//		N.lock();
		Stat x = N.stat;
		if(eop.watch && defaultwatcher!=null) N.aux.getOrInit().datawatches.add(defaultwatcher);
//		N.unlock();
		return x; //todo -- return copy of stat?
	}

	public Object apply(SetOp sop) throws KeeperException
	{
		File f = new File(sop.path);
		Node N = map.get(f);
		if(N == null) throw new KeeperException.NoNodeException();
//		N.lock();
		if(sop.version!=-1 && sop.version!=N.stat.getVersion())
			throw new KeeperException.BadVersionException();
		N.data = sop.data;
		N.stat.setMzxid(numentries); //todo: can this cause problems since we don't use curpos?
		Stat x = N.stat;
		//mb: triggers both 'exists' and 'getdata' watches
		NodeAux aux = N.aux.get();
		if(aux != null && aux.datawatches != null && !aux.datawatches.isEmpty())
			triggerwatches(aux.datawatches, new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, KeeperState.SyncConnected, N.path));
//		N.unlock();
		return x; //handle stat copying either here or at modification
	}

		public Object apply(GetOp gop) throws KeeperException
	{
		File F = new File(gop.path);
		Node N = map.get(F);
		if(N == null) throw new KeeperException.NoNodeException();

//		N.lock();
		Stat x = N.stat;
		byte[] y = N.data;
		if(gop.watch && defaultwatcher!=null) N.aux.getOrInit().datawatches.add(defaultwatcher);
//		N.unlock();
		return new Pair<byte[], Stat>(y, x); //todo -- copy data and stat

	}

	public Object apply(GetChildrenOp gcop) throws KeeperException
	{
		File F = new File(gcop.path);
		Node N = map.get(F);
		if(N == null) throw new KeeperException.NoNodeException();

//		N.lock();

		final List<String> childNames;
		NodeAux aux = N.aux.get();
		if(aux == null) {
			if(gcop.watch && defaultwatcher!=null) {
				N.aux.getOrInit().childrenwatches.add(defaultwatcher);
			}
			return Collections.emptyList();
		}

		List<Node> children = aux.children;
		if(children.isEmpty()) childNames = Collections.emptyList();
		else {
			childNames = new ArrayList<>(children.size());
			for(Node n: children) childNames.add(n.path);
		}
		if(gcop.watch && defaultwatcher!=null) aux.childrenwatches.add(defaultwatcher);
//		N.unlock();
		return childNames;
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

	public void waitForResults(Results results) throws KeeperException
	{
		synchronized(results)
		{
			while(!results.ready)
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
		if(results.code != 0) throw KeeperException.create(Code.get(results.code));
	}

	//synchronous methods -- create, exists, setdata:
	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException
	{
		Results results = AsyncToSync.ret.get();
		results.ready = false;
		//call asynchronous version
		this.create(path, data, acl, createMode, new AsyncToSync(), results);
		waitForResults(results);
		return (String)results.val;
	}

	public Object localCreate(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException
	{
		CreateOp cop = new CreateOp(path, data, acl, createMode, new AsyncToSync(), null);
		return this.apply(cop);
	}

	public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException
	{
		Results results = AsyncToSync.ret.get();
		results.ready = false;
		this.setData(path, data, version, new AsyncToSync(), results);
		waitForResults(results);
		return (Stat)results.val;
	}

	public Stat exists(String path, boolean watcher) throws KeeperException, InterruptedException
	{
		Results results = AsyncToSync.ret.get();
		results.ready = false;
		this.exists(path, watcher, new AsyncToSync(), results);
		waitForResults(results);
		return (Stat)results.val;
	}

	public void delete(String path, int version) throws KeeperException
	{
		Results results = AsyncToSync.ret.get();
		results.ready = false;
		this.delete(path, version, new AsyncToSync(), results);
		waitForResults(results);
	}

	public byte[] getData(String path, boolean watcher, Stat stat) throws KeeperException, InterruptedException
	{
		//todo -- what about stat?
		Results results = AsyncToSync.ret.get();
		results.ready = false;
		this.getData(path, watcher, new AsyncToSync(), results);
		waitForResults(results);
		return (byte[])results.val;
	}

	public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException
	{
		Results results = AsyncToSync.ret.get();
		results.ready = false;
		this.getChildren(path, watch, new AsyncToSync(), results);
		waitForResults(results);
		return (List<String>)results.val;
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

final class Results {
	public boolean ready = false;
	public int code = 0;
	public Object val = null;
}

class AsyncToSync implements AsyncCallback.StringCallback, AsyncCallback.StatCallback, AsyncCallback.VoidCallback, AsyncCallback.DataCallback, AsyncCallback.ChildrenCallback
{
	static final ThreadLocal<Results> ret =
		new ThreadLocal<Results>() {
			@Override protected final Results initialValue() {
					return new Results();
			}
		};

	@Override
	public void processResult(int rc, String path, Object ctx, String name)
	{
		Results results = (Results)ctx;
		synchronized(results)
		{
			results.code = rc;
			results.val = name;
			results.ready = true;
			results.notify();
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, Stat stat)
	{
		Results results = (Results)ctx;
		synchronized(results)
		{
			results.code = rc;
			results.val = stat;
			results.ready = true;
			results.notify();
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx)
	{
		Results results = (Results)ctx;
		synchronized(results)
		{
			results.code = rc;
			results.val = null;
			results.ready = true;
			results.notify();
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, byte[] data,
			Stat stat)
	{
		Results results = (Results)ctx;
		synchronized(results)
		{
			results.code = rc;
			results.val = data;
			results.ready = true;
			results.notify();
		}
	}

	@Override
	public void processResult(int rc, String path, Object ctx, List<String> children)
	{
		Results results = (Results)ctx;
		synchronized(results)
		{
			results.code = rc;
			results.val = children;
			results.ready = true;
			results.notify();
		}
	}
}

abstract class Lazy<T> {
	private T val = null;

	public final T get() {
		return val;
	}

	public final T getOrInit() {
		if(val == null) val = init();
		return val;
	}

	protected abstract T init();
}

class NodeAux {
	final List<Node> children = new LinkedList<Node>();
	final Map<File, AtomicInteger> sequentialcounters = new HashMap<File, AtomicInteger>();
	final Set<Watcher> datawatches = new HashSet<Watcher>();
	final Set<Watcher> childrenwatches = new HashSet<Watcher>();
}

class Node implements Serializable
{
	transient Stat stat;
	// Lock L;
	byte data[];
	final boolean ephemeral;
	final String path;

	Lazy<NodeAux> aux =
	 new Lazy<NodeAux>() {
		@Override protected NodeAux init() {
			return new NodeAux();
		}
	 };

	public Node(byte data[], String tpath)
	{
		ephemeral = false;
		this.data = data;
		// children = new LinkedList<Node>();
		// L = new ReentrantLock();
		stat = new Stat();
		path = tpath;
		// datawatches = new HashSet<Watcher>();
		// childrenwatches = new HashSet<Watcher>();
		// sequentialcounters = new HashMap<File, AtomicInteger>();
	}
	/*public void lock()
	{
		L.lock();
	}
	public void unlock()
	{
		L.unlock();
	}*/
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
