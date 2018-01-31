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

	public String path() {
		return this.path;
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

	public String path() {
		return this.path;
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

	public String path() {
		return this.path;
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

	public String path() {
		return this.path;
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

	public String path() {
		return this.path;
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

	public String path() {
		return this.path;
	}
}

interface Rename {}

final class RenamePart1 extends FLZKOp implements Rename
{
	String oldPath;
	String newPath;
	Object ctxt;
	AsyncCallback.StringCallback cb;

	@Override
	public final boolean equals(Object cop) {
		if(cop == null) return false;
		if(this == cop) return true;
		if(cop instanceof RenamePart1) {
			RenamePart1 rop = (RenamePart1)cop;
			return rop.oldPath.equals(this.oldPath) && rop.newPath.equals(this.newPath);
		}
		return false;
	}

	public RenamePart1(String oldPath, String newPath,  AsyncCallback.StringCallback cb, Object ctxt)
	{
		this.oldPath = oldPath;
		this.newPath = newPath;
		this.cb = cb;
		this.ctxt = ctxt;
	}

	public RenamePart1(FLZKOp flop)
	{
		super(flop.id, flop.kind);
	}

	public void callback(KeeperException ke, Object O)
	{
		throw new RuntimeException("unimplemented");
		// if(ke!=null)
		// 	cb.processResult(ke.code().intValue(), path, ctxt, (String)O);
		// else
		// 	cb.processResult(Code.OK.intValue(), path, ctxt, (String)O);
	}

	public static final byte kind = 7;
	public final byte kind() { return this.kind; }

	private void writeObject(java.io.ObjectOutputStream out) throws IOException
	{
		out.writeObject(oldPath);
		out.writeObject(newPath);
	}
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
	{
		oldPath = (String)in.readObject();
		newPath = (String)in.readObject();
	}

	@Override
	public final void writeData(DataOutputStream out) throws IOException {
		super.writeData(out);
		out.writeUTF(oldPath);
		out.writeUTF(newPath);
	}

	public void readFields(DataInputStream in) throws IOException {
		this.oldPath = in.readUTF();
		this.newPath = in.readUTF();
	}

	public boolean hasCallback() {
		return cb != null;
	}

	public String path() {
		return this.oldPath;
	}

	public String path2() {
		return this.newPath;
	}
}

final class RenameOldExists extends FLZKOp implements Rename
{
	String oldPath;
	String newPath;
	CreateMode cm;
	byte[] data;
	List<ACL> acl;

	@Override
	public final boolean equals(Object cop) {
		if(cop == null) return false;
		if(this == cop) return true;
		if(cop instanceof RenameOldExists) {
			RenameOldExists rop = (RenameOldExists)cop;
			return rop.oldPath.equals(this.oldPath) &&
				rop.newPath.equals(this.newPath) &&
				rop.cm.equals(this.cm) &&
				Arrays.equals(rop.data, this.data) &&
				rop.acl.equals(this.acl);
		}
		return false;
	}

	public RenameOldExists(String oldPath, String newPath, byte[] data, List<ACL> acl, CreateMode createMode)
	{
		this.oldPath = oldPath;
		this.newPath = newPath;
		this.data = data;
		this.acl = acl;
		this.cm = createMode;
	}

	public RenameOldExists(FLZKOp flop)
	{
		super(flop.id, flop.kind);
	}

	public static final byte kind = 8;
	public final byte kind() { return this.kind; }

	private void writeObject(java.io.ObjectOutputStream out) throws IOException
	{
		out.writeObject(cm);
		out.writeObject(data);
		out.writeObject(acl);
		out.writeObject(oldPath);
		out.writeObject(newPath);
	}
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
	{
		cm = (CreateMode)in.readObject();
		data = (byte[])in.readObject();
		acl = (List<ACL>)in.readObject();
		oldPath = (String)in.readObject();
		newPath = (String)in.readObject();
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
		out.writeUTF(oldPath);
		out.writeUTF(newPath);
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
		this.oldPath = in.readUTF();
        this.newPath = in.readUTF();
	}

	public void callback(KeeperException ke, Object O)
	{
		throw new RuntimeException("unimplemented");
		// if(ke!=null)
		// 	cb.processResult(ke.code().intValue(), path, ctxt, (String)O);
		// else
		// 	cb.processResult(Code.OK.intValue(), path, ctxt, (String)O);
	}

	public boolean hasCallback() {
		return false;
	}

	public String path() {
		return this.oldPath;
	}

	public String path2() {
		return this.newPath;
	}
}

final class RenameNewEmpty extends FLZKOp implements Rename
{
	String oldPath;
	String newPath;

	@Override
	public final boolean equals(Object cop) {
		if(cop == null) return false;
		if(this == cop) return true;
		if(cop instanceof RenameNewEmpty) {
			RenameNewEmpty rop = (RenameNewEmpty)cop;
			return rop.oldPath.equals(this.oldPath) && rop.newPath.equals(this.newPath);
		}
		return false;
	}

	public RenameNewEmpty(String oldPath, String newPath)
	{
		this.oldPath = oldPath;
		this.newPath = newPath;
	}

	public RenameNewEmpty(FLZKOp flop)
	{
		super(flop.id, flop.kind);
	}

	public static final byte kind = 9;
	public final byte kind() { return this.kind; }

	private void writeObject(java.io.ObjectOutputStream out) throws IOException
	{
		out.writeObject(oldPath);
		out.writeObject(newPath);
	}
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
	{
		oldPath = (String)in.readObject();
		newPath = (String)in.readObject();
	}

	@Override
	public final void writeData(DataOutputStream out) throws IOException {
		super.writeData(out);
		out.writeUTF(oldPath);
		out.writeUTF(newPath);
	}
	public void readFields(DataInputStream in) throws IOException {
		this.oldPath = in.readUTF();
        this.newPath = in.readUTF();
	}

	public void callback(KeeperException ke, Object O)
	{
		throw new RuntimeException("unimplemented");
		// if(ke!=null)
		// 	cb.processResult(ke.code().intValue(), path, ctxt, (String)O);
		// else
		// 	cb.processResult(Code.OK.intValue(), path, ctxt, (String)O);
	}

	public boolean hasCallback() {
		return false;
	}

	public String path() {
		return this.oldPath;
	}

	public String path2() {
		return this.newPath;
	}
}

final class RenameNack extends FLZKOp implements Rename
{
	String oldPath;
	String newPath;

	@Override
	public final boolean equals(Object cop) {
		if(cop == null) return false;
		if(this == cop) return true;
		if(cop instanceof RenameNack) {
			RenameNack rop = (RenameNack)cop;
			return rop.oldPath.equals(this.oldPath) && rop.newPath.equals(this.newPath);
		}
		return false;
	}

	public RenameNack(String oldPath, String newPath)
	{
		this.oldPath = oldPath;
		this.newPath = newPath;
	}

	public RenameNack(FLZKOp flop)
	{
		super(flop.id, flop.kind);
	}

	public static final byte kind = 10;
	public final byte kind() { return this.kind; }

	private void writeObject(java.io.ObjectOutputStream out) throws IOException
	{
		out.writeObject(oldPath);
		out.writeObject(newPath);
	}
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
	{
		oldPath = (String)in.readObject();
		newPath = (String)in.readObject();
	}

	@Override
	public final void writeData(DataOutputStream out) throws IOException {
		super.writeData(out);
		out.writeUTF(oldPath);
		out.writeUTF(newPath);
	}
	public void readFields(DataInputStream in) throws IOException {
        this.oldPath = in.readUTF();
        this.newPath = in.readUTF();
	}

	public void callback(KeeperException ke, Object O)
	{
		throw new RuntimeException("unimplemented");
		// if(ke!=null)
		// 	cb.processResult(ke.code().intValue(), path, ctxt, (String)O);
		// else
		// 	cb.processResult(Code.OK.intValue(), path, ctxt, (String)O);
	}

	public boolean hasCallback() {
		return false;
	}

	public String path() {
		return this.oldPath;
	}

	public String path2() {
		return this.newPath;
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

			case RenamePart1.kind:
				RenamePart1 rp1 = new RenamePart1(flop);
				rp1.readFields(in);
				return new OpData(rp1);

			case RenameOldExists.kind:
				RenameOldExists rp2 = new RenameOldExists(flop);
				rp2.readFields(in);
				return new OpData(rp2);

			case RenameNewEmpty.kind:
				RenameNewEmpty rne = new RenameNewEmpty(flop);
				rne.readFields(in);
				return new OpData(rne);

			case RenameNack.kind:
				RenameNack rn = new RenameNack(flop);
				rn.readFields(in);
				return new OpData(rn);

			default: throw new IOException("unrecoginzed kind " + flop.kind + " " + flop.id + " " + flop.idcounter.get());
		}
	}
}

final class MarkerOp extends FLZKOp
{

	public static final MarkerOp Instance = new MarkerOp();

	public static final byte kind = 125;
	public final byte kind() { return this.kind; }

	public MarkerOp() {}

	public void callback(KeeperException ke, Object O) {}

	public boolean hasCallback() {
		return false;
	}

	public String path() {
		return null;
	}
}

final class WakeOp extends FLZKOp
{

	public static final WakeOp Instance = new WakeOp();

	public static final byte kind = 127;
	public final byte kind() { return this.kind; }

	public WakeOp() {}

	public void callback(KeeperException ke, Object O) {}

	public boolean hasCallback() {
		return false;
	}

	public String path() {
		return null;
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

	private final ConcurrentHashMap<Object, FLZKOp> pendingOps;  //mutating operations, must sunchronize on appendclient
	private final LinkedBlockingQueue<FLZKOp> passivelist; //non-mutating operations

	private final LinkedBlockingQueue<Object> callbacklist;
	private final Watcher defaultwatcher;
	private final Map<String, Set<Watcher>> existswatches;

	private final LinkedBlockingQueue<FLZKOp> fromDe;

	// private byte[] writeBuffer;

	private long numentries=0;

	private final String myRoot;
	private final int mycolor;
	private final HashMap<String, Integer> colorForRoot;

	private final boolean debugprints = false;

	public FLZK(ProxyHandle client, int tmycolor, String myRoot, HashMap<String, Integer> colorForRoot, Watcher W) throws Exception, KeeperException
	{
		this.client = client;

		defaultwatcher = W;
		pendingOps = new ConcurrentHashMap<Object, FLZKOp>();
		passivelist = new LinkedBlockingQueue<FLZKOp>();
		callbacklist = new LinkedBlockingQueue<Object>();
		map = new HashMap<File, Node>();
		existswatches = new HashMap<String, Set<Watcher>>();
		this.myRoot = myRoot;
		this.colorForRoot = colorForRoot;
		{
			Node rootNode = new Node("root".getBytes(), "/");
			for(String partition: colorForRoot.keySet()) {
				Node node = new Node(partition.getBytes(), partition);
				rootNode.children.add(node);
				map.put(new File(node.path), node);
			}
			map.put(new File(rootNode.path), rootNode);
		}

		mycolor = tmycolor;

		// writeBuffer = new byte[512];

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
					if(!(zop instanceof Rename)) {
						FLZKOp mycop = pendingOps.remove(zop.id);
						if(mycop != null) {
							zop = mycop;
						}
					} else {
						FLZKOp mycop = pendingOps.get(zop.id);
						if(mycop != null && mycop.equals(zop)) {
							zop = mycop;
							pendingOps.remove(zop.id);
						}
					}
					Object ret = null;
					try
					{
						// if(debugprints) System.out.println("applying op" + zop);
						ret = this.apply(zop);
						if(zop.hasCallback()) {
							// objectsApplied += 1;
							schedulecallback(zop, null, ret);
							//mycop.callback(null, ret);
							// System.out.println("objects applied " + objectsApplied);
						}

					}
					catch(KeeperException ke)
					{
						if(zop.hasCallback()) schedulecallback(zop, ke, ret);
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


	//TODO is there a better way to do this?
	// synhronization: everything in this method needs to be threadsafe on its own
	void processAsync(FLZKOp flop, boolean mutate)
	{
		//signal to learning thread
		// if(debugprints) System.out.println("process");
		if(mutate)
		{
			// client.append(mycolor, ObjectToBytes(flop));
			// CacheArrayOutputStream out = ObjectToBytes(flop, this.writeBuffer);
			// pendingOps.put(flop.id, flop);
			// client.append(mycolor, out.buffer(), out.length());
			// passivelist.add(WakeOp.Instance);
			// this.writeBuffer = out.buffer();
			//we don't have to wait for the append to finish;
			//we just wait for it to appear in the learner thread
			// if(debugprints) System.out.println("process " + flop);
			pendingOps.put(flop.id, flop);
			if(flop instanceof Rename) {
				int root1End = flop.path().indexOf('/', 1);
				int otherColor = colorForRoot.get(flop.path().substring(0, root1End));
				if(otherColor == mycolor) {
					int root2End = flop.path2().indexOf('/', 1);
					otherColor = colorForRoot.get(flop.path2().substring(0, root2End));
				}
				if (otherColor != mycolor) {
					if(flop instanceof RenamePart1) client.append(new int[]{mycolor, otherColor}, flop);
					else client.multiple_appends(new int[]{mycolor, otherColor}, flop);
					// client.append(new int[]{mycolor, otherColor}, flop);
				}
				else client.append(mycolor, flop);
			} else client.append(mycolor, flop);
			passivelist.add(WakeOp.Instance);
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
			if(pendingOps.isEmpty()) {
				pop = passivelist.take();
			} else {
				pop = passivelist.poll();
			}
			if(pendingOps.isEmpty() && pop == MarkerOp.Instance) {
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
			// if(objectsFound != 0) System.out.println("fetch time: " + fetchTime + "ns " + objectsFound + " events " + hz + "hz");

			handle_passive: for(; pop != MarkerOp.Instance; pop = passivelist.poll()) {
				if (pop == null || pop == WakeOp.Instance) continue handle_passive;
				if (pop == MarkerOp.Instance) break handle_passive;
				//TODO we don't need passive ops to be totally ordered
				fromDe.offer(pop);
			}
			// if(debugprints && objectsFound != 0) System.out.println("finished sync: " + numentries);
			// if(debugprints) System.out.println("finished sync: " + passivelist.size() + ", " + pendingOps.size());
		}
	}

	public void schedulecallback(FLZKOp cop, KeeperException ke, Object ret)
	{
		callbacklist.offer(new Pair<Pair<FLZKOp, KeeperException>, Object>(new Pair<FLZKOp, KeeperException>(cop, ke), ret));
	}

	public Object apply(FLZKOp cop) throws KeeperException
	{
		if(cop instanceof RenameNack) {
			apply((RenameNack) cop);
			return null;
		}
		else if(cop instanceof RenameNewEmpty) {
			apply((RenameNewEmpty) cop);
			return null;
		}
		else if(cop instanceof RenameOldExists) {
			apply((RenameOldExists) cop);
			return null;
		}

		Node n = map.get(cop.path());
		if(n != null && n.hasPendingRename()) {
			n.pendingOps.addLast(cop);
			return null;
		}

		if(cop instanceof CreateOp) return apply((CreateOp)cop);
		else if(cop instanceof DeleteOp) return apply((DeleteOp)cop);
		else if(cop instanceof ExistsOp) return apply((ExistsOp)cop);
		else if(cop instanceof SetOp) return apply((SetOp)cop);
		else if(cop instanceof GetOp) return apply((GetOp)cop);
		else if(cop instanceof GetChildrenOp) return apply((GetChildrenOp)cop);
		else if(cop instanceof RenamePart1) {
			apply((RenamePart1)cop);
			return null;
		}
		else
		{
			System.out.println(cop.getClass());
			throw new RuntimeException("This should never get called " + cop.getClass().getName());
		}
	}

	public void apply(RenamePart1 rop) throws KeeperException
	{
		String oldPath = rop.oldPath;
		String newPath = rop.newPath;
		{
			File oldFile = new File(oldPath);
			if (oldFile.toPath().startsWith(myRoot)) {
				File F = oldFile;
				Node N = map.get(F);
				if(N == null) {
					send_rename_nack(oldPath, newPath);
					return;
				}
				//TODO
				// if(rop.version!=-1 && N.stat.getVersion()!=rop.version)
				// 	return send_rename_nack(oldPath, newPath);

				if(N.children.size()>0) {
					send_rename_nack(oldPath, newPath);
					return;
				}

				send_rename_old_exists(oldPath, newPath, N.data, null, CreateMode.PERSISTENT);
				N.pendingRename = new PendingRename(rop);
			}
		}
		{
			File newFile = new File(newPath);
			if (newFile.toPath().startsWith(myRoot)){ //the other path must start with my root
				File f = newFile;

				Node oldNode = map.get(f);
				if(oldNode != null) {
					send_rename_nack(oldPath, newPath);
					oldNode.pendingRename = new PendingRename(rop);
					return;
				}

				Node newnode = new Node(null, newPath);
				newnode.pendingRename = new PendingRename(rop);

				Node N = map.get(f.getParentFile());
				if(N == null) {
					send_rename_nack(oldPath, newPath);
					return;
				}

				if(N.isEphemeral()) {
					send_rename_nack(oldPath, newPath);
					return;
				}
				send_rename_new_empty(oldPath, newPath);
				map.put(f, newnode);
			}
		}
		return;
	}

	public void send_rename_nack(String oldPath, String newPath) {
		// throw new RuntimeException("NACK " + oldPath + " " + newPath);
		RenameNack nack = new RenameNack(oldPath, newPath);
		processAsync(nack, true);
	}

	public void send_rename_old_exists(String oldPath, String newPath, byte[] data, List<ACL> acl, CreateMode cm) {
		RenameOldExists roe = new RenameOldExists(oldPath, newPath, data, acl, cm);
		processAsync(roe, true);
	}

	public void send_rename_new_empty(String oldPath, String newPath) {
		RenameNewEmpty rne = new RenameNewEmpty(oldPath, newPath);
		processAsync(rne, true);
	}

	public void apply(RenameNewEmpty rop) throws KeeperException {
		//FIXME handle aborts
		{
			String oldPath = rop.oldPath;
			File oldFile = new File(oldPath);
			//TODO just use tring prefix?
			if (oldFile.toPath().startsWith(myRoot)) {
				Node n = map.get(oldFile);
				if(n == null) return;
				if(!n.pendingRename.part1.newPath.equals(rop.newPath)) {
					n.pendingOps.addLast(rop);
					return;
				}
				n.pendingRename.newEmpty = rop;
				if(n.pendingRename.nack) abortRename(n);
				if(n.pendingRename.oldExists != null) finishRenameRemove(oldFile, n);
			}
		}
		{
			String newPath = rop.newPath;
			File newFile = new File(newPath);
			if (newFile.toPath().startsWith(myRoot)) {
				Node n = map.get(newFile);
				if(n == null) return;
				if(!n.pendingRename.part1.oldPath.equals(rop.oldPath)) {
					n.pendingOps.addLast(rop);
					return;
				}
				n.pendingRename.newEmpty = rop;
				if(n.pendingRename.nack) abortRename(n);
				if(n.pendingRename.oldExists != null) {
					byte[] data = n.pendingRename.oldExists.data;
					finishRenameCreate(newFile, n, data);
				}
			}
		}
	}

	public void apply(RenameOldExists rop) throws KeeperException {
		//FIXME handle aborts
		{
			String oldPath = rop.oldPath;
			File oldFile = new File(oldPath);
			if (oldFile.toPath().startsWith(myRoot)) {
				Node n = map.get(oldFile);
				if(!n.pendingRename.part1.newPath.equals(rop.newPath)) {
					n.pendingOps.addLast(rop);
					return;
				}
				n.pendingRename.oldExists = rop;
				if(n.pendingRename.nack) abortRename(n);
				if(n.pendingRename.newEmpty != null) finishRenameRemove(oldFile, n);
			}
		}
		{
			String newPath = rop.newPath;
			File newFile = new File(newPath);
			if (newFile.toPath().startsWith(myRoot)) {
				Node n = map.get(newFile);
				if(n == null) return;
				if(!n.pendingRename.part1.oldPath.equals(rop.oldPath)) {
					n.pendingOps.addLast(rop);
					return;
				}
				n.pendingRename.oldExists = rop;
				if(n.pendingRename.nack) {
					map.remove(newFile);
					abortRename(n);
				};
				if(n.pendingRename.newEmpty != null) finishRenameCreate(newFile, n, rop.data);
			}
		}
	}

	public void apply(RenameNack rop) throws KeeperException {
		{
			String oldPath = rop.oldPath;
			File oldFile = new File(oldPath);
			if (oldFile.toPath().startsWith(myRoot)) {
				Node n = map.get(oldFile);
				if(n == null) return;
				if(!n.pendingRename.part1.newPath.equals(rop.newPath)) {
					n.pendingOps.addLast(rop);
					return;
				}
				else if(n.pendingRename.nack) abortRename(n);
				else n.pendingRename.nack = true;
			}
		}
		{
			String newPath = rop.newPath;
			File newFile = new File(newPath);
			if (newFile.toPath().startsWith(myRoot)) {
				Node n = map.get(newFile);
				if(n == null) return;
				if(!n.pendingRename.part1.oldPath.equals(rop.oldPath)) {
					n.pendingOps.addLast(rop);
					return;
				}
				else if(n.pendingRename.nack) abortRename(n);
				else n.pendingRename.nack = true;
			}
		}
	}

	public final void abortRename(Node n) throws KeeperException {
		flushPendingOps(n);
	}

	public void finishRenameRemove(File f, Node n) throws KeeperException {
		Node parent = map.get(f.getParentFile());
		parent.children.remove(n);
		triggerwatches(parent.childrenwatches, new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, KeeperState.SyncConnected, parent.path));
		triggerwatches(n.datawatches, new WatchedEvent(Watcher.Event.EventType.NodeDeleted, KeeperState.SyncConnected, n.path));
		map.remove(f);
		flushPendingOps(n);
	}

	public void finishRenameCreate(File f, Node newnode, byte[] data) throws KeeperException {
		//TODO
		// if(newnode.cm.isSequential())
		// {
		// 	AtomicInteger I = newnode.sequentialcounters.get(f);
		// 	if(I==null)
		// 	{
		// 		I = new AtomicInteger(0);
		// 		newnode.sequentialcounters.put(f,  I);
		// 	}
		// 	int x = I.getAndIncrement();
		// 	String y = String.format("%010d", x);
		// 	String path = f.getPath() + y;
		// 	f = new File(path);
		// }

		newnode.data = data;
		newnode.stat.setCzxid(numentries); //copy stat either here or on read
		//TODO: is it okay to use numentries here? will this be problematic if different clients see nodes in different orders?
		Node N = map.get(f.getParentFile());
		N.children.add(newnode);
		triggerwatches(existswatches.get(newnode.path), new WatchedEvent(Watcher.Event.EventType.NodeCreated, KeeperState.SyncConnected,newnode.path));
		triggerwatches(N.childrenwatches, new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, KeeperState.SyncConnected, N.path));
		map.put(f, newnode);
		flushPendingOps(newnode);
	}

	public void flushPendingOps(Node n) throws KeeperException {
		n.pendingRename = null;
		while(!n.pendingOps.isEmpty()) {
			FLZKOp op = n.pendingOps.removeFirst();
			apply(op);
		}

	}

	public Object apply(CreateOp cop) throws KeeperException
	{
		String path = cop.path;
		File f = new File(path);
		if(map.containsKey(f))
			throw new KeeperException.NodeExistsException();

		Node N = map.get(f.getParentFile());
		if(N == null)
			throw new KeeperException.NoNodeException();

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

	public void rename(String oldPath, String newPath)
	{
		RenamePart1 rp1 = new RenamePart1(oldPath, newPath, null, null);
		processAsync(rp1, true);
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

final class PendingRename {
	public final RenamePart1 part1;
	public RenameOldExists oldExists;
	public RenameNewEmpty newEmpty;
	public boolean nack;

	public PendingRename(RenamePart1 part1) {
		this.part1 = part1;
		nack = false;
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
	PendingRename pendingRename;
	ArrayDeque<FLZKOp> pendingOps;

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
		pendingRename = null;
		pendingOps = new ArrayDeque<>();
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

	public boolean hasPendingRename() {
		return this.pendingRename != null;
	}
}
