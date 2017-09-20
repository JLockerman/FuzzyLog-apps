import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.AsyncCallback;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import fuzzy_log.ProxyHandle;

abstract class FLZKOp implements Serializable, ProxyHandle.Data
{

	public static final FLZKOp INSTANCE = new NoOp(0, (byte)0);

	private static final class NoOp extends FLZKOp{
		NoOp(int id, byte kind) { super(id, kind); }
		public void callback(KeeperException k, Object O) {}
		public boolean hasCallback() { return false; }
		public final byte kind() { return 0; }
	}

	static AtomicInteger idcounter = new AtomicInteger();
	public final int id;
	public final byte kind;
	public FLZKOp()
	{
		//FIXME distringuish clients
		id = idcounter.getAndIncrement();
		kind = 0;
		//System.out.println(this + "::" + this.id);
	}

	public FLZKOp(int id, byte kind)
	{
		//FIXME distringuish clients
		this.id = id;
		this.kind = kind;
		//System.out.println(this + "::" + this.id);
	}

	public boolean equals(FLZKOp cop)
	{
		return cop.id == this.id;
	}
	public void callback(KeeperException ke)
	{
		callback(ke, null);
	}
	public abstract void callback(KeeperException k, Object O);

	public abstract boolean hasCallback();

	public abstract byte kind();

	@Override
	public void writeData(DataOutputStream out) throws IOException {
		out.write(this.kind());
		out.writeInt(id);
	}

	@Override
	public FLZKOp readData(DataInputStream in) throws IOException {
		byte kind = in.readByte();
		int id = in.readInt();
		return new NoOp(id, kind);
	}
}
