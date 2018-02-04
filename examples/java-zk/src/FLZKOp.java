import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.AsyncCallback;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import fuzzy_log.ProxyHandle;

abstract class FLZKOp implements Serializable, ProxyHandle.Data
{

	public static final FLZKOp INSTANCE = new NoOp(new Id(0, 0), (byte)0);

	private static final class NoOp extends FLZKOp{
		final byte kind;
		NoOp(Id id, byte kind) { super(id); this.kind = kind; }
		public final void callback(KeeperException k, Object O) {}
		public final boolean hasCallback() { return false; }
		public final byte kind() { return this.kind; }
		public final String path() { return null; }
	}

	public static final class Id {

		static AtomicInteger counter = new AtomicInteger();

		final int client;
		final int count;

		private Id(int client) {
			this.count = counter.getAndIncrement();
			this.client = client;
		}

		private Id(int client, int count) {
			this.count = count;
			this.client = client;
		}

		@Override
		public final boolean equals(Object obj) {
			if(obj instanceof Id) {
				Id other = (Id)obj;
				return this.client == other.client && this.count == other.count;
			} else {
				return false;
			}
		}

		@Override
		public final int hashCode() {
			int hash = 1;
			hash += 31 * hash + client;
			hash += 31 * hash + count;
			return hash;
		}

		@Override
		public String toString() {
			return "Id { client=" + client + "; count=" + count +" }";
		}

		public final void write(DataOutputStream out) throws IOException {
			out.writeInt(this.client);
			out.writeInt(this.count);
		}

		public static final Id read(DataInputStream in) throws IOException {
			int client = in.readInt();
			int count = in.readInt();
			return new Id(client, count);
		}

	}

	public static int client = ThreadLocalRandom.current().nextInt();

	public final Id id;

	public FLZKOp()
	{
		this.id = new Id(FLZKOp.client);
		//System.out.println(this + "::" + this.id);
	}

	public FLZKOp(Id id)
	{
		//FIXME distringuish clients
		this.id = id;
		//System.out.println(this + "::" + this.id);
	}

	public final boolean equals(FLZKOp cop)
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

	public abstract String path();
	public String path2() {
		return null;
	}

	@Override
	public void writeData(DataOutputStream out) throws IOException {
		out.write(this.kind());
		this.id.write(out);
	}

	@Override
	public FLZKOp readData(DataInputStream in) throws IOException {
		byte kind = in.readByte();
		Id id = Id.read(in);
		return new NoOp(id, kind);
	}
}
