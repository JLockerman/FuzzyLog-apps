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

abstract class FLZKOp implements Serializable
{
	static AtomicInteger idcounter = new AtomicInteger();
	int id;
	public FLZKOp()
	{
		//FIXME distringuish clients
		id = idcounter.getAndIncrement();
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

	/*
	public ByteBuffer writeBytes(ByteBuffer out) {
		return out.putInt(this.id);
	}

	public int readId(ByteBuffer in) {
		return in.getInt();
	}*/
}
