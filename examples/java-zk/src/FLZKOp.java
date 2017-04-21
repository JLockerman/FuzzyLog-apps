import java.io.*;
import java.util.*;
import client.ProxyClient;
import client.WriteID;
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
	Object id;
	public FLZKOp()
	{
		id = new Integer(idcounter.getAndIncrement());
		System.out.println(this + "::" + this.id);
	}
	public boolean equals(FLZKOp cop)
	{
		return cop.id.equals(this.id);
	}
	public void callback(KeeperException ke)
	{
		callback(ke, null);
	}
	public abstract void callback(KeeperException k, Object O);
}
