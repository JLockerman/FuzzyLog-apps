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


public class FLZKCausal extends FLZK
{	
	int othercolors[];
	public FLZKCausal(ProxyClient tclient, ProxyClient tclient2, int tmycolor, Watcher W, int[] tothercolors) throws Exception, KeeperException
	{
		super(tclient, tclient2, tmycolor, W);
		othercolors = tothercolors;
		System.out.println("Creating FLZKCausal instance...");		
	}
	
	
	void processAsync(FLZKOp flop, boolean mutate)
	{
		//signal to learning thread
		synchronized(this) //sync can't run in parallel; flproxy is not threadsafe
		{
			if(mutate)
			{
				pendinglist.put(flop.id, flop);
				int[] colors = new int[1]; colors[0] = mycolor;
				//client.append(ObjectToBytes(flop));
				try
				{
					appendclient.async_append_causal(colors, othercolors, ObjectToBytes(flop), new WriteID());
					appendclient.wait_any_append(new WriteID());
					//we don't have to wait for the append to finish;
					//we just wait for it to appear in the learner thread					
				}
				catch (Exception E)
				{
					//do what here?
					throw new RuntimeException(E);
				}
			}
			else
			{
				passivelist.add(flop);
			}
			this.notify(); //wake up sync thread to process entries
		}
	}
}
