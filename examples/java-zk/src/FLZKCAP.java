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


public class FLZKCAP extends FLZK
{
	boolean netpart;
	int primarycolor[] = new int[1];
	int secondarycolor[] = new int[1];
	int bothcolors[] = new int[2];
	
	public FLZKCAP(ProxyClient tclient, ProxyClient tclient2, int tprimarycolor, Watcher W, int tsecondarycolor) throws Exception, KeeperException
	{
		super(tclient, tclient2, tprimarycolor, W);
		primarycolor[0] = tprimarycolor;
		secondarycolor[0] = tsecondarycolor;
		bothcolors[0] = primarycolor[0];
		bothcolors[1] = secondarycolor[0];

		System.out.println("Creating FLZKCAP instance...");		
	}
	
	public void disconnect()
	{
		netpart = true;
		fork();
	}
	
	public void reconnect()
	{
		netpart = false;
		join();
	}

	public void fork()
	{
		try
		{
			synchronized(this)
			{
				appendclient.async_append_causal(secondarycolor, primarycolor, ObjectToBytes(new ForkOp()), new WriteID());
				appendclient.wait_any_append(new WriteID());
				this.notify(); //wake up sync thread to process entries
			}
		}
		catch(IOException E)
		{
			throw new RuntimeException(E);
		}
	}
	
	
	public void join()
	{
		try
		{
			synchronized(this)
			{
				//for this to be accurate, we may have to wait until all secondary appends
				//have completed;
				//replacing all wait_any_append with waiting for a specific append 
				//might solve that, or we might need more complex logic.
				appendclient.async_append_causal(primarycolor, secondarycolor, ObjectToBytes(new JoinOp()), new WriteID());
				appendclient.wait_any_append(new WriteID());
				this.notify(); //wake up sync thread to process entries
			}
		}
		catch(IOException E)
		{
			throw new RuntimeException(E);
		}
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
				try
				{
					if(!netpart)
					{
						appendclient.async_append(primarycolor, ObjectToBytes(flop), new WriteID());
						appendclient.wait_any_append(new WriteID());											
					}
					else
					{
						appendclient.async_append(secondarycolor, ObjectToBytes(flop), new WriteID());
						appendclient.wait_any_append(new WriteID());
					}
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
	
	public Object apply(FLZKOp cop) throws KeeperException
	{
		//inefficient -- fix later
		if(cop instanceof JoinOp) return apply((JoinOp)cop);
		else if(cop instanceof ForkOp) return apply((ForkOp)cop);
		else return super.apply(cop);
	}
	
	public Object apply(ForkOp forkop)
	{
		System.out.println("Played fork op!");
		return null;
	}
	
	public Object apply(JoinOp joinop)
	{
		System.out.println("Played join op!");
		return null;
	}

	
}

class ForkOp extends FLZKOp implements Serializable
{
	public ForkOp()
	{
	}
	
	public void callback(KeeperException ke, Object O)
	{
	}
	private void writeObject(java.io.ObjectOutputStream out) throws IOException
	{
	}
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
	{
	}
}

class JoinOp extends FLZKOp implements Serializable
{
	public JoinOp()
	{
	}
	
	public void callback(KeeperException ke, Object O)
	{
	}
	private void writeObject(java.io.ObjectOutputStream out) throws IOException
	{
	}
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
	{
	}
}