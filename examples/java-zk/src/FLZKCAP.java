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
	List<FLZKOp> buffer;
	HashMap<File, Node> mapclone;
	
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
		try
		{
			fork();
		}
		catch(KeeperException e)
		{
			throw new RuntimeException(e);
		}
	}
	
	public void reconnect()
	{
		try
		{
			join();
		}
		catch(KeeperException e)
		{
			throw new RuntimeException(e);
		}		
	}
	
	public void fork() throws KeeperException
	{
		Object[] results = new Object[1];
		this.fork(new AsyncToSync(), results);
		waitForResults(results);	
	}


	public void fork(AsyncCallback.VoidCallback cb, Object ctxt)
	{
		try
		{
			synchronized(this)
			{
				ForkOp fop = new ForkOp(cb, ctxt);
				pendinglist.put(fop.id, fop);
				appendclient.async_append_causal(secondarycolor, primarycolor, ObjectToBytes(fop), new WriteID());
				appendclient.wait_any_append(new WriteID());
				this.notify(); //wake up sync thread to process entries
			}
		}
		catch(IOException E)
		{
			throw new RuntimeException(E);
		}
	}
	
	
	public void join() throws KeeperException
	{
		Object[] results = new Object[1];
		this.join(new AsyncToSync(), results);
		waitForResults(results);	
	}
	
	public void join(AsyncCallback.VoidCallback cb, Object ctxt)
	{
		try
		{
			synchronized(this)
			{
				//for this to be accurate, we may have to wait until all secondary appends
				//have completed;
				//replacing all wait_any_append with waiting for a specific append 
				//might solve that, or we might need more complex logic.
				JoinOp jop = new JoinOp(cb, ctxt);
				pendinglist.put(jop.id, jop);
				appendclient.async_append_causal(primarycolor, secondarycolor, ObjectToBytes(jop), new WriteID());
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
				WrapperOp wop = new WrapperOp(flop, mycolor);
				System.out.println("putting " + wop.id + " wrapping " + flop.id + " in pending list");
				pendinglist.put(wop.id, flop);
				int[] colors = new int[1]; colors[0] = mycolor;
				try
				{
					if(!netpart)
					{
						appendclient.async_append(primarycolor, ObjectToBytes(wop), new WriteID());
						appendclient.wait_any_append(new WriteID());											
					}
					else
					{
						appendclient.async_append(secondarycolor, ObjectToBytes(wop), new WriteID());
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
		System.out.println("Playing op!" + cop);	
		//inefficient -- fix later
		if(cop instanceof JoinOp) return apply((JoinOp)cop);
		else if(cop instanceof ForkOp) return apply((ForkOp)cop);
		else if(cop instanceof WrapperOp)
		{
			if(netpart)
			{
				buffer.add((WrapperOp)cop);
			}
			return apply(((WrapperOp)cop).innerop);
		}
		else
		{
			System.out.println("super apply op!" + cop);		
			Object R = super.apply(cop);
			System.out.println("Played op!" + cop);		
			return R;
		}		
	}
	
	public Object apply(ForkOp forkop)
	{
		System.out.println("Played fork op!");
		netpart = true;
		mycolor = secondarycolor[0];
		System.out.println("Switching to color " + secondarycolor[0]);
		mapclone = (HashMap<File, Node>)deepclone(map);
		buffer = new LinkedList<FLZKOp>();
		System.out.println("Done playing fork op!");		
		return null;
	}
	
	public Object apply(JoinOp joinop)
	{
		System.out.println("Played join op!");
		netpart = false;
		mycolor = primarycolor[0];
		map = mapclone;
		//apply primary ops first and secondary ops next
		for(int i=0;i<2;i++)
		{
			Iterator it = buffer.iterator();		
			while(it.hasNext())
			{
				WrapperOp wop = (WrapperOp)it.next();
				try
				{
					if(wop.color==primarycolor[0] && i==0)
						apply(wop);
					else if(wop.color==secondarycolor[0] && i==1)
						apply(wop);
				}
				catch(KeeperException e)
				{
					//ignore and continue
					System.out.println("keeper exception...");
				}
			}
		}
		return null;
	}
	
	public Object deepclone(Object O)
	{
		try 
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(O);
			ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			ObjectInputStream ois = new ObjectInputStream(bais);
			return ois.readObject();
		}
		catch (IOException e) 
		{
			throw new RuntimeException(e);
		} 
		catch (ClassNotFoundException e) 
		{
			throw new RuntimeException(e);
		}
	}

	
}

class ForkOp extends FLZKOp implements Serializable
{
	AsyncCallback.VoidCallback cb;
	Object ctxt;
	public ForkOp(AsyncCallback.VoidCallback cb, Object ctxt)
	{
		this.cb = cb;
		this.ctxt = ctxt;
	}
	
	public void callback(KeeperException ke, Object O)
	{
		if(ke!=null)
			cb.processResult(ke.code().intValue(), null, ctxt);
		else
			cb.processResult(Code.OK.intValue(), null, ctxt);	
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
	AsyncCallback.VoidCallback cb;
	Object ctxt;

	public JoinOp(AsyncCallback.VoidCallback cb, Object ctxt)
	{
		this.cb = cb;
		this.ctxt = ctxt;
	}
	
	public void callback(KeeperException ke, Object O)
	{
		if(ke!=null)
			cb.processResult(ke.code().intValue(), null, ctxt);
		else
			cb.processResult(Code.OK.intValue(), null, ctxt);		
	}
	private void writeObject(java.io.ObjectOutputStream out) throws IOException
	{
	}
	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
	{
	}
}

class WrapperOp extends FLZKOp implements Serializable
{
	FLZKOp innerop;
	int color;
	public WrapperOp(FLZKOp flzkop, int tcolor)
	{
		innerop = flzkop;
		color = tcolor;
	}
	public void callback(KeeperException ke, Object O)
	{
	}
	
}