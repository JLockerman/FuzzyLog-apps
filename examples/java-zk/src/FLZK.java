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


class Pair<A,B>
{
	A first;
	B second;
	Pair(A first, B second)
	{
		this.first = first;
		this.second = second;
	}
}

class SetOp extends FLZKOp implements Serializable
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

}


class CreateOp extends FLZKOp implements Serializable
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
}

class GetChildrenOp extends FLZKOp
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
		
}

class GetOp extends FLZKOp
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
	
}


class ExistsOp extends FLZKOp
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
	
}

class DeleteOp extends FLZKOp implements Serializable
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

}



public class FLZK implements IZooKeeper, Runnable
{
	protected ProxyClient appendclient;
	protected ProxyClient playbackclient;
	HashMap<File, Node> map;
	
	HashMap<Object, FLZKOp> pendinglist;  //mutating operations
	LinkedList<FLZKOp> passivelist; //non-mutating operations
	LinkedList<Object> callbacklist;
	Watcher defaultwatcher;
	Map<String, Set<Watcher>> existswatches;
	
	long numentries=0;
	
	int mycolor = 0;
	
	boolean debugprints;
	
	public FLZK(ProxyClient tclient, ProxyClient tclient2, int tmycolor, Watcher W) throws Exception, KeeperException
	{
		defaultwatcher = W;
		pendinglist = new HashMap<Object, FLZKOp>();
		passivelist = new LinkedList<FLZKOp>();
		callbacklist = new LinkedList<Object>();
		debugprints = false;
		map = new HashMap<File, Node>();
		map.put(new File("/"), new Node("foobar".getBytes(), "/"));		
		existswatches = new HashMap<String, Set<Watcher>>();
		appendclient = tclient;
		playbackclient = tclient2;
		mycolor = tmycolor;
		
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
					synchronized(callbacklist)
					{
						if(callbacklist.size()>0)
						{
							O = callbacklist.removeFirst();
						}
						else
						{
							try 
							{
								callbacklist.wait();
							} 
							catch (InterruptedException e) 
							{
								//do nothing, keep going
							}
						}
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
	}
	
	public synchronized String toString()
	{
		String x = map.toString();
		return x;
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
					appendclient.async_append(colors, ObjectToBytes(flop), new WriteID());
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
	
	@Override
	public void run() 
	{
		while(true)
		{
			try
			{
				synchronized(this)
				{
					sync();
					//wait for a signal to sync
					this.wait();
					if(debugprints) System.out.println(passivelist.size() + ":" + pendinglist.size());
				}
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
	
	
	public synchronized void sync() throws IOException
	{
		try
		{
			boolean tailreached = false;
			
			playbackclient.snapshot();
			
			while(true)
			{
				FLZKOp mycop = null;
				Object ret = null;
				FLZKOp zop = null;
				byte[] b = new byte[4096]; //hardcoded
				int[] colors = new int[1]; //hardcoded
				int[] results = new int[2]; //hardcoded
				
				if(debugprints) System.out.println("sync");
				if(!tailreached)
				{
					tailreached = !playbackclient.get_next(b, colors, results); // what do we do with results?
					//encountered current tail of log -- sync complete
				}
				
				if(!tailreached)
				{
					numentries++; //todo: we're using this instead of curpos
					if(debugprints) System.out.println("Tail not reached");
					ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(b));
					zop = (FLZKOp)ois.readObject();
					//is this a pending operation?
					mycop = pendinglist.remove(zop.id);
					if(debugprints)
					{
						if(mycop!=null) System.out.println("Found pending op!");
						else System.out.println("Didn't find pending op!");
					}
				}
				else if(passivelist.size()==0) break;
				else if(passivelist.size()>0)
				{
					zop = (FLZKOp)passivelist.removeFirst();
					mycop = zop;
				}
				
				try
				{
					if(debugprints) System.out.println("applying op");
					ret = this.apply(zop);
					if(mycop!=null)
						schedulecallback(mycop, null, ret);
						//mycop.callback(null, ret);
				}
				catch(KeeperException ke)
				{
					if(mycop!=null)
						schedulecallback(mycop, ke, ret);
						//mycop.callback(ke, ret);
				}
			}
		}
		catch(ClassNotFoundException c)
		{
			throw new RuntimeException(c);
		}
		if(debugprints) System.out.println("returning from sync: " + passivelist.size() + ", " + pendinglist.size());
	}

	public void schedulecallback(FLZKOp cop, KeeperException ke, Object ret)
	{
		synchronized(callbacklist)
		{
			callbacklist.add(new Pair<Pair<FLZKOp, KeeperException>, Object>(new Pair<FLZKOp, KeeperException>(cop, ke), ret));
			callbacklist.notify();
		}
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
	
	public synchronized Object apply(CreateOp cop) throws KeeperException
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
	
	public synchronized Object apply(DeleteOp dop) throws KeeperException
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

	
	
	public synchronized Object apply(ExistsOp eop) throws KeeperException
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
	
	public synchronized Object apply(SetOp sop) throws KeeperException
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
	
		public synchronized Object apply(GetOp gop) throws KeeperException
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
	
	public synchronized Object apply(GetChildrenOp gcop) throws KeeperException
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
		synchronized(callbacklist)
		{
			callbacklist.add(new Pair<Watcher, WatchedEvent>(W, WE));
			callbacklist.notify();
		}
	}


	
	byte[] ObjectToBytes(FLZKOp O)
	{
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
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

class Node
{
	Stat stat;
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
