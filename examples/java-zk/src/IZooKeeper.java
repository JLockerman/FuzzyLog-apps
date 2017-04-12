import java.util.List;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

public interface IZooKeeper
{
	//Mutating async ops that append to the log
	void create(String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctxt);
	public void delete(String path, int version, AsyncCallback.VoidCallback cb, Object ctx);
	public void setData(String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx); 

	//accessor async operations that do not append to the log
	public void exists(String path, boolean watch, AsyncCallback.StatCallback cb, Object ctx);	
	public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx);
	public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx);
	

	//sync methods
	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException;	
	public Stat exists(String path, boolean watcher) throws KeeperException, InterruptedException;
	public Stat setData(String path, byte[] data, int version) throws KeeperException, InterruptedException;

	public void delete(String path, int version) throws KeeperException;
	public byte[] getData(String path, boolean watcher, Stat stat) throws KeeperException, InterruptedException;
	public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException;
	
}

