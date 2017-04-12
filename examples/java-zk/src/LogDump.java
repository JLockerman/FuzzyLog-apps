import client.ProxyClient;
import client.WriteID;

public class LogDump
{
	public static void main(String[] args) throws Exception
	{
		if(args.length<1)
		{
			System.out.println("Usage: java LogDump [proxyport]");
			System.exit(0);
		}
		ProxyClient client = new ProxyClient(Integer.parseInt(args[0]));
		
		int[] acolors = new int[1]; acolors[0] = 1;
		byte[] adata = new byte[10]; adata[0] = 123;
		client.async_append(acolors, adata, new WriteID());
		WriteID ack_wid = new WriteID(0,0);
		client.wait_any_append(ack_wid);

		
		client.snapshot();
		//sizes hardcoded for now, ugh
		byte[] data = new byte[4096];
		int[] colors = new int[512];
		int[] results = new int[2];
		while(client.get_next(data, colors, results))
		{
			System.out.println(colors + "::" + data);
		}
		
		System.out.println("hello world!");
	}
}
