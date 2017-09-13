import fuzzy_log.FuzzyLog;
import java.util.Arrays;

public class LogDump
{
	public static void main(String[] args) throws Exception
	{
		if(args.length<1)
		{
			System.out.println("Usage: java LogDump [server addr]");
			System.exit(0);
		}
		FuzzyLog client = new FuzzyLog(new String[] {args[0]});

		int[] acolors = new int[1]; acolors[0] = 1;
		byte[] adata = new byte[10]; adata[0] = 123;
		client.async_append(acolors, adata);
		client.wait_any_append();


		client.snapshot();
		FuzzyLog.Events events = client.get_events();
        for(FuzzyLog.Event event: events) {
            System.out.println(Arrays.toString(event.locations) + "::" + Arrays.toString(event.data));
        }

		System.out.println("hello world!");
	}
}
