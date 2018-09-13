import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main
{
	private static final int MIN_PORT = 20000;
	private static final int MAX_PORT = 29999;
	private final int id;
	private final int numProcesses;
	private final int port;
	private final long[] timestampFromServerId;
	private final LinkedList<String>[] messageQForId;
	private final List<String> messages = new ArrayList<>();
	private final ExecutorService threadPool;

	public static void main(String[] args) { new Main(args); }
 
	private Main(String[] args)
	{
		if (args.length != 3)
			throw new RuntimeException("Needs id, num processes, port: " + args.length);

		id = Integer.parseInt(args[0]);
		numProcesses = Integer.parseInt(args[1]);
		port = Integer.parseInt(args[2]);
		timestampFromServerId = new long[numProcesses];
		timestampFromServerId[id] = 0L;
		messageQForId = (LinkedList<String>[]) new LinkedList[numProcesses];

		threadPool = Executors.newFixedThreadPool(numProcesses * 3);
		openSendSockets();
		threadPool.execute(this::openReceiveSocket);
		threadPool.execute(this::openMasterSocket);
		createShutdownListener();
	}
	private void openSendSockets()
	{
		for (int i = 0; i < numProcesses; i++)
		{
			int serverPort = portForId(i);

			LinkedList<String> queue = new LinkedList<>();
			messageQForId[i] = queue;

			if (i == id)
				continue;
			threadPool.execute(() -> {
				while (true)
				{
					try
					{
						Socket socket = new Socket("localhost", serverPort);
						socket.setReuseAddress(true);
						PrintWriter out = getWriter(socket);

						String message = null;
						//send heartbeat + any broadcast
						while (true)
						{
							do
							{
								if (message == null)
									message = "";
								out.println(id + " " + message);
							} while ((message = queue.poll()) != null);
							Thread.sleep(500);
						}
					}
					catch (Exception ignored) {}
				}
			});
		}
	}
	private void openReceiveSocket()
	{
		try
		{
			ServerSocket serverSocket = new ServerSocket(portForId(id));
			serverSocket.setReuseAddress(true);
			for (int i = 0; i < numProcesses; i++)
			{
				threadPool.execute(() -> {
					while (true)
					{
						try
						{
							Socket socket = serverSocket.accept();
							BufferedReader in = getReader(socket);

							//receive & timestamp heartbeat
							String line;
							while ((line = in.readLine()) != null)
							{
								String[] parts = line.split("\\s");
								int id = Integer.parseInt(parts[0]);
								String message = line.substring(parts[0].length() + 1);
								if (!message.isEmpty())
									messages.add(message);
								timestampFromServerId[id] = System.currentTimeMillis();
							}
						}
						catch (IOException ignored) {}
					}
				});
			}
		}
		catch (IOException ignored) {}
	}
	private void openMasterSocket()
	{
		try
		{
			while (true)
			{
				ServerSocket serverSocket = new ServerSocket(port);
				serverSocket.setReuseAddress(true);
				Socket socket = serverSocket.accept();
				PrintWriter out = getWriter(socket);
				BufferedReader in = getReader(socket);

				String line;
				while ((line = in.readLine()) != null)
				{
					//read from server
					switch (line)
					{
						case "get":
							String message = "messages " + String.join(",", messages);
							out.println(message.length() + "-" + message);
							break;
						case "alive":
							long now = System.currentTimeMillis();
							List<String> alives = new ArrayList<>(numProcesses);
							for (int i = 0; i < timestampFromServerId.length; i++)
								if (i == id || now - timestampFromServerId[i] < 1000)
									alives.add(String.valueOf(i));
							Collections.sort(alives);
							message = "alive " + String.join(",", alives);
							out.println(message.length() + "-" + message);
							break;
						default:
							String broadcast = "broadcast ";
							if (!line.startsWith(broadcast))
								break;
							message = line.substring(broadcast.length());
							addMessageToQs(message);
							messages.add(message);
							break;
					}
				}

			}
		}
		catch (IOException ignored) {}
	}

	private void createShutdownListener()
	{
		Runtime.getRuntime().addShutdownHook(new Thread(threadPool::shutdown));
	}
	private PrintWriter getWriter(Socket socket) throws IOException
	{
		return new PrintWriter(socket.getOutputStream(), true);
	}
	private BufferedReader getReader(Socket socket) throws IOException
	{
		return new BufferedReader(new InputStreamReader(socket.getInputStream()));
	}
	private int portForId(int id)
	{
		return MIN_PORT + id;
	}
	private void addMessageToQs(String message)
	{
		for (LinkedList<String> q : messageQForId)
			q.add(message);
	}
}
