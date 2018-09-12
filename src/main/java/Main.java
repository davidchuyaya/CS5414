import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Main
{
	private static final int MIN_PORT = 20000;
	private static final int MAX_PORT = 29999;
	private final String id;
	private final int numProcesses;
	private final int port;
	private int privatePort;
	private final Map<String, Long> timestampFromServerId;
	private final List<String> messages = new ArrayList<>();
	private final ExecutorService threadPool;
	private ServerSocket serverSocket;

	public static void main(String[] args) { new Main(args); }

	private Main(String[] args)
	{
		if (args.length != 3)
			throw new RuntimeException("Needs id, num processes, port");

		id = args[0];
		numProcesses = Integer.parseInt(args[1]);
		port = Integer.parseInt(args[2]);
		timestampFromServerId = new HashMap<>(numProcesses);

		threadPool = Executors.newFixedThreadPool(numProcesses * 3);
		createShutdownListener();
		threadPool.execute(this::openMasterSocket);
		threadPool.execute(this::openReceiveSocket);
		openSendSockets();
	}
	private void openSendSockets()
	{
		for (int i = 0; i < numProcesses; i++)
		{
			int serverPort = i + MIN_PORT;
			if (serverPort == privatePort)
				continue;
			threadPool.execute(() -> {
				while (true)
				{
					try
					{
						Socket socket = new Socket("127.0.0.1", serverPort);
						socket.setReuseAddress(true);
						PrintWriter out = getWriter(socket);

						//send heartbeat
						while (true)
						{
							out.println(id);
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
			serverSocket = nextAvailableSocket();
			privatePort = serverSocket.getLocalPort();
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
							String id;
							while ((id = in.readLine()) != null)
								timestampFromServerId.put(id, System.currentTimeMillis());
						}
						catch (IOException ignored) {
							System.out.println("clients are ded");
						}
					}
				});
			}
		}
		catch (IOException e) { System.out.println("server ded"); }
	}
	private void openMasterSocket()
	{
		try
		{
			serverSocket = new ServerSocket(port);
			serverSocket.setReuseAddress(true);
			while (true)
			{
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
							message = "alive " + Stream.concat(Stream.of(id),
									timestampFromServerId.entrySet().stream()
											.filter(entry -> now - entry.getValue() < 1000)
											.map(Map.Entry::getKey))
									.sorted()
									.collect(Collectors.joining(","));
							out.println(message.length() + "-" + message);
							break;
						default:
							String broadcast = "broadcast ";
							if (!line.startsWith(broadcast))
								throw new IOException("Invalid message from master");
							messages.add(line.substring(broadcast.length()));
							break;
					}
				}

			}
		}
		catch (IOException e) { System.out.println("server master ded"); }
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
	private ServerSocket nextAvailableSocket()
	{
		for (int i = MIN_PORT; i <= MAX_PORT; i++)
		{
			try { return new ServerSocket(i); }
			catch (IOException ignored) {} //port in use
		}
		System.out.println("All ports in use");
		return null;
	}
}
