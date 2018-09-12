import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class Main
{
	private static final int MIN_PORT = 20000;
	private static final int MAX_PORT = 29999;
	private final String id;
	private final int numProcesses;
	private final int port;
	private int privatePort;
	private final Map<String, Long> timestampFromServerId;
	private final Map<Integer, LinkedList<String>> messageQForServerPort;
	private final List<String> messages = new ArrayList<>();
	private final ExecutorService threadPool;

	public static void main(String[] args) { new Main(args); }

	private Main(String[] args)
	{
		if (args.length != 3)
			throw new RuntimeException("Needs id, num processes, port: " + args.length);

		id = args[0];
		numProcesses = Integer.parseInt(args[1]);
		port = Integer.parseInt(args[2]);
		timestampFromServerId = new HashMap<>(numProcesses);
		timestampFromServerId.put(id, 0L);
		messageQForServerPort = new HashMap<>(numProcesses);

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

			LinkedList<String> queue = new LinkedList<>();
			messageQForServerPort.put(serverPort, queue);
			threadPool.execute(() -> {
				while (true)
				{
					try
					{
						Socket socket = new Socket("localhost", serverPort);
						socket.setReuseAddress(true);
						PrintWriter out = getWriter(socket);

						//send heartbeat + any broadcast
						while (true)
						{
							String message = queue.poll();
							if (message == null)
								message = "";
							out.println(id + " " + message);
							Thread.sleep(200);
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
			ServerSocket serverSocket = nextAvailableSocket();
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
							String line;
							while ((line = in.readLine()) != null)
							{
								String[] parts = line.split("\\s");
								String id = parts[0];
								String message = line.substring(id.length() + 1);
								if (!message.isEmpty())
									messages.add(message);
								timestampFromServerId.put(id, System.currentTimeMillis());
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
							message = "alive " +
									timestampFromServerId.entrySet().stream()
											.filter(entry -> entry.getKey().equals(id) ||
													now - entry.getValue() < 1000)
									.map(Map.Entry::getKey)
									.sorted()
									.collect(Collectors.joining(","));
							out.println(message.length() + "-" + message);
							break;
						default:
							String broadcast = "broadcast ";
							if (!line.startsWith(broadcast))
								throw new IOException("Invalid message from master");
							message = line.substring(broadcast.length());
							addMessageToQs(message-);
//							messages.add(message);
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
	private void addMessageToQs(String message)
	{
		messageQForServerPort.forEach((port, q) -> q.addFirst(message));
	}
}
