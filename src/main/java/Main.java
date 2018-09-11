import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Main
{
	private final String id;
	private final int numProcesses;
	private final int port;

	public static void main(String[] args) { new Main(args); }

	private Main(String[] args)
	{
		if (args.length != 3)
			throw new RuntimeException("Needs id, num processes, port");

		id = args[0];
		numProcesses = Integer.parseInt(args[1]);
		port = Integer.parseInt(args[2]);

		ExecutorService threadPool = Executors.newFixedThreadPool(numProcesses);

		try
		{
			openReceiveSocket();
			openSendSocket();
		}
		catch (IOException e) { System.out.println("Error opening socket: " + e.getMessage()); }
	}
	private void openSendSocket() throws IOException
	{
		Socket socket = new Socket(id, port);
		PrintWriter out = getWriter(socket);
		BufferedReader in = getReader(socket);
	}
	private void openReceiveSocket() throws IOException
	{
		ServerSocket serverSocket = new ServerSocket(port);
		Socket socket = serverSocket.accept();
		PrintWriter out = getWriter(socket);
		BufferedReader in = getReader(socket);
	}

	private PrintWriter getWriter(Socket socket) throws IOException
	{
		return new PrintWriter(socket.getOutputStream(), true);
	}
	private BufferedReader getReader(Socket socket) throws IOException
	{
		return new BufferedReader(new InputStreamReader(socket.getInputStream()));
	}
}
