package se.kth.kthfsdashboard.host;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
public class AgentClient {

    private String server;
    private int port;

    AgentClient(String server, int port) {
        this.server = server;
        this.port = port;
    }

    void run(String command) {
        try {
            InetAddress host = InetAddress.getByName(server);
            Socket socket = new Socket(host.getHostName(), port);

            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.println(command);
            String response =  in.readLine();
            
            System.out.println("response: " + response);

        } catch (IOException e) {
        }
    }
}
