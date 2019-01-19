package spark_apps;
import java.io.*;
import java.net.*;

public class ServerThread extends Thread{
    ObjectInputStream in;
    ObjectOutputStream out;


    public ServerThread(Socket connection) {
        try {
            in = new ObjectInputStream(connection.getInputStream());
            out = new ObjectOutputStream(connection.getOutputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        try {
            int a = in.readInt();
            int b = in.readInt();
            int total = a+b;

            out.writeInt(total);
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                in.close();
                out.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }
}
