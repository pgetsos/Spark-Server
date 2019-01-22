package spark_apps;
import java.io.*;
import java.net.*;


public class DataServer {
    ServerSocket providerSocket;
    Socket connection = null;

    void openServer() {
        try {

            providerSocket = new ServerSocket(4321, 10);

            while (true) {

                connection = providerSocket.accept();
                Thread t = new DataServerThread(connection);
                t.start();

            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } finally {
            try {
                providerSocket.close();
            } catch (IOException ioException) {
                ioException.printStackTrace();
            }
        }
    }

    public static void main(String args[]) {
        DataServer dt = new DataServer();
        dt.openServer();
    }
}


