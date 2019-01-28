package auebdreamteam.com.dssparkclient;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class DataSocketServer {
    ServerSocket providerSocket;
    Socket connection = null;


    List<String> filenames = new LinkedList<String>();
    public void listFilesForFolder(final File folder) {
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                listFilesForFolder(fileEntry);
            } else {
                if(fileEntry.getName().contains(".csv"))
                    filenames.add(fileEntry.getName());
            }
        }
    }


    public void readFiles() {

    }

//    public void sendCSVLine(String line){
//
//        try {
//
//            out.writeUTF(line);
//            out.flush();
//
////            System.out.println("Server>" + String.valueOf(in.readInt()));
//
//        } catch (IOException ioException) {
//            ioException.printStackTrace();
//        }
//    }
    void openServer() {
        try {

            providerSocket = new ServerSocket(4321, 10);

            while (true) {
                System.out.println("Socket data server is running...");
                connection = providerSocket.accept();
//                Thread t = new DataServerThread(connection);
//                t.start();
                System.out.println("Connection established!");
                //ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
                PrintWriter out = new PrintWriter(connection.getOutputStream());

                try {
                    final File folder = new File("/media/spiros/Data/SparkDataset/");
                    listFilesForFolder(folder);

                    for (String filename : filenames){
                        Scanner scanner = new Scanner(new File("/media/spiros/Data/SparkDataset/"+filename));
                        System.out.print("Printing lines...");
//                        scanner.useDelimiter(",");
                        int counter = 0;
                        StringBuilder sb = new StringBuilder();
                        while(scanner.hasNext()){
                            String line = scanner.next();
                            sb.append(line).append("\n");
                            counter++;
                            if (counter == 10) {
                                counter = 0;


                                out.print(sb.toString().replaceAll("[^\\p{ASCII}]", ""));
                                out.flush();
                                System.out.println(sb.toString());
                                sb.setLength(0);
                                //Thread.sleep(5000);
                            }
                        }
                        if (sb.length() > 0) {
                            out.print(sb.toString());
                            out.flush();
                            System.out.println(sb.toString());
                            sb.setLength(0);
                        }
                        scanner.close();
                    }} catch (FileNotFoundException e) {
                    e.printStackTrace();

                }


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
        new DataSocketServer().openServer();

    }
}
