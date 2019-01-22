package spark_apps;

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
                ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());

                try {
                    final File folder = new File("/media/spiros/Data/SparkDataset/");
                    listFilesForFolder(folder);

                    for (String filename : filenames){
                        Scanner scanner = new Scanner(new File("/media/spiros/Data/SparkDataset/"+filename));
                        System.out.print("Printing lines...");
//                        scanner.useDelimiter(",");
                        while(scanner.hasNext()){
                            String line = scanner.next();
                            line = line.replaceAll(",", "\t");
                            System.out.println(line);
                            System.out.println("\n");


                            out.writeUTF(line);
                            out.writeUTF("\n");
                            out.flush();


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
