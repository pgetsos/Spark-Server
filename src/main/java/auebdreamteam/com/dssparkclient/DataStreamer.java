package auebdreamteam.com.dssparkclient;

import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class DataStreamer {

    Socket requestSocket = null;
    ObjectOutputStream out = null;
    ObjectInputStream in = null;


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
        createStreams();
        try {
            final File folder = new File("/media/spiros/Data/SparkDataset/");
            listFilesForFolder(folder);

            for (String filename : filenames){
                Scanner scanner = new Scanner(new File("/media/spiros/Data/SparkDataset/"+filename));
    //                scanner.useDelimiter(",");
                while(scanner.hasNext()){
                    String line = scanner.next();
                    System.out.print(line);
                    sendCSVLine(line);

                }
                scanner.close();
        }} catch (FileNotFoundException e) {
            e.printStackTrace();

        }
    }

    public void sendCSVLine(String line){

        try {

            out.writeUTF(line);
            out.flush();

//            System.out.println("Server>" + String.valueOf(in.readInt()));

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public void createStreams(){
        try {
            requestSocket = new Socket("127.0.0.1", 4321);

            out = new ObjectOutputStream(requestSocket.getOutputStream());
            in = new ObjectInputStream(requestSocket.getInputStream());
        } catch (UnknownHostException unknownHost) {
            System.err.println("You are trying to connect to an unknown host!");
        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }

    public static void main(String args[]) {
        new DataStreamer().readFiles();
    }

}
