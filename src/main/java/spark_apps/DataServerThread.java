package spark_apps;
import org.spark_project.dmg.pmml.True;

import java.io.*;
import java.net.*;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

public class DataServerThread extends Thread{
    ObjectInputStream in;
    ObjectOutputStream out;


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


    public DataServerThread(Socket connection) {
        try {

            in = new ObjectInputStream(connection.getInputStream());
            out = new ObjectOutputStream(connection.getOutputStream());


        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    public void run() {

        System.out.println("Connection established!");
        //ObjectInputStream in = new ObjectInputStream(connection.getInputStream());
//        ObjectOutputStream out = new ObjectOutputStream(connection.getOutputStream());

        try {
            final File folder = new File("/media/spiros/Data/SparkDataset/");
            listFilesForFolder(folder);

            for (String filename : filenames) {
                Scanner scanner = new Scanner(new File("/media/spiros/Data/SparkDataset/" + filename));
                System.out.print("Printing lines...");
                while (scanner.hasNext()) {
                    String line = scanner.next();


                    out.writeChars(line);
                    out.flush();



                }
                scanner.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();

        } catch (IOException ioException) {
            ioException.printStackTrace();
        }
    }
}
