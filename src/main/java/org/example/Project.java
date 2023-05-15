package org.example;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Project {
    private String projectName;
    private URL url;
    private ArrayList<String> projectStations;
    private FileWriter file;

    public Project(String projectName) throws MalformedURLException {
        this.projectName = projectName;
        this.url = new URL("https://airqino-api.magentalab.it/getStations/"+projectName);
        projectStations= new ArrayList<>();
    }
    public ArrayList<String> getProjectStations() {
        return projectStations;
    }

    public void readListOfSensors() throws IOException {
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        con.setRequestProperty("Accept", "application/json");
        InputStream inputStream= new BufferedInputStream(con.getInputStream());
        String m = new BufferedReader(new InputStreamReader(inputStream)).lines().collect(Collectors.joining("\n"));
        try {
            file = new FileWriter("sensors"+projectName+".txt");
            file.write(m);
            file.close();
            System.out.println("Successfully wrote to the file.");
        } catch (IOException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
    public void getStationsNames() throws FileNotFoundException {
        projectStations.clear();
        File readebleFile= new File("sensors"+projectName+".txt");
        Scanner myReader = new Scanner(readebleFile);
        int count= 0;
        while (myReader.hasNextLine()) {
            String data= myReader.nextLine();
            count++;
            if(count%7 == 5){
                String[] line= data.split(" ");
                String[] word= line[9].split("");
                String name="";
                for(int i= 1; i<word.length-2; i++)
                name += word[i];
                projectStations.add(name);
            }
        }
        myReader.close();

    }

    public boolean conteins(Object o) {
        for(String station: projectStations){
            if(station == o.toString()){
                return true;
            }
        }
        return false;
    }
}
