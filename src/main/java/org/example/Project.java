package org.example;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Scanner;
import java.util.stream.Collectors;

public class Project {
    //attributes
    private String projectName;
    private URL url;
    private ArrayList<Station> projectStations;
    private FileWriter file;
    private String location;

    //constructor
    public Project(String projectName, String location) throws MalformedURLException {
        this.projectName = projectName;
        this.url = new URL("https://airqino-api.magentalab.it/getStations/"+projectName);
        this.projectStations= new ArrayList<>();
        this.location = location;
    }

    //getter
    public ArrayList<Station> getProjectStations() {
        return projectStations;
    }
    public String getProjectName() {
        return projectName;
    }
    public String getLocation() {
        return location;
    }

    //metodo che legge i dati dei sensori relativi ad un progetto facendo una request http e li scrive in un file.txt
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

    //metodo che legge i nomi dei sensori, la loro latitudine e longitudine da file e li inserisce in un lista
    public void getStationsNames() throws FileNotFoundException {
        File readebleFile= new File("sensors"+projectName+".txt");
        Scanner myReader = new Scanner(readebleFile);
        int count= 0;
        String name="";
        String latitude="";
        String longitude="";
        while (myReader.hasNextLine()) {
            String data= myReader.nextLine();
            count++;
            if(count%7 == 5){
                String[] line= data.split(" ");
                String[] word= line[9].split("");
                name="";
                for(int i= 1; i<word.length-2; i++)
                    name += word[i];
            }
            if(count%7 == 6){
                String[] line= data.split(" ");
                String[] word= line[9].split("");
                latitude="";
                for(int i= 0; i<word.length-1; i++)
                    latitude += word[i];
            }
            if(count%7 == 0){
                String[] line= data.split(" ");
                String[] word= line[9].split("");
                longitude="";
                for(int i= 0; i<word.length; i++)
                    longitude += word[i];
            }
            if(count%7 == 1){
                if(!latitude.equals("") && !longitude.equals(""))
                    projectStations.add(new Station(name,Float.parseFloat(latitude), Float.parseFloat(longitude)));
            }
        }
        myReader.close();

    }
    //aggiorna la lista dei progetti, rileggento i dati dei sensori e rimettendo i nomi nella lista
    public void updateProjects(DBConnector dbConnector) throws IOException {
        projectStations.clear();
        readListOfSensors();
        getStationsNames();
        dbConnector.insertProject(this);
    }

    //controlla se l'elemento passato in input fa parte della lista dei progetti
    public boolean contains(Object o) {
        for(Station station: projectStations){
            if(Objects.equals(station.getName(), String.valueOf(o))){
                return true;
            }
        }
        return false;
    }


}
