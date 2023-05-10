package org.example;
import java.sql.*;
import java.math.*;

//classe che si connette al database e passa le informazioni

public class dbConnecter {
    private Consumer consumer;
    private Connection conn;

    public dbConnecter(Consumer consumer) throws SQLException {
        this.consumer = consumer;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        }
        catch(ClassNotFoundException ex) {
            System.out.println("Error: unable to load driver class!");
            System.exit(1);
        }
        String URL = "jdbc:postgresql://136.243.101.139:5433/postgres";
        String USER = "postgres";
        String PASS = "***REDACTED***";
        conn = DriverManager.getConnection(URL, USER, PASS);
    }

    public void createTable(){
        PreparedStatement pstmt = null;
        try {
            String query= "CREATE TABLE message_type ('message type' varchar(255) primarykey);";
            pstmt = conn.prepareStatement(query);
        }
        catch (SQLException e) {
            System.out.println("error: SQL doesnt found");
        }


    }
    public void insertMessage(){
        String query= "INSERT INTO messages VALUES ();";
    }
}
