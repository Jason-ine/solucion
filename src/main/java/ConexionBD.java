package main.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConexionBD {
    private static final String URL_ORIGEN = "jdbc:sqlserver://0057A31D:1433;databaseName=prod;encrypt=true;trustServerCertificate=true";
    private static final String URL_DESTINO = "jdbc:sqlserver://0057A31D:1433;databaseName=prod;encrypt=true;trustServerCertificate=true";
    private static final String USUARIOORIGEN = "sa";
    private static final String CONTRASENAORIGEN = "Abc$2020";
    private static final String USUARIODESTINO = "sa";
    private static final String CONTRASENADESTINO = "Abc$2020";

    public static Connection obtenerConexionOrigen() throws SQLException {
        return DriverManager.getConnection(URL_ORIGEN, USUARIOORIGEN, CONTRASENAORIGEN);
    }

    public static Connection obtenerConexionDestino() throws SQLException {
        return DriverManager.getConnection(URL_DESTINO, USUARIODESTINO, CONTRASENADESTINO);
    }
}