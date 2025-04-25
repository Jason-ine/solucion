package main.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConexionBD {
    private static final String URL_ORIGEN = "jdbc:sqlserver://ipcprod.database.windows.net;databaseName=db-indices;encrypt=true;trustServerCertificate=true";
    private static final String URL_DESTINO = "jdbc:sqlserver://10.0.0.19;databaseName=TablerosIPC;encrypt=true;trustServerCertificate=true";
    private static final String USUARIOORIGEN = "ipcreader";
    private static final String CONTRASENAORIGEN = "inzp5Y30xXwsOov";
    private static final String USUARIODESTINO = "ipcwork";
    private static final String CONTRASENADESTINO = "1pcWR1t3R/*2025";
    private static final int MAX_REINTENTOS = 5;
    private static final int TIEMPO_ESPERA = 5000; 

    public static Connection obtenerConexionOrigen() throws SQLException {
        return obtenerConexionConReintentos(URL_ORIGEN, USUARIOORIGEN, CONTRASENAORIGEN);
    }

    public static Connection obtenerConexionDestino() throws SQLException {
        return obtenerConexionConReintentos(URL_DESTINO, USUARIODESTINO, CONTRASENADESTINO);
    }

    private static Connection obtenerConexionConReintentos(String url, String user, String pass) 
            throws SQLException {
        SQLException lastError = null;
        
        for (int i = 0; i < MAX_REINTENTOS; i++) {
            try {
                return DriverManager.getConnection(url, user, pass);
            } catch (SQLException e) {
                lastError = e;
                if (i < MAX_REINTENTOS - 1) {
                    try {
                        Thread.sleep(TIEMPO_ESPERA);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new SQLException("Conexión interrumpida", ie);
                    }
                }
            }
        }
        throw lastError;
    }
}