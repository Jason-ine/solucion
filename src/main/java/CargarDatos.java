package main.java;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CargarDatos {
    private static final int MAX_BLOCK_SIZE = 7500; // Ajustado exactamente al límite de tu SP
    private static final int RECORDS_PER_BLOCK = 30; // Más conservador para tu estructura de datos
    private static final int YEAR = 2023;
    private static final int MONTH = 10;

    public static void main(String[] args) {
        // Configuración de conexiones
        String urlOrigen = "jdbc:sqlserver://0057A31D:1433;databaseName=prod;encrypt=true;trustServerCertificate=true";
        String urlDestino = "jdbc:sqlserver://0057A31D:1433;databaseName=prod;encrypt=true;trustServerCertificate=true";
        String usuario = "sa";
        String contraseña = "Abc$2020";

        try (Connection conexionOrigen = DriverManager.getConnection(urlOrigen, usuario, contraseña);
             Connection conexionDestino = DriverManager.getConnection(urlDestino, usuario, contraseña)) {

            // 1. Limpieza inicial
            limpiarDatosExistentes(conexionDestino, YEAR, MONTH);
            
            // 2. Obtención de datos
            List<String> datos = obtenerDatos(conexionOrigen);
            
            // 3. Preparación de bloques
            List<String> bloques = prepararBloquesDatos(datos);
            
            // 4. Ejecución por bloques
            ejecutarBloques(conexionDestino, bloques, YEAR, MONTH);

            System.out.println("Proceso completado exitosamente. Bloques procesados: " + bloques.size());

        } catch (SQLException e) {
            System.err.println("Error crítico en el proceso:");
            e.printStackTrace();
        }
    }

    private static void limpiarDatosExistentes(Connection conexion, int anio, int mes) throws SQLException {
        String sql = "{call dbo.sp_tran_SIP(?, ?, ?, ?)}";
        try (CallableStatement cstmt = conexion.prepareCall(sql)) {
            cstmt.setString(1, "LIMPIAR_IPC_INDICES_PONDERACIONES_COTIZACIONES");
            cstmt.setInt(2, anio);
            cstmt.setInt(3, mes);
            cstmt.setString(4, "");
            cstmt.execute();
            System.out.println("Limpieza de datos existentes completada.");
        }
    }

    private static List<String> obtenerDatos(Connection conexion) throws SQLException {
        List<String> datos = new ArrayList<>();
        String sql = "{call dbo.sp_NEW_get_indice_ponderaciones_cotizaciones(?, ?)}";
     
        try (CallableStatement cstmt = conexion.prepareCall(sql)) {
            cstmt.setInt(1, YEAR);
            cstmt.setInt(2, MONTH);
            
            System.out.println("Recuperando datos desde stored procedure...");
            try (ResultSet rs = cstmt.executeQuery()) {
                int contador = 0;
                while (rs.next()) {
                    datos.add(formatearFila(rs));
                    contador++;
                    
                    if (contador % 1000 == 0) {
                        System.out.println("Registros leídos: " + contador);
                    }
                }
                System.out.println("Total registros obtenidos: " + contador);
            }
        }
        return datos;
    }

    private static String formatearFila(ResultSet rs) throws SQLException {
        return String.format(
            "(%d, '%s', %.18f, %d, '%s', '%s', %.18f, %d, %d, %d, %s, %.18f, %.18f, %d, %d, %d, %d, %d, '%s', %d, %d, '%s')",
            rs.getInt("region_id"),
            escapeSQL(rs.getString("tipo_grupo")),
            rs.getBigDecimal("ponderacion_republica"),
            rs.getInt("grupo_codigo"),
            escapeSQL(rs.getString("grupo_nombre")),
            escapeSQL(rs.getString("fuente_info")),
            rs.getBigDecimal("ponderacion_region"),
            rs.getInt("good_group_region_id"),
            rs.getInt("good_group_id"),
            rs.getInt("orden"),
            (rs.getObject("grupo_padre") == null ? "NULL" : rs.getInt("grupo_padre")),
            rs.getBigDecimal("indice_grupo"),
            rs.getBigDecimal("indice_anterior"),
            rs.getInt("variedad_id"),
            rs.getInt("numero_cotizaciones"),
            rs.getInt("numero_pe"),
            rs.getInt("cotizaciones_realizadas"),
            rs.getInt("calculo_ipc"),
            escapeSQL(rs.getString("estado")),
            rs.getInt("anio"),
            rs.getInt("mes"),
            escapeSQL(rs.getString("nombre_mes"))
        );
    }
    
    private static String escapeSQL(String valor) {
        if (valor == null) return "";
        return valor.replace("'", "''")
                   .replace("\n", " ")
                   .replace("\r", " ")
                   .replace("\\", "\\\\")
                   .trim();
    }

    private static List<String> prepararBloquesDatos(List<String> datos) {
        List<String> bloques = new ArrayList<>();
        StringBuilder bloqueActual = new StringBuilder();
        int registrosEnBloque = 0;

        for (String fila : datos) {
            if (debeCrearNuevoBloque(bloqueActual, fila, registrosEnBloque)) {
                agregarBloque(bloques, bloqueActual);
                bloqueActual = new StringBuilder();
                registrosEnBloque = 0;
            }

            if (bloqueActual.length() > 0) {
                bloqueActual.append(",");
            }
            bloqueActual.append(fila);
            registrosEnBloque++;
        }

        agregarBloque(bloques, bloqueActual);
        System.out.println("Total bloques generados: " + bloques.size());
        return bloques;
    }

    private static boolean debeCrearNuevoBloque(StringBuilder bloqueActual, String nuevaFila, int registrosEnBloque) {
        return bloqueActual.length() + nuevaFila.length() + 1 > MAX_BLOCK_SIZE || 
               registrosEnBloque >= RECORDS_PER_BLOCK;
    }

    private static void agregarBloque(List<String> bloques, StringBuilder bloque) {
        if (bloque.length() > 0) {
            bloques.add(bloque.toString());
        }
    }

    private static void ejecutarBloques(Connection conexion, List<String> bloques, int anio, int mes) throws SQLException {
        int totalBloques = bloques.size();
        int exitosos = 0;
        
        for (int i = 0; i < bloques.size(); i++) {
            String bloque = bloques.get(i);
            if (ejecutarBloque(conexion, bloque, anio, mes, i+1, totalBloques)) {
                exitosos++;
            }
            
            // Pequeña pausa cada 50 bloques para evitar sobrecarga
            if ((i+1) % 50 == 0) {
                try { Thread.sleep(500); } catch (InterruptedException e) {}
            }
        }
        
        System.out.println("Resumen: " + exitosos + "/" + totalBloques + " bloques procesados exitosamente");
    }

    private static boolean ejecutarBloque(Connection conexion, String bloque, int anio, int mes, 
                                         int bloqueActual, int totalBloques) {
        String sql = "{call dbo.sp_tran_SIP(?, ?, ?, ?)}";
        try (CallableStatement cstmt = conexion.prepareCall(sql)) {
            cstmt.setString(1, "ADD_IPC_INDICES_PONDERACIONES_COTIZACIONES");
            cstmt.setInt(2, anio);
            cstmt.setInt(3, mes);
            cstmt.setString(4, bloque);
            cstmt.setQueryTimeout(120); // 2 minutos de timeout
            
            System.out.printf("[Bloque %d/%d] Ejecutando (size: %d chars)...%n",
                bloqueActual, totalBloques, bloque.length());
            
            cstmt.execute();
            return true;
            
        } catch (SQLException e) {
            System.err.printf("Error en bloque %d/%d (size: %d chars): %s%n",
                bloqueActual, totalBloques, bloque.length(), e.getMessage());
            
            // Log detallado del bloque problemático
            System.err.println("Inicio bloque: " + bloque.substring(0, Math.min(100, bloque.length())));
            System.err.println("Fin bloque: " + bloque.substring(Math.max(0, bloque.length() - 100)));
            return false;
        }
    }
}