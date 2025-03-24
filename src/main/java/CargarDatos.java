package main.java;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class CargarDatos {

    public static void main(String[] args) {
        // Configuración de conexiones
        String urlOrigen = "jdbc:sqlserver://0057A31D:1433;databaseName=prod;encrypt=true;trustServerCertificate=true";
        String usuarioOrigen = "sa";
        String contraseñaOrigen = "Abc$2020";
        String urlDestino = "jdbc:sqlserver://0057A31D:1433;databaseName=prod;encrypt=true;trustServerCertificate=true";
        String usuarioDestino = "sa";
        String contraseñaDestino = "Abc$2020";

        try (Connection conexionOrigen = DriverManager.getConnection(urlOrigen, usuarioOrigen, contraseñaOrigen);
             Connection conexionDestino = DriverManager.getConnection(urlDestino, usuarioDestino, contraseñaDestino)) {

            List<String> datos = obtenerDatosDesdeStoredProcedure(conexionOrigen);
            String datosFormateados = formatearDatosParaParDatos(datos);
            List<String> bloquesDatos = dividirDatosEnBloques(datosFormateados, 7000);

            for (String bloque : bloquesDatos) {
                ejecutarStoredProcedure(conexionDestino, bloque);
            }

            System.out.println("Datos cargados exitosamente.");

        } catch (SQLException e) {
            System.err.println("Error al cargar datos:");
            e.printStackTrace();
        }
    }

    private static List<String> obtenerDatosDesdeStoredProcedure(Connection conexion) throws SQLException {
        List<String> datos = new ArrayList<>();
        String sql = "{call dbo.sp_NEW_get_indice_ponderaciones_cotizaciones(?, ?)}";
        
        try (CallableStatement cstmt = conexion.prepareCall(sql)) {
            cstmt.setInt(1, 2023);
            cstmt.setInt(2, 10);
            ResultSet rs = cstmt.executeQuery();

            while (rs.next()) {
                String fila = construirFilaFormateada(rs);
                datos.add(fila);
            }
        }
        return datos;
    }

    private static String construirFilaFormateada(ResultSet rs) throws SQLException {
        return String.format(
            "(%d, '%s', %f, %d, '%s', '%s', %f, %d, %d, %d, %s, %f, %f, %d, %d, %d, %d, %d, '%s', %d, %d, '%s')",
            rs.getInt("region_id"),
            escaparComillas(rs.getString("tipo_grupo")),
            rs.getBigDecimal("ponderacion_republica"),
            rs.getInt("grupo_codigo"),
            escaparComillas(rs.getString("grupo_nombre")),
            escaparComillas(rs.getString("fuente_info")),
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
            escaparComillas(rs.getString("estado")),
            rs.getInt("anio"),
            rs.getInt("mes"),
            escaparComillas(rs.getString("nombre_mes"))
        );
    }

    private static String escaparComillas(String valor) {
        if (valor == null) {
            return "";
        }
        // Escapar comillas simples y limpiar caracteres problemáticos
        return valor.replace("'", "''")
                   .replace("\n", " ")
                   .replace("\r", " ")
                   .trim();
    }

    private static String formatearDatosParaParDatos(List<String> datos) {
        return String.join(",", datos);
    }

    private static List<String> dividirDatosEnBloques(String datos, int tamañoMaximo) {
        List<String> bloques = new ArrayList<>();
        int inicio = 0;
        
        while (inicio < datos.length()) {
            int fin = Math.min(inicio + tamañoMaximo, datos.length());
            
            if (fin < datos.length()) {
                // Buscar el último paréntesis de cierre completo
                int ultimoParentesis = datos.lastIndexOf("),", fin);
                if (ultimoParentesis > inicio) {
                    fin = ultimoParentesis + 1;
                }
            }
            
            String bloque = datos.substring(inicio, fin).trim();
            
            // Asegurar que el bloque comience y termine correctamente
            if (!bloque.startsWith("(")) {
                int primerParentesis = bloque.indexOf('(');
                if (primerParentesis > 0) {
                    bloque = bloque.substring(primerParentesis);
                }
            }
            
            if (!bloque.endsWith(")")) {
                int ultimoParentesis = bloque.lastIndexOf(')');
                if (ultimoParentesis > 0) {
                    bloque = bloque.substring(0, ultimoParentesis + 1);
                }
            }
            
            if (!bloque.isEmpty()) {
                bloques.add(bloque);
            }
            
            inicio = fin;
        }
        
        return bloques;
    }

    private static void ejecutarStoredProcedure(Connection conexion, String bloqueDatos) throws SQLException {
        // Validación final del formato
        if (!bloqueDatos.startsWith("(") || !bloqueDatos.endsWith(")")) {
            System.err.println("Bloque mal formado - no se ejecutará: " + bloqueDatos.substring(0, Math.min(100, bloqueDatos.length())));
            return;
        }

        System.out.println("Ejecutando bloque de " + bloqueDatos.length() + " caracteres");
        System.out.println("Inicio: " + bloqueDatos.substring(0, Math.min(50, bloqueDatos.length())));
        System.out.println("Fin: " + bloqueDatos.substring(Math.max(0, bloqueDatos.length() - 50)));

        String sql = "{call dbo.sp_tran_SIP(?, ?, ?, ?)}";
        try (CallableStatement cstmt = conexion.prepareCall(sql)) {
            cstmt.setString(1, "ADD_IPC_INDICES_PONDERACIONES_COTIZACIONES");
            cstmt.setInt(2, 0);
            cstmt.setInt(3, 0);
            cstmt.setString(4, bloqueDatos);
            cstmt.execute();
        }
    }
}