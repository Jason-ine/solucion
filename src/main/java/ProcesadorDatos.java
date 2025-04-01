package main.java;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class ProcesadorDatos {
    private static final int MAX_BLOCK_SIZE = 7000;
    private static final int RECORDS_PER_BLOCK = 30;

    public static void limpiarIndices(Connection conexion, int anio, int mes) throws SQLException {
        ejecutarSP(conexion, "LIMPIAR_IPC_INDICES_PONDERACIONES_COTIZACIONES", anio, mes, "");
    }
    public static void limpiarCoberturaFuentes(Connection conexion) throws SQLException {
        ejecutarSP(conexion, "LIMPIAR_COBERTURA_FUENTES", 0, 0, "");
    }
    public static void limpiarIPM(Connection conexion) throws SQLException {
        ejecutarSP(conexion, "LIMPIAR_IPM", 0, 0, "");
    }
    public static void limpiarIPMC(Connection conexion) throws SQLException {
        ejecutarSP(conexion, "LIMPIAR_IPMC", 0, 0, "");
    }
    public static void limpiarIPP(Connection conexion) throws SQLException {
        ejecutarSP(conexion, "LIMPIAR_IPP", 0, 0, "");
    }

    public static void limpiarFuentes(Connection conexion) throws SQLException {
        ejecutarSP(conexion, "LIMPIAR_IPC_GET_FUENTES", 0, 0, "");
    }

    public static void limpiarPrecios(Connection conexion, int anio, int mes) throws SQLException {
        ejecutarSP(conexion, "LIMPIAR_IPC_PRECIOS_PROMEDIO", anio, mes, "");
    }

    public static void cargarIndices(Connection conexionOrigen, Connection conexionDestino, int anio, int mes) throws SQLException {
        limpiarIndices(conexionDestino, anio, mes);
        
        List<String> datos = obtenerDatosIndices(conexionOrigen, anio, mes);
        
        List<String> bloques = prepararBloquesDatos(datos);
        
        ejecutarBloques(conexionDestino, bloques, anio, mes);
    }

    public static void cargarFuentes(Connection conexionOrigen, Connection conexionDestino) throws SQLException {
        limpiarFuentes(conexionDestino);
        
        List<FuenteDTO> datosFuentes = obtenerDatosFuentes(conexionOrigen);
        
        insertarFuentes(conexionDestino, datosFuentes);
    }

    private static List<String> obtenerDatosIndices(Connection conexion, int anio, int mes) throws SQLException {
        List<String> datos = new ArrayList<>();
        String sql = "{call dbo.sp_get_indice_ponderaciones_cotizaciones(?, ?)}";
     
        try (CallableStatement cstmt = conexion.prepareCall(sql)) {
            cstmt.setInt(1, anio);
            cstmt.setInt(2, mes);
            
            System.out.println("Recuperando datos desde stored procedure...");
            try (ResultSet rs = cstmt.executeQuery()) {
                int contador = 0;
                String fila;
                while (rs.next()) {
                    fila = formatearFilaIndices(rs);
                    if (fila.length() > 6000) {
                        System.err.println("¡ADVERTENCIA! Registro muy grande (" + fila.length() + " chars): " + fila.substring(0, Math.min(100, fila.length())) + "...");
                    }
                    datos.add(fila);
                    contador++;
                    
                    if (contador % 500 == 0) {
                        System.out.println("Registros leídos: " + contador);
                    }
                }
                System.out.println("Total registros obtenidos: " + contador);
            }
        }
        return datos;
    }

    private static List<FuenteDTO> obtenerDatosFuentes(Connection conexion) throws SQLException {
        List<FuenteDTO> fuentes = new ArrayList<>();
        String sql = "{call dbo.sp_get_fuentes()}";
     
        try (CallableStatement cstmt = conexion.prepareCall(sql);
             ResultSet rs = cstmt.executeQuery()) {
            
            while (rs.next()) {
                FuenteDTO fuente = new FuenteDTO();
                
                fuente.setRegionId(rs.getLong("region_id"));
                fuente.setDepartamento(rs.getString("Departamento"));
                fuente.setMunicipio(rs.getString("Municipio"));
                fuente.setDecada(rs.getString("decada"));
                fuente.setDiaVisita(rs.getString("dia_visita"));
                fuente.setDiaVisitaObligatorio(rs.getString("dia_visita_obligatorio"));
                fuente.setUsuarioCodigo(rs.getLong("usuario_codigo"));
                fuente.setEmail(rs.getString("email"));
                fuente.setUsuarioNombre(rs.getString("usuario_nombre"));
                fuente.setNumArticulos(rs.getInt("num_articulos"));
                fuente.setFuenteCodigo(rs.getLong("fuente_codigo"));
                fuente.setFuenteNombre(rs.getString("fuente_nombre"));
                fuente.setFuenteDireccion(rs.getString("fuente_direccion"));
                fuente.setSector(rs.getString("Sector"));
                fuente.setFuenteTipo(rs.getString("fuente_tipo"));
                fuente.setFuenteArea(rs.getString("fuente_area"));
                fuente.setOrdenEnRuta(rs.getLong("orden_en_ruta"));
                
                fuente.setLatitude(rs.getBigDecimal("latitude"));
                fuente.setLongitude(rs.getBigDecimal("longitude"));
                fuente.setAltitude(rs.getBigDecimal("altitude"));
                
                fuente.setFechaAlta(rs.getTimestamp("fecha_alta"));
                fuente.setAnioAlta(rs.getInt("anio_alta"));
                fuente.setMesAlta(rs.getInt("mes_alta"));
                fuente.setNombreMes(rs.getString("nombre_mes"));
                fuente.setFuenteEstado(rs.getString("fuente_estado"));
                fuente.setGeoreferenciada(rs.getInt("Georeferenciada"));
                
                fuentes.add(fuente);
            }
        }
        return fuentes;
    }

    private static String formatearFilaIndices(ResultSet rs) throws SQLException {
        return String.format(
            "(%d, '%s', %.18f, %d, '%s', '%s', %.18f, %d, %d, %d, %s, %.18f, %.18f, %d, %d, %d, %d, %d, '%s', %d, %d, '%s')",
            rs.getInt("region_id"),
            escapeSQL(rs.getString("tipo_grupo")),
            rs.getBigDecimal("ponderacion_republica"),
            rs.getInt("grupo_codigo"),
            escapeSQL(rs.getString("grupo_nombre")),
            escapeSQL(rs.getString("grupo_info")),
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
    
    private static void insertarFuentes(Connection conexion, List<FuenteDTO> fuentes) throws SQLException {
        String sql = "INSERT INTO SIP_IPC_Get_Fuentes ("
                + "region_id, departamento, municipio, decada, dia_visita, dia_visita_obligatorio, "
                + "usuario_codigo, email, usuario_nombre, num_articulos, fuente_codigo, fuente_nombre, "
                + "fuente_direccion, Sector, fuente_tipo, fuente_area, orden_en_ruta, latitude, longitude, "
                + "altitude, fecha_alta, anio_alta, mes_alta, nombre_mes, fuente_estado, georeferenciada"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = conexion.prepareStatement(sql)) {
            for (FuenteDTO fuente : fuentes) {
                int paramIndex = 1;
                
                pstmt.setLong(paramIndex++, fuente.getRegionId());
                pstmt.setString(paramIndex++, fuente.getDepartamento());
                pstmt.setString(paramIndex++, fuente.getMunicipio());
                pstmt.setString(paramIndex++, fuente.getDecada());
                pstmt.setString(paramIndex++, fuente.getDiaVisita());
                pstmt.setString(paramIndex++, fuente.getDiaVisitaObligatorio());
                pstmt.setLong(paramIndex++, fuente.getUsuarioCodigo());
                pstmt.setString(paramIndex++, fuente.getEmail());
                pstmt.setString(paramIndex++, fuente.getUsuarioNombre());
                pstmt.setInt(paramIndex++, fuente.getNumArticulos());
                pstmt.setLong(paramIndex++, fuente.getFuenteCodigo());
                pstmt.setString(paramIndex++, fuente.getFuenteNombre());
                pstmt.setString(paramIndex++, fuente.getFuenteDireccion());
                pstmt.setString(paramIndex++, fuente.getSector());
                pstmt.setString(paramIndex++, fuente.getFuenteTipo());
                pstmt.setString(paramIndex++, fuente.getFuenteArea());
                pstmt.setLong(paramIndex++, fuente.getOrdenEnRuta());
                
                if (fuente.getLatitude() != null) {
                    pstmt.setBigDecimal(paramIndex++, fuente.getLatitude());
                } else {
                    pstmt.setNull(paramIndex++, Types.DECIMAL);
                }
                
                if (fuente.getLongitude() != null) {
                    pstmt.setBigDecimal(paramIndex++, fuente.getLongitude());
                } else {
                    pstmt.setNull(paramIndex++, Types.DECIMAL);
                }
                
                if (fuente.getAltitude() != null) {
                    pstmt.setBigDecimal(paramIndex++, fuente.getAltitude());
                } else {
                    pstmt.setNull(paramIndex++, Types.DECIMAL);
                }
                
                pstmt.setTimestamp(paramIndex++, fuente.getFechaAlta());
                pstmt.setInt(paramIndex++, fuente.getAnioAlta());
                pstmt.setInt(paramIndex++, fuente.getMesAlta());
                pstmt.setString(paramIndex++, fuente.getNombreMes());
                pstmt.setString(paramIndex++, fuente.getFuenteEstado());
                pstmt.setInt(paramIndex++, fuente.getGeoreferenciada());
                
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
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
        int totalCaracteres = 0;

        for (int i = 0; i < datos.size(); i++) {
            String fila = datos.get(i);
            int longitudFila = fila.length() + (bloqueActual.length() > 0 ? 1 : 0);
            
            if ((totalCaracteres + longitudFila) > MAX_BLOCK_SIZE || registrosEnBloque >= RECORDS_PER_BLOCK) {
                System.out.printf("Creando bloque #%d: %d registros, %d caracteres\n",
                    bloques.size() + 1, registrosEnBloque, bloqueActual.length());
                
                agregarBloque(bloques, bloqueActual);
                bloqueActual = new StringBuilder();
                registrosEnBloque = 0;
                totalCaracteres = 0;
            }

            if (bloqueActual.length() > 0) {
                bloqueActual.append(",");
                totalCaracteres++;
            }
            
            bloqueActual.append(fila);
            registrosEnBloque++;
            totalCaracteres += fila.length();
            
            if (i > 0 && i % 100 == 0) {
                System.out.printf("Procesados %d/%d registros, últimos %d caracteres\n", i, datos.size(), bloqueActual.length());
            }
        }

        if (bloqueActual.length() > 0) {
            System.out.printf("Creando último bloque #%d: %d registros, %d caracteres\n", bloques.size() + 1, registrosEnBloque, bloqueActual.length());
            agregarBloque(bloques, bloqueActual);
        }

        System.out.println("Total bloques generados: " + bloques.size());
        return bloques;
    }

    private static void agregarBloque(List<String> bloques, StringBuilder bloque) {
        if (bloque.length() > 0) {
            bloques.add(bloque.toString());
            if (bloques.size() == 1) {
                System.out.println("Primer bloque (inicio): " + bloque.substring(0, Math.min(100, bloque.length())));
            }
        }
    }

    private static void ejecutarBloques(Connection conexion, List<String> bloques, int anio, int mes) throws SQLException {
        int totalBloques = bloques.size();
        int exitosos = 0;
        
        for (int i = 0; i < bloques.size(); i++) {
            String bloque = bloques.get(i);
            System.out.printf("[Bloque %d/%d] Tamaño: %d caracteres\n", 
                i+1, totalBloques, bloque.length());
            
            try {
                ejecutarSP(conexion, "ADD_IPC_INDICES_PONDERACIONES_COTIZACIONES", anio, mes, bloque);
                exitosos++;
            } catch (SQLException e) {
                System.err.printf("¡ERROR en bloque %d/%d (tamaño: %d chars)! Código: %s, Estado: %s\n",
                    i+1, totalBloques, bloque.length(), e.getErrorCode(), e.getSQLState());
                System.err.println("Mensaje: " + e.getMessage());
                
                System.err.println("Inicio del bloque problemático:\n" + 
                    bloque.substring(0, Math.min(200, bloque.length())));
                System.err.println("\nFin del bloque problemático:\n" + 
                    bloque.substring(Math.max(0, bloque.length() - 200)));
            }
            
            if ((i+1) % 20 == 0) {
                try { Thread.sleep(300); } catch (InterruptedException e) {}
            }
        }
        
        System.out.println("Resumen final: " + exitosos + "/" + totalBloques + " bloques procesados exitosamente");
    }

    private static void ejecutarSP(Connection conexion, String funcion, int anio, int mes, String datos) throws SQLException {
        String sql = "{call dbo.sp_tran_SIP(?, ?, ?, ?)}";
        try (CallableStatement cstmt = conexion.prepareCall(sql)) {
            cstmt.setString(1, funcion);
            cstmt.setInt(2, anio);
            cstmt.setInt(3, mes);
            cstmt.setString(4, datos);
            cstmt.execute();
        }
    }

}