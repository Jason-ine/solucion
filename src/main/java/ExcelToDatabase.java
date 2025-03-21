package main.java;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ExcelToDatabase {

    
    public static void main(String[] args) {
        
        procesarArchivoExcel(
            "C:\\Users\\jdpivaral\\WebScraping\\solucion\\archivos_excel\\Base_IPM.xlsx",
            "SIP_IPM"
        );

        procesarArchivoExcel(
            "C:\\Users\\jdpivaral\\WebScraping\\solucion\\archivos_excel\\EMPRESAS_IPP.xlsx",
            "SIP_IPP"
        );

        procesarArchivoExcel(
            "C:\\Users\\jdpivaral\\WebScraping\\solucion\\archivos_excel\\Regiones.xlsx",
            "SIP_Cobertura_Fuentes"
        );

        procesarArchivoExcel(
            "C:\\Users\\jdpivaral\\WebScraping\\solucion\\archivos_excel\\Precios_promedio_IPC_x_mes_region.xlsx",
            "SIP_IPC_Precios_Promedio"
        );

        procesarArchivoExcel(
            "C:\\Users\\jdpivaral\\WebScraping\\solucion\\archivos_excel\\Base_IPMC.xlsx",
            "SIP_IPMC"
        );
    }

    
    public static void procesarArchivoExcel(String rutaExcel, String nombreTabla) {
        Connection conexion = null;
        try {
          
            String url = "jdbc:sqlserver://10.0.0.19:1433;databaseName=TablerosIPC;encrypt=true;trustServerCertificate=true";
            String usu = "ipcwork";
            String contraseña = "1pcWR1t3R/*2025";
            conexion = DriverManager.getConnection(url, usu, contraseña);

            // Leer el archivo Excel
            InputStream archivoExcel = new FileInputStream(rutaExcel);
            Workbook workbook = new XSSFWorkbook(archivoExcel);
            Sheet hoja = workbook.getSheetAt(0); // Lee la primera hoja

            // Lee la primera fila (encabezados) para obtener los índices de las columnas
            Row encabezados = hoja.getRow(0);
            Map<String, Integer> columnas = new HashMap<>();

            for (Cell celda : encabezados) {
                columnas.put(celda.getStringCellValue().trim().toLowerCase(), celda.getColumnIndex());
            }

            // Preparar la consulta SQL para insertar datos
            String sql = "";
            PreparedStatement pstmt = null;

            switch (nombreTabla) {
                case "SIP_IPM":
                    sql = "INSERT INTO SIP_IPM (region, departamento, municipio, semana, usuario, numero_boleta, codigo_tipo_fuente, tipo_fuente_nombre, codigo_fuente, nombre_fuente, direccion, zona, latitud, longitud, georefenciada, id, correlativo, fecha, mes, anio) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    pstmt = conexion.prepareStatement(sql);
                    procesarSIP_IPM(hoja, pstmt, columnas);
                    break;

                case "SIP_IPP":
                    sql = "INSERT INTO SIP_IPP (numero, estado, empadronada, tipo_empresa, codigo_tipologia, tipologia_nombre, nit, ajuste, razon_social, nombre_comercial, direccion, departamento, municipio, zona, latitud, longitud, georeferenciada, telefono, actividad_economica, ciiu) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    pstmt = conexion.prepareStatement(sql);
                    procesarSIP_IPP(hoja, pstmt, columnas);
                    break;

                case "SIP_Cobertura_Fuentes":
                    sql = "INSERT INTO SIP_Cobertura_Fuentes (region_id, ubicacion, faltantes, departamento) " +
                          "VALUES (?, ?, ?, ?)";
                    pstmt = conexion.prepareStatement(sql);
                    procesarSIP_Cobertura_Fuentes(hoja, pstmt, columnas);
                    break;

                case "SIP_IPC_Precios_Promedio":
                    sql = "INSERT INTO SIP_IPC_Precios_Promedio (codigo_producto, producto_nombre, codigo_variedad, variedad_nombre, region_id, cantidad_base, precio, variacion, anio, mes) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    pstmt = conexion.prepareStatement(sql);
                    procesarSIP_IPC_Precios_Promedio(hoja, pstmt, columnas);
                    break;

                case "SIP_IPMC":
                    sql = "INSERT INTO SIP_IPMC (region, departamento, municipio, semana, usuario, numero_boleta, codigo_tipo_fuente, tipo_fuente_nombre, codigo_fuente, nombre_fuente, direccion, zona, latitud, longitud, georefenciada, id, correlativo, fecha, mes, anio) " +
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    pstmt = conexion.prepareStatement(sql);
                    procesarSIP_IPMC(hoja, pstmt, columnas);
                    break;
            }

            
            workbook.close();
            archivoExcel.close();
            pstmt.close();
            System.out.println("Datos cargados correctamente en la tabla: " + nombreTabla);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            
            if (conexion != null) {
                try {
                    conexion.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    
    private static void procesarSIP_IPM(Sheet hoja, PreparedStatement pstmt, Map<String, Integer> columnas) throws Exception {

        String region;
        String departamento;
        String municipio;
        Double semana;
        String usuario;
        Double numeroBoleta;
        String codigoTipoFuente;
        String tipoFuenteNombre;
        Double codigoFuente;
        String nombreFuente;
        String direccion;
        Double zona;
        Double latitud;
        Double longitud;
        Double georefenciada;
        Double id;
        Double correlativo;
        String fechaStr;
        String mes;
        Double anio;
        String latitudStr;
        String longitudStr;
        for (Row fila : hoja) {
            // Saltar la primera fila (encabezados)
            if (fila.getRowNum() == 0) {
                continue;
            }
    
           
            region = obtenerValorCelda(fila.getCell(columnas.get("region")));
            departamento = obtenerValorCelda(fila.getCell(columnas.get("departamento")));
            municipio = obtenerValorCelda(fila.getCell(columnas.get("municipio")));
            semana = obtenerValorNumerico(fila.getCell(columnas.get("semana")));
            usuario = obtenerValorCelda(fila.getCell(columnas.get("usuario")));
            numeroBoleta = obtenerValorNumerico(fila.getCell(columnas.get("numero_boleta")));
            codigoTipoFuente = obtenerValorCelda(fila.getCell(columnas.get("codigo_tipo_fuente")));
            tipoFuenteNombre = obtenerValorCelda(fila.getCell(columnas.get("tipo_fuente_nombre")));
            codigoFuente = obtenerValorNumerico(fila.getCell(columnas.get("codigo_fuente")));
            nombreFuente = obtenerValorCelda(fila.getCell(columnas.get("nombre_fuente")));
            direccion = obtenerValorCelda(fila.getCell(columnas.get("direccion")));
            zona = obtenerValorNumerico(fila.getCell(columnas.get("zona")));
            latitud = obtenerValorNumerico(fila.getCell(columnas.get("latitud"))); 
            longitud = obtenerValorNumerico(fila.getCell(columnas.get("longitud"))); 
            georefenciada = obtenerValorNumerico(fila.getCell(columnas.get("georefenciada")));
            id = obtenerValorNumerico(fila.getCell(columnas.get("id")));
            correlativo = obtenerValorNumerico(fila.getCell(columnas.get("correlativo")));
            fechaStr = obtenerValorCelda(fila.getCell(columnas.get("fecha")));
            mes = obtenerValorCelda(fila.getCell(columnas.get("mes")));
            anio = obtenerValorNumerico(fila.getCell(columnas.get("anio")));
    
            // Convertir la fecha de String a Timestamp
            Timestamp fechaTimestamp = null;
            if (fechaStr != null) {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date fecha = dateFormat.parse(fechaStr);
                fechaTimestamp = new Timestamp(fecha.getTime());
            }
    
            // Convertir latitud y longitud a String
            latitudStr = (latitud != null) ? String.valueOf(latitud) : null;
            longitudStr = (longitud != null) ? String.valueOf(longitud) : null;
    
            
            pstmt.setString(1, region);
            pstmt.setString(2, departamento);
            pstmt.setString(3, municipio);
            if (semana != null) {
                pstmt.setInt(4, semana.intValue());
            } else {
                pstmt.setNull(4, java.sql.Types.INTEGER);
            }
            pstmt.setString(5, usuario);
            if (numeroBoleta != null) {
                pstmt.setInt(6, numeroBoleta.intValue());
            } else {
                pstmt.setNull(6, java.sql.Types.INTEGER);
            }
            pstmt.setString(7, codigoTipoFuente);
            pstmt.setString(8, tipoFuenteNombre);
            if (codigoFuente != null) {
                pstmt.setInt(9, codigoFuente.intValue()); 
            } else {
                pstmt.setNull(9, java.sql.Types.INTEGER);
            }
            pstmt.setString(10, nombreFuente);
            pstmt.setString(11, direccion);
            if (zona != null) {
                pstmt.setInt(12, zona.intValue());
            } else {
                pstmt.setNull(12, java.sql.Types.INTEGER);
            }
            pstmt.setString(13, latitudStr); 
            pstmt.setString(14, longitudStr); 
            if (georefenciada != null) {
                pstmt.setInt(15, georefenciada.intValue());
            } else {
                pstmt.setNull(15, java.sql.Types.INTEGER);
            }
            if (id != null) {
                pstmt.setInt(16, id.intValue());
            } else {
                pstmt.setNull(16, java.sql.Types.INTEGER);
            }
            if (correlativo != null) {
                pstmt.setInt(17, correlativo.intValue());
            } else {
                pstmt.setNull(17, java.sql.Types.INTEGER);
            }
            if (fechaTimestamp != null) {
                pstmt.setTimestamp(18, fechaTimestamp);
            } else {
                pstmt.setNull(18, java.sql.Types.TIMESTAMP);
            }
            pstmt.setString(19, mes);
            if (anio != null) {
                pstmt.setInt(20, anio.intValue());
            } else {
                pstmt.setNull(20, java.sql.Types.INTEGER);
            }
    
            
            pstmt.executeUpdate();
        }
    }

    
    private static void procesarSIP_IPP(Sheet hoja, PreparedStatement pstmt, Map<String, Integer> columnas) throws Exception {
        Double numero;
        String estado;
        String empadronada;
        String tipoEmpresa;
        Double codigoTipologia;
        String tipologiaNombre;
        String nit;
        String ajuste;
        String razonSocial;
        String nombreComercial;
        String direccion;
        String departamento;
        String municipio;
        Double zona ;
        Double latitud ;
        Double longitud;
        Double georeferenciada;
        String telefono ;
        String actividadEconomica ;
        String ciiu ;
        String latitudStr;
        String longitudStr;
        for (Row fila : hoja) {
            // Saltar la primera fila (encabezados)
            if (fila.getRowNum() == 0) {
                continue;
            }
    
            numero = obtenerValorNumerico(fila.getCell(columnas.get("numero")));
            estado = obtenerValorCelda(fila.getCell(columnas.get("estado")));
            empadronada = obtenerValorCelda(fila.getCell(columnas.get("empadronada")));
            tipoEmpresa = obtenerValorCelda(fila.getCell(columnas.get("tipo_empresa")));
            codigoTipologia = obtenerValorNumerico(fila.getCell(columnas.get("codigo_tipologia")));
            tipologiaNombre = obtenerValorCelda(fila.getCell(columnas.get("tipologia_nombre")));
            nit = obtenerValorCelda(fila.getCell(columnas.get("nit")));
            ajuste = obtenerValorCelda(fila.getCell(columnas.get("ajuste")));
            razonSocial = obtenerValorCelda(fila.getCell(columnas.get("razon_social")));
            nombreComercial = obtenerValorCelda(fila.getCell(columnas.get("nombre_comercial")));
            direccion = obtenerValorCelda(fila.getCell(columnas.get("direccion")));
            departamento = obtenerValorCelda(fila.getCell(columnas.get("departamento")));
            municipio = obtenerValorCelda(fila.getCell(columnas.get("municipio")));
            zona = obtenerValorNumerico(fila.getCell(columnas.get("zona")));
            latitud = obtenerValorNumerico(fila.getCell(columnas.get("latitud"))); 
            longitud = obtenerValorNumerico(fila.getCell(columnas.get("longitud"))); 
            georeferenciada = obtenerValorNumerico(fila.getCell(columnas.get("georeferenciada")));
            telefono = obtenerValorCelda(fila.getCell(columnas.get("telefono")));
            actividadEconomica = obtenerValorCelda(fila.getCell(columnas.get("actividad_economica")));
            ciiu = obtenerValorCelda(fila.getCell(columnas.get("ciiu")));
    
            
            latitudStr = (latitud != null) ? String.valueOf(latitud) : null;
            longitudStr = (longitud != null) ? String.valueOf(longitud) : null;
    
           
            if (numero != null) {
                pstmt.setDouble(1, numero);
            } else {
                pstmt.setNull(1, java.sql.Types.DOUBLE);
            }
            pstmt.setString(2, estado);
            pstmt.setString(3, empadronada);
            pstmt.setString(4, tipoEmpresa);
            if (codigoTipologia != null) {
                pstmt.setLong(5, codigoTipologia.longValue());
            } else {
                pstmt.setNull(5, java.sql.Types.BIGINT);
            }
            pstmt.setString(6, tipologiaNombre);
            pstmt.setString(7, nit);
            pstmt.setString(8, ajuste);
            pstmt.setString(9, razonSocial);
            pstmt.setString(10, nombreComercial);
            pstmt.setString(11, direccion);
            pstmt.setString(12, departamento);
            pstmt.setString(13, municipio);
            if (zona != null) {
                pstmt.setLong(14, zona.longValue());
            } else {
                pstmt.setNull(14, java.sql.Types.BIGINT);
            }
            pstmt.setString(15, latitudStr); 
            pstmt.setString(16, longitudStr); 
            if (georeferenciada != null) {
                pstmt.setLong(17, georeferenciada.longValue());
            } else {
                pstmt.setNull(17, java.sql.Types.BIGINT);
            }
            pstmt.setString(18, telefono);
            pstmt.setString(19, actividadEconomica);
            pstmt.setString(20, ciiu);
    
            pstmt.executeUpdate();
        }
    }

    private static void procesarSIP_Cobertura_Fuentes(Sheet hoja, PreparedStatement pstmt, Map<String, Integer> columnas) throws Exception {
        Double regionId;
        String ubicacion;
        Double faltantes;
        String departamento;
        for (Row fila : hoja) {
            // Saltar la primera fila (encabezados)
            if (fila.getRowNum() == 0) {
                continue;
            }
    
            
            regionId = obtenerValorNumerico(fila.getCell(columnas.get("region_id")));
            ubicacion = obtenerValorCelda(fila.getCell(columnas.get("ubicacion")));
            faltantes = obtenerValorNumerico(fila.getCell(columnas.get("faltantes")));
            departamento = obtenerValorCelda(fila.getCell(columnas.get("departamento")));
    
           
            if (regionId != null) {
                pstmt.setLong(1, regionId.longValue());
            } else {
                pstmt.setNull(1, java.sql.Types.BIGINT);
            }
            pstmt.setString(2, ubicacion);
            if (faltantes != null) {
                pstmt.setLong(3, faltantes.longValue());
            } else {
                pstmt.setNull(3, java.sql.Types.BIGINT);
            }
            pstmt.setString(4, departamento);
    
           
            pstmt.executeUpdate();
        }
    }


    private static void procesarSIP_IPC_Precios_Promedio(Sheet hoja, PreparedStatement pstmt, Map<String, Integer> columnas) throws Exception {
        Double codigoProducto;
        String productoNombre;
        Double codigoVariedad;
        String variedadNombre;
        Double regionId;
        Double cantidadBase;
        Double precio;
        Double variacion;
        Double anio ;
        Double mes;
        for (Row fila : hoja) {
            // Saltar la primera fila (encabezados)
            if (fila.getRowNum() == 0) {
                continue;
            }
    
            
            codigoProducto = obtenerValorNumerico(fila.getCell(columnas.get("cod_prod")));
            productoNombre = obtenerValorCelda(fila.getCell(columnas.get("producto_nombre")));
            codigoVariedad = obtenerValorNumerico(fila.getCell(columnas.get("codigo_articulo")));
            variedadNombre = obtenerValorCelda(fila.getCell(columnas.get("articulo")));
            regionId = obtenerValorNumerico(fila.getCell(columnas.get("region_id")));
            cantidadBase = obtenerValorNumerico(fila.getCell(columnas.get("cant_b")));
            precio = obtenerValorNumerico(fila.getCell(columnas.get("pgeo")));
            variacion = obtenerValorNumerico(fila.getCell(columnas.get("variacion")));
            anio = obtenerValorNumerico(fila.getCell(columnas.get("anio")));
            mes = obtenerValorNumerico(fila.getCell(columnas.get("mes")));
    
            
            if (codigoProducto != null) {
                pstmt.setLong(1, codigoProducto.longValue());
            } else {
                pstmt.setNull(1, java.sql.Types.BIGINT);
            }
            pstmt.setString(2, productoNombre);
            if (codigoVariedad != null) {
                pstmt.setLong(3, codigoVariedad.longValue());
            } else {
                pstmt.setNull(3, java.sql.Types.BIGINT);
            }
            pstmt.setString(4, variedadNombre);
            if (regionId != null) {
                pstmt.setLong(5, regionId.longValue());
            } else {
                pstmt.setNull(5, java.sql.Types.BIGINT);
            }
            if (cantidadBase != null) {
                pstmt.setDouble(6, cantidadBase);
            } else {
                pstmt.setNull(6, java.sql.Types.DOUBLE);
            }
            if (precio != null) {
                pstmt.setDouble(7, precio);
            } else {
                pstmt.setNull(7, java.sql.Types.DOUBLE);
            }
            if (variacion != null) {
                pstmt.setDouble(8, variacion);
            } else {
                pstmt.setNull(8, java.sql.Types.DOUBLE);
            }
            if (anio != null) {
                pstmt.setInt(9, anio.intValue());
            } else {
                pstmt.setNull(9, java.sql.Types.INTEGER);
            }
            if (mes != null) {
                pstmt.setInt(10, mes.intValue());
            } else {
                pstmt.setNull(10, java.sql.Types.INTEGER);
            }
    
           
            pstmt.executeUpdate();
        }
    }

    
    private static void procesarSIP_IPMC(Sheet hoja, PreparedStatement pstmt, Map<String, Integer> columnas) throws Exception {
        String region;
        String departamento;
        String municipio;
        Double semana;
        String usuario;
        Double numeroBoleta;
        String codigoTipoFuente;
        String tipoFuenteNombre;
        String codigoFuente;
        String nombreFuente;
        String direccion;
        Double zona;
        Double latitud;
        Double longitud;
        Double georefenciada;
        Double id;
        Double correlativo;
        String fechaStr;
        String mes;
        int anio;
        String latitudStr;
        String longitudStr;
        for (Row fila : hoja) {
            // Saltar la primera fila (encabezados)
            if (fila.getRowNum() == 0) {
                continue;
            }
    
            
            fechaStr = obtenerValorCelda(fila.getCell(columnas.get("fecha")));
            region = obtenerValorCelda(fila.getCell(columnas.get("region")));
            departamento = obtenerValorCelda(fila.getCell(columnas.get("departamento")));
            municipio = obtenerValorCelda(fila.getCell(columnas.get("municipio")));
            semana = obtenerValorNumerico(fila.getCell(columnas.get("semana")));
            usuario = obtenerValorCelda(fila.getCell(columnas.get("usuario")));
            numeroBoleta = obtenerValorNumerico(fila.getCell(columnas.get("numero_boleta")));
            codigoTipoFuente = obtenerValorCelda(fila.getCell(columnas.get("codigo_tipo_fuente")));
            tipoFuenteNombre = obtenerValorCelda(fila.getCell(columnas.get("tipo_fuente")));
            codigoFuente = obtenerValorCelda(fila.getCell(columnas.get("codigo_fuente")));
            nombreFuente = obtenerValorCelda(fila.getCell(columnas.get("nombre_fuente")));
            direccion = obtenerValorCelda(fila.getCell(columnas.get("direccion")));
            zona = obtenerValorNumerico(fila.getCell(columnas.get("zona")));
            latitud = obtenerValorNumerico(fila.getCell(columnas.get("gps_latitud"))); 
            longitud = obtenerValorNumerico(fila.getCell(columnas.get("gps_longitud"))); 
            georefenciada = obtenerValorNumerico(fila.getCell(columnas.get("georeferenciada")));
            id = obtenerValorNumerico(fila.getCell(columnas.get("id")));
            correlativo = obtenerValorNumerico(fila.getCell(columnas.get("correlativo")));
            mes = obtenerValorCelda(fila.getCell(columnas.get("mes")));
    
            
            Timestamp fechaTimestamp = null;
            if (fechaStr != null) {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                Date fecha = dateFormat.parse(fechaStr);
                fechaTimestamp = new Timestamp(fecha.getTime());
            }
    
            
            anio = obtenerAnioDesdeFecha(fechaTimestamp != null ? new Date(fechaTimestamp.getTime()) : new Date());
    
           
            latitudStr = (latitud != null) ? String.valueOf(latitud) : null;
            longitudStr = (longitud != null) ? String.valueOf(longitud) : null;
    
    
            
            pstmt.setString(1, region);
            pstmt.setString(2, departamento);
            pstmt.setString(3, municipio);
            if (semana != null) {
                pstmt.setInt(4, semana.intValue());
            } else {
                pstmt.setNull(4, java.sql.Types.INTEGER);
            }
            pstmt.setString(5, usuario);
            if (numeroBoleta != null) {
                pstmt.setInt(6, numeroBoleta.intValue());
            } else {
                pstmt.setNull(6, java.sql.Types.INTEGER);
            }
            pstmt.setString(7, codigoTipoFuente);
            pstmt.setString(8, tipoFuenteNombre);
            pstmt.setString(9, codigoFuente);
            pstmt.setString(10, nombreFuente);
            pstmt.setString(11, direccion);
            if (zona != null) {
                pstmt.setInt(12, zona.intValue());
            } else {
                pstmt.setNull(12, java.sql.Types.INTEGER);
            }
            pstmt.setString(13, latitudStr); 
            pstmt.setString(14, longitudStr); 
            if (georefenciada != null) {
                pstmt.setInt(15, georefenciada.intValue());
            } else {
                pstmt.setNull(15, java.sql.Types.INTEGER);
            }
            if (id != null) {
                pstmt.setInt(16, id.intValue());
            } else {
                pstmt.setNull(16, java.sql.Types.INTEGER);
            }
            if (correlativo != null) {
                pstmt.setInt(17, correlativo.intValue());
            } else {
                pstmt.setNull(17, java.sql.Types.INTEGER);
            }
            if (fechaTimestamp != null) {
                pstmt.setTimestamp(18, fechaTimestamp);
            } else {
                pstmt.setNull(18, java.sql.Types.TIMESTAMP);
            }
            pstmt.setString(19, mes);
            pstmt.setInt(20, anio);
    
            
            pstmt.executeUpdate();
        }
    }
    // Método auxiliar para obtener el año desde una fecha
    private static int obtenerAnioDesdeFecha(Date fecha) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy");
        return Integer.parseInt(dateFormat.format(fecha));
    }
    // Método auxiliar para obtener el valor de una celda como String
    private static String obtenerValorCelda(Cell celda) {
        if (celda == null || celda.getCellType() == CellType.BLANK) {
            return null; 
        }
        switch (celda.getCellType()) {
            case STRING:
                return celda.getStringCellValue().trim(); 
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(celda)) {
                    // Si es una fecha, formatearla como texto
                    Date fecha = DateUtil.getJavaDate(celda.getNumericCellValue());
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    return dateFormat.format(fecha);
                } else {
                    
                    return String.valueOf((long) celda.getNumericCellValue());
                }
            case BOOLEAN:
                return String.valueOf(celda.getBooleanCellValue());
            default:
                return null; // Devuelve null para otros tipos no manejados
        }
    }
    // Método auxiliar para obtener el valor de una celda como número
    private static Double obtenerValorNumerico(Cell celda) {
        if (celda == null || celda.getCellType() == CellType.BLANK) {
            return null; 
        }
        if (celda.getCellType() == CellType.NUMERIC) {
            return celda.getNumericCellValue(); 
        }
        return null; // Devuelve null para otros tipos no manejados
    }
}