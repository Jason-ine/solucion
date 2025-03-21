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

    // Método principal
    public static void main(String[] args) {
        // Procesar cada archivo Excel y tabla correspondiente
        procesarArchivoExcel(
            "C:\\Users\\jason\\source\\repos\\solucion\\archivos_excel\\Base_IPM.xlsx",
            "SIP_IPM"
        );

        procesarArchivoExcel(
            "C:\\Users\\jason\\source\\repos\\solucion\\archivos_excel\\EMPRESAS_IPP.xlsx",
            "SIP_IPP"
        );

        procesarArchivoExcel(
            "C:\\Users\\jason\\source\\repos\\solucion\\archivos_excel\\Regiones.xlsx",
            "SIP_Cobertura_Fuentes"
        );

        procesarArchivoExcel(
            "C:\\Users\\jason\\source\\repos\\solucion\\archivos_excel\\Precios_promedio_IPC_x_mes_region.xlsx",
            "SIP_IPC_Precios_Promedio"
        );

        procesarArchivoExcel(
            "C:\\Users\\jason\\source\\repos\\solucion\\archivos_excel\\Base_IPMC.xlsx",
            "SIP_IPMC"
        );
    }

    // Método para procesar un archivo Excel y cargar los datos en la tabla correspondiente
    public static void procesarArchivoExcel(String rutaExcel, String nombreTabla) {
        Connection conexion = null;
        try {
            // 1. Conectar a la base de datos (SQL Server)
            String url = "jdbc:sqlserver://JASON_PIVARAL:1433;databaseName=db_excel;encrypt=true;trustServerCertificate=true";
            String usu = "sa";
            String contraseña = "Abc$2020";
            conexion = DriverManager.getConnection(url, usu, contraseña);

            // 2. Leer el archivo Excel
            InputStream archivoExcel = new FileInputStream(rutaExcel);
            Workbook workbook = new XSSFWorkbook(archivoExcel);
            Sheet hoja = workbook.getSheetAt(0); // Lee la primera hoja

            // 3. Leer la primera fila (encabezados) para obtener los índices de las columnas
            Row encabezados = hoja.getRow(0);
            Map<String, Integer> columnas = new HashMap<>();

            for (Cell celda : encabezados) {
                columnas.put(celda.getStringCellValue().trim().toLowerCase(), celda.getColumnIndex());
            }

            // 4. Preparar la consulta SQL para insertar datos
            String sql = "";
            PreparedStatement pstmt = null;

            // 5. Determinar la estructura de la tabla y preparar la consulta SQL
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

            // 6. Cerrar recursos
            workbook.close();
            archivoExcel.close();
            pstmt.close();
            System.out.println("Datos cargados correctamente en la tabla: " + nombreTabla);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Cerrar la conexión a la base de datos
            if (conexion != null) {
                try {
                    conexion.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    // Método para procesar datos de la tabla SIP_IPM
    private static void procesarSIP_IPM(Sheet hoja, PreparedStatement pstmt, Map<String, Integer> columnas) throws Exception {
        for (Row fila : hoja) {
            // Saltar la primera fila (encabezados)
            if (fila.getRowNum() == 0) {
                continue;
            }

            // Obtener los valores de cada celda usando los nombres de las columnas
            String region = obtenerValorCelda(fila.getCell(columnas.get("region")));
            String departamento = obtenerValorCelda(fila.getCell(columnas.get("departamento")));
            String municipio = obtenerValorCelda(fila.getCell(columnas.get("municipio")));
            int semana = (int) obtenerValorNumerico(fila.getCell(columnas.get("semana")));
            String usuario = obtenerValorCelda(fila.getCell(columnas.get("usuario")));
            int numeroBoleta = (int) obtenerValorNumerico(fila.getCell(columnas.get("numero_boleta")));
            String codigoTipoFuente = obtenerValorCelda(fila.getCell(columnas.get("codigo_tipo_fuente")));
            String tipoFuenteNombre = obtenerValorCelda(fila.getCell(columnas.get("tipo_fuente_nombre")));
            String codigoFuente = obtenerValorCelda(fila.getCell(columnas.get("codigo_fuente")));
            String nombreFuente = obtenerValorCelda(fila.getCell(columnas.get("nombre_fuente")));
            String direccion = obtenerValorCelda(fila.getCell(columnas.get("direccion")));
            int zona = (int) obtenerValorNumerico(fila.getCell(columnas.get("zona")));
            String latitud = obtenerValorCelda(fila.getCell(columnas.get("latitud")));
            String longitud = obtenerValorCelda(fila.getCell(columnas.get("longitud")));
            int georefenciada = (int) obtenerValorNumerico(fila.getCell(columnas.get("georefenciada")));
            int id = (int) obtenerValorNumerico(fila.getCell(columnas.get("id")));
            int correlativo = (int) obtenerValorNumerico(fila.getCell(columnas.get("correlativo")));
            String fechaStr = obtenerValorCelda(fila.getCell(columnas.get("fecha")));
            String mes = obtenerValorCelda(fila.getCell(columnas.get("mes")));
            int anio = (int) obtenerValorNumerico(fila.getCell(columnas.get("anio")));

            // Convertir la fecha de String a Timestamp
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date fecha = dateFormat.parse(fechaStr);
            Timestamp fechaTimestamp = new Timestamp(fecha.getTime());

            // Asignar los valores a la consulta SQL
            pstmt.setString(1, region);
            pstmt.setString(2, departamento);
            pstmt.setString(3, municipio);
            pstmt.setInt(4, semana);
            pstmt.setString(5, usuario);
            pstmt.setInt(6, numeroBoleta);
            pstmt.setString(7, codigoTipoFuente);
            pstmt.setString(8, tipoFuenteNombre);
            pstmt.setString(9, codigoFuente);
            pstmt.setString(10, nombreFuente);
            pstmt.setString(11, direccion);
            pstmt.setInt(12, zona);
            pstmt.setString(13, latitud);
            pstmt.setString(14, longitud);
            pstmt.setInt(15, georefenciada);
            pstmt.setInt(16, id);
            pstmt.setInt(17, correlativo);
            pstmt.setTimestamp(18, fechaTimestamp);
            pstmt.setString(19, mes);
            pstmt.setInt(20, anio);

            // Ejecutar la inserción
            pstmt.executeUpdate();
        }
    }

    // Método para procesar datos de la tabla SIP_IPP
    private static void procesarSIP_IPP(Sheet hoja, PreparedStatement pstmt, Map<String, Integer> columnas) throws Exception {
        for (Row fila : hoja) {
            // Saltar la primera fila (encabezados)
            if (fila.getRowNum() == 0) {
                continue;
            }

            // Obtener los valores de cada celda usando los nombres de las columnas
            long numero = (long) obtenerValorNumerico(fila.getCell(columnas.get("numero")));
            String estado = obtenerValorCelda(fila.getCell(columnas.get("estado")));
            String empadronada = obtenerValorCelda(fila.getCell(columnas.get("empadronada")));
            String tipoEmpresa = obtenerValorCelda(fila.getCell(columnas.get("tipo_empresa")));
            long codigoTipologia = (long) obtenerValorNumerico(fila.getCell(columnas.get("codigo_tipologia")));
            String tipologiaNombre = obtenerValorCelda(fila.getCell(columnas.get("tipologia_nombre")));
            String nit = obtenerValorCelda(fila.getCell(columnas.get("nit")));
            String ajuste = obtenerValorCelda(fila.getCell(columnas.get("ajuste")));
            String razonSocial = obtenerValorCelda(fila.getCell(columnas.get("razon_social")));
            String nombreComercial = obtenerValorCelda(fila.getCell(columnas.get("nombre_comercial")));
            String direccion = obtenerValorCelda(fila.getCell(columnas.get("direccion")));
            String departamento = obtenerValorCelda(fila.getCell(columnas.get("departamento")));
            String municipio = obtenerValorCelda(fila.getCell(columnas.get("municipio")));
            long zona = (long) obtenerValorNumerico(fila.getCell(columnas.get("zona")));
            String latitud = obtenerValorCelda(fila.getCell(columnas.get("latitud")));
            String longitud = obtenerValorCelda(fila.getCell(columnas.get("longitud")));
            long georeferenciada = (long) obtenerValorNumerico(fila.getCell(columnas.get("georeferenciada")));
            String telefono = obtenerValorCelda(fila.getCell(columnas.get("telefono")));
            String actividadEconomica = obtenerValorCelda(fila.getCell(columnas.get("actividad_economica")));
            String ciiu = obtenerValorCelda(fila.getCell(columnas.get("ciiu")));

            // Asignar los valores a la consulta SQL
            pstmt.setLong(1, numero);
            pstmt.setString(2, estado);
            pstmt.setString(3, empadronada);
            pstmt.setString(4, tipoEmpresa);
            pstmt.setLong(5, codigoTipologia);
            pstmt.setString(6, tipologiaNombre);
            pstmt.setString(7, nit);
            pstmt.setString(8, ajuste);
            pstmt.setString(9, razonSocial);
            pstmt.setString(10, nombreComercial);
            pstmt.setString(11, direccion);
            pstmt.setString(12, departamento);
            pstmt.setString(13, municipio);
            pstmt.setLong(14, zona);
            pstmt.setString(15, latitud);
            pstmt.setString(16, longitud);
            pstmt.setLong(17, georeferenciada);
            pstmt.setString(18, telefono);
            pstmt.setString(19, actividadEconomica);
            pstmt.setString(20, ciiu);

            // Ejecutar la inserción
            pstmt.executeUpdate();
        }
    }

    // Método para procesar datos de la tabla SIP_Cobertura_Fuentes
    private static void procesarSIP_Cobertura_Fuentes(Sheet hoja, PreparedStatement pstmt, Map<String, Integer> columnas) throws Exception {
        for (Row fila : hoja) {
            // Saltar la primera fila (encabezados)
            if (fila.getRowNum() == 0) {
                continue;
            }

            // Obtener los valores de cada celda usando los nombres de las columnas
            long regionId = (long) obtenerValorNumerico(fila.getCell(columnas.get("region_id")));
            String ubicacion = obtenerValorCelda(fila.getCell(columnas.get("ubicacion")));
            long faltantes = (long) obtenerValorNumerico(fila.getCell(columnas.get("faltantes")));
            String departamento = obtenerValorCelda(fila.getCell(columnas.get("departamento")));

            // Asignar los valores a la consulta SQL
            pstmt.setLong(1, regionId);
            pstmt.setString(2, ubicacion);
            pstmt.setLong(3, faltantes);
            pstmt.setString(4, departamento);

            // Ejecutar la inserción
            pstmt.executeUpdate();
        }
    }

    // Método para procesar datos de la tabla SIP_IPC_Precios_Promedio
    private static void procesarSIP_IPC_Precios_Promedio(Sheet hoja, PreparedStatement pstmt, Map<String, Integer> columnas) throws Exception {
        for (Row fila : hoja) {
            // Saltar la primera fila (encabezados)
            if (fila.getRowNum() == 0) {
                continue;
            }

            // Obtener los valores de cada celda usando los nombres de las columnas
            long codigoProducto = (long) obtenerValorNumerico(fila.getCell(columnas.get("cod_prod")));
            String productoNombre = obtenerValorCelda(fila.getCell(columnas.get("producto_nombre")));
            long codigoVariedad = (long) obtenerValorNumerico(fila.getCell(columnas.get("codigo_articulo")));
            String variedadNombre = obtenerValorCelda(fila.getCell(columnas.get("articulo")));
            long regionId = (long) obtenerValorNumerico(fila.getCell(columnas.get("region_id")));
            double cantidadBase = obtenerValorNumerico(fila.getCell(columnas.get("cant_b")));
            double precio = obtenerValorNumerico(fila.getCell(columnas.get("pgeo")));
            double variacion = obtenerValorNumerico(fila.getCell(columnas.get("variacion")));
            int anio = (int) obtenerValorNumerico(fila.getCell(columnas.get("anio")));
            int mes = (int) obtenerValorNumerico(fila.getCell(columnas.get("mes")));

            // Asignar los valores a la consulta SQL
            pstmt.setLong(1, codigoProducto);
            pstmt.setString(2, productoNombre);
            pstmt.setLong(3, codigoVariedad);
            pstmt.setString(4, variedadNombre);
            pstmt.setLong(5, regionId);
            pstmt.setDouble(6, cantidadBase);
            pstmt.setDouble(7, precio);
            pstmt.setDouble(8, variacion);
            pstmt.setInt(9, anio);
            pstmt.setInt(10, mes);

            // Ejecutar la inserción
            pstmt.executeUpdate();
        }
    }

    private static void procesarSIP_IPMC(Sheet hoja, PreparedStatement pstmt, Map<String, Integer> columnas) throws Exception {
        for (Row fila : hoja) {
            // Saltar la primera fila (encabezados)
            if (fila.getRowNum() == 0) {
                continue;
            }
    
            // Obtener los valores de cada celda usando los nombres de las columnas
            String fechaStr = obtenerValorCelda(fila.getCell(columnas.get("fecha")));
            String region = obtenerValorCelda(fila.getCell(columnas.get("region")));
            String departamento = obtenerValorCelda(fila.getCell(columnas.get("departamento")));
            String municipio = obtenerValorCelda(fila.getCell(columnas.get("municipio")));
            int semana = (int) obtenerValorNumerico(fila.getCell(columnas.get("semana")));
            String usuario = obtenerValorCelda(fila.getCell(columnas.get("usuario")));
            int numeroBoleta = (int) obtenerValorNumerico(fila.getCell(columnas.get("numero_boleta")));
            String codigoTipoFuente = obtenerValorCelda(fila.getCell(columnas.get("codigo_tipo_fuente")));
            String tipoFuenteNombre = obtenerValorCelda(fila.getCell(columnas.get("tipo_fuente"))); // Mapeado a "tipo_fuente" en el Excel
            String codigoFuente = obtenerValorCelda(fila.getCell(columnas.get("codigo_fuente")));
            String nombreFuente = obtenerValorCelda(fila.getCell(columnas.get("nombre_fuente")));
            String direccion = obtenerValorCelda(fila.getCell(columnas.get("direccion")));
            int zona = (int) obtenerValorNumerico(fila.getCell(columnas.get("zona")));
            String latitud = obtenerValorCelda(fila.getCell(columnas.get("gps_latitud"))); // Mapeado a "gps_latitud" en el Excel
            String longitud = obtenerValorCelda(fila.getCell(columnas.get("gps_longitud"))); // Mapeado a "gps_longitud" en el Excel
            int georefenciada = (int) obtenerValorNumerico(fila.getCell(columnas.get("georeferenciada"))); // Mapeado a "georeferenciada" en el Excel
            int id = (int) obtenerValorNumerico(fila.getCell(columnas.get("id")));
            int correlativo = (int) obtenerValorNumerico(fila.getCell(columnas.get("correlativo")));
            String mes = obtenerValorCelda(fila.getCell(columnas.get("mes")));
    
            // Convertir la fecha de String a Timestamp
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date fecha = dateFormat.parse(fechaStr);
            Timestamp fechaTimestamp = new Timestamp(fecha.getTime());
    
            // Extraer el año de la fecha
            int anio = obtenerAnioDesdeFecha(fecha);
    
            // Asignar los valores a la consulta SQL
            pstmt.setString(1, region);
            pstmt.setString(2, departamento);
            pstmt.setString(3, municipio);
            pstmt.setInt(4, semana);
            pstmt.setString(5, usuario);
            pstmt.setInt(6, numeroBoleta);
            pstmt.setString(7, codigoTipoFuente);
            pstmt.setString(8, tipoFuenteNombre);
            pstmt.setString(9, codigoFuente);
            pstmt.setString(10, nombreFuente);
            pstmt.setString(11, direccion);
            pstmt.setInt(12, zona);
            pstmt.setString(13, latitud);
            pstmt.setString(14, longitud);
            pstmt.setInt(15, georefenciada);
            pstmt.setInt(16, id);
            pstmt.setInt(17, correlativo);
            pstmt.setTimestamp(18, fechaTimestamp);
            pstmt.setString(19, mes);
            pstmt.setInt(20, anio);
    
            // Ejecutar la inserción
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
        if (celda == null) {
            return "";
        }
        switch (celda.getCellType()) {
            case STRING:
                return celda.getStringCellValue();
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(celda)) {
                    Date fecha = DateUtil.getJavaDate(celda.getNumericCellValue());
                    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    return dateFormat.format(fecha);
                } else {
                    return String.valueOf(celda.getNumericCellValue());
                }
            case BOOLEAN:
                return String.valueOf(celda.getBooleanCellValue());
            default:
                return "";
        }
    }

    // Método auxiliar para obtener el valor de una celda como número
    private static double obtenerValorNumerico(Cell celda) {
        if (celda == null || celda.getCellType() != CellType.NUMERIC) {
            return 0.0;
        }
        return celda.getNumericCellValue();
    }
}