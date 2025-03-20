package main.java;

import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

public class ExcelToDatabase {

    // Método para leer un archivo Excel y cargar los datos en la base de datos
    public static void cargarExcelABaseDeDatos(String rutaExcel, String nombreTabla, Map<Integer, String> mapeoColumnas) {
        Connection conexion = null;
        try {
            // 1. Conectar a la base de datos (SQL Server)
            String url = "jdbc:sqlserver://localhost:1433;databaseName=tu_basedatos";
            String usuario = "tu_usuario";
            String contraseña = "tu_contraseña";
            conexion = DriverManager.getConnection(url, usuario, contraseña);

            // 2. Leer el archivo Excel
            InputStream archivoExcel = new FileInputStream(rutaExcel);
            Workbook workbook = new XSSFWorkbook(archivoExcel);
            Sheet hoja = workbook.getSheetAt(0); // Lee la primera hoja

            // 3. Preparar la consulta SQL para insertar datos
            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO " + nombreTabla + " (");
            for (String columna : mapeoColumnas.values()) {
                sqlBuilder.append(columna).append(", ");
            }
            sqlBuilder.delete(sqlBuilder.length() - 2, sqlBuilder.length()); // Eliminar la última coma
            sqlBuilder.append(") VALUES (");
            for (int i = 0; i < mapeoColumnas.size(); i++) {
                sqlBuilder.append("?, ");
            }
            sqlBuilder.delete(sqlBuilder.length() - 2, sqlBuilder.length()); // Eliminar la última coma
            sqlBuilder.append(")");

            String sql = sqlBuilder.toString();
            PreparedStatement pstmt = conexion.prepareStatement(sql);

            // 4. Recorrer las filas del Excel e insertar los datos
            for (Row fila : hoja) {
                for (Map.Entry<Integer, String> entry : mapeoColumnas.entrySet()) {
                    int indiceColumna = entry.getKey();
                    Cell celda = fila.getCell(indiceColumna);

                    if (celda != null) {
                        switch (celda.getCellType()) {
                            case STRING:
                                pstmt.setString(indiceColumna + 1, celda.getStringCellValue());
                                break;
                            case NUMERIC:
                                pstmt.setDouble(indiceColumna + 1, celda.getNumericCellValue());
                                break;
                            case BOOLEAN:
                                pstmt.setBoolean(indiceColumna + 1, celda.getBooleanCellValue());
                                break;
                            default:
                                pstmt.setString(indiceColumna + 1, ""); // Valor por defecto
                        }
                    } else {
                        pstmt.setString(indiceColumna + 1, ""); // Valor por defecto si la celda está vacía
                    }
                }
                pstmt.executeUpdate();
            }

            // 5. Cerrar recursos
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

    public static void main(String[] args) {
        // Mapeo de columnas para cada archivo Excel
        Map<Integer, String> mapeoArchivo1 = new HashMap<>();
        mapeoArchivo1.put(0, "nombre"); // Columna A (índice 0) -> columna "nombre"
        mapeoArchivo1.put(1, "edad");   // Columna B (índice 1) -> columna "edad"
        mapeoArchivo1.put(2, "salario"); // Columna C (índice 2) -> columna "salario"

        Map<Integer, String> mapeoArchivo2 = new HashMap<>();
        mapeoArchivo2.put(0, "producto"); // Columna A (índice 0) -> columna "producto"
        mapeoArchivo2.put(1, "precio");   // Columna B (índice 1) -> columna "precio"
        mapeoArchivo2.put(2, "cantidad"); // Columna C (índice 2) -> columna "cantidad"

        // Rutas de los archivos Excel y sus mapeos correspondientes
        String[] archivosExcel = {
            "archivos_excel/archivo1.xlsx",
            "archivos_excel/archivo2.xlsx"
        };

        String[] nombresTablas = {
            "tabla1",
            "tabla2"
        };

        @SuppressWarnings("unchecked")
        Map<Integer, String>[] mapeosColumnas = new Map[] {
            mapeoArchivo1,
            mapeoArchivo2
        };

        // Cargar cada archivo Excel en su respectiva tabla
        for (int i = 0; i < archivosExcel.length; i++) {
            cargarExcelABaseDeDatos(archivosExcel[i], nombresTablas[i], mapeosColumnas[i]);
        }
    }
}