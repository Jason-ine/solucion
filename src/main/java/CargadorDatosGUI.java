package main.java;

import javax.swing.*;
import java.awt.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;

public class CargadorDatosGUI extends JFrame {
    private JTextField txtAnio;
    private JTextField txtMes;
    private JButton btnLimpiar;
    private JButton btnCargar;
    private JTextArea txtLog;

    public CargadorDatosGUI() {
        initialize();
    }

    private void initialize() {
        setTitle("Cargador de Datos SIP");
        setSize(400, 300);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout());

        // Obtener fecha actual
        LocalDate fechaActual = LocalDate.now();
        int anioActual = fechaActual.getYear();
        int mesActual = fechaActual.getMonthValue();


        // Panel de configuración
        JPanel panelConfig = new JPanel(new GridLayout(2, 2, 5, 5));
        panelConfig.setBorder(BorderFactory.createTitledBorder("Configuracion"));

        panelConfig.add(new JLabel("Anio:"));
        txtAnio = new JTextField(String.valueOf(anioActual));
        panelConfig.add(txtAnio);

        panelConfig.add(new JLabel("Mes:"));
        txtMes = new JTextField(String.valueOf(mesActual));
        panelConfig.add(txtMes);

        add(panelConfig, BorderLayout.NORTH);

        // Panel de botones
        JPanel panelBotones = new JPanel(new FlowLayout());
        btnLimpiar = new JButton("Limpiar Datos");
        btnCargar = new JButton("Cargar Datos");
        panelBotones.add(btnLimpiar);
        panelBotones.add(btnCargar);
        add(panelBotones, BorderLayout.SOUTH);

        // Área de log
        txtLog = new JTextArea();
        txtLog.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(txtLog);
        add(scrollPane, BorderLayout.CENTER);

        // Configurar acciones
        btnLimpiar.addActionListener(e -> limpiarDatos());
        btnCargar.addActionListener(e -> cargarDatos());
    }

    private void limpiarDatos() {
        new Thread(() -> {
            try {
                int anio = Integer.parseInt(txtAnio.getText());
                int mes = Integer.parseInt(txtMes.getText());
                
                appendLog("Iniciando limpieza de datos...");
                
                try (Connection conexionOrigen = ConexionBD.obtenerConexionOrigen();
                     Connection conexionDestino = ConexionBD.obtenerConexionDestino()) {
                    
                    ProcesadorDatos.limpiarIndices(conexionDestino, anio, mes);
                    appendLog("Limpieza de indices_ponderaciones completada");

                    ProcesadorDatos.limpiarCoberturaFuentes(conexionDestino);
                    appendLog("Limpieza de cobertura fuentes completada");

                    ProcesadorDatos.limpiarIPM(conexionDestino);
                    appendLog("Limpieza de IPM completada");

                    ProcesadorDatos.limpiarIPMC(conexionDestino);
                    appendLog("Limpieza de IPMC completada");

                    ProcesadorDatos.limpiarIPP(conexionDestino);
                    appendLog("Limpieza de IPP completada");
                    
                    ProcesadorDatos.limpiarFuentes(conexionDestino);
                    appendLog("Limpieza de fuentes completada");
                    
                    ProcesadorDatos.limpiarPrecios(conexionDestino, anio, mes);
                    appendLog("Limpieza de precios completada");
                    
                    appendLog("Limpieza completada exitosamente");
                }
            } catch (NumberFormatException ex) {
                appendLog("Error: Anio y mes deben ser numeros validos");
            } catch (SQLException ex) {
                appendLog("Error al limpiar datos: " + ex.getMessage());
            }
        }).start();
    }

    private void cargarDatos() {
        new Thread(() -> {
            try {
                int anio = Integer.parseInt(txtAnio.getText());
                int mes = Integer.parseInt(txtMes.getText());
                
                appendLog("Iniciando carga de datos...");
                
                try (Connection conexionOrigen = ConexionBD.obtenerConexionOrigen();
                     Connection conexionDestino = ConexionBD.obtenerConexionDestino()) {

                    appendLog("Cargando datos desde archivos Excel...");
                    ProcesadorExcel.cargarDesdeExcel(conexionDestino);
                    appendLog("Carga desde Excel completada");

                    appendLog("Cargando fuentes...");
                    ProcesadorDatos.cargarFuentes(conexionOrigen, conexionDestino);
                    appendLog("Carga de fuentes completada");

                    appendLog("Cargando índices y ponderaciones...");
                    ProcesadorDatos.cargarIndices(conexionOrigen, conexionDestino, anio, mes);
                    appendLog("Carga de indices completada");
                    
                    appendLog("Proceso completado exitosamente");
                }
            } catch (NumberFormatException ex) {
                appendLog("Error: Anio y mes deben ser numeros validos");
            } catch (Exception ex) {
                appendLog("Error al cargar datos: " + ex.getMessage());
            }
        }).start();
    }

    private void appendLog(String mensaje) {
        SwingUtilities.invokeLater(() -> {
            txtLog.append(mensaje + "\n");
            txtLog.setCaretPosition(txtLog.getDocument().getLength());
        });
    }
}