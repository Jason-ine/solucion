package main.java;

import javax.swing.*;
import javax.swing.border.TitledBorder;

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
    Color azulOscuro = new Color(6, 20, 86); // Color definido
    Color blanco = Color.WHITE;

    // Obtener fecha actual
    LocalDate fechaActual = LocalDate.now();
    int anioActual = fechaActual.getYear();
    int mesActual = fechaActual.getMonthValue();

    getRootPane().setBorder(BorderFactory.createLineBorder(azulOscuro, 4));

    // Panel de configuración - Fondo azul oscuro y texto blanco
    JPanel panelConfig = new JPanel(new GridLayout(2, 2, 5, 5));
    panelConfig.setBackground(azulOscuro);
    panelConfig.setBorder(BorderFactory.createTitledBorder(
        BorderFactory.createLineBorder(blanco), 
        "Configuracion", 
        TitledBorder.LEFT, 
        TitledBorder.TOP, 
        new Font("Arial", Font.BOLD, 12), 
        blanco // Texto blanco
    ));

    // Etiquetas (JLabel) en blanco
    JLabel lblAnio = new JLabel("Anio:");
    lblAnio.setForeground(blanco);
    panelConfig.add(lblAnio);

    txtAnio = new JTextField(String.valueOf(anioActual));
    panelConfig.add(txtAnio);

    JLabel lblMes = new JLabel("Mes:");
    lblMes.setForeground(blanco);
    panelConfig.add(lblMes);

    txtMes = new JTextField(String.valueOf(mesActual));
    panelConfig.add(txtMes);

    add(panelConfig, BorderLayout.NORTH);

    // Panel de botones - Fondo azul oscuro
    JPanel panelBotones = new JPanel(new FlowLayout());
    panelBotones.setBackground(azulOscuro);

    btnLimpiar = new JButton("Limpiar Datos");
    btnCargar = new JButton("Cargar Datos");

    panelBotones.add(btnLimpiar);
    panelBotones.add(btnCargar);
    add(panelBotones, BorderLayout.SOUTH);

    // Área de log (sin cambios)
    txtLog = new JTextArea();
    txtLog.setEditable(false);
    JScrollPane scrollPane = new JScrollPane(txtLog);
    add(scrollPane, BorderLayout.CENTER);

    // Configurar acciones (sin cambios)
    btnLimpiar.addActionListener(e -> limpiarDatos());
    btnCargar.addActionListener(e -> cargarDatos());
}

    private void limpiarDatos() {
        new Thread(() -> {
            SwingUtilities.invokeLater(() -> txtLog.setText("=== Nuevo proceso iniciado ===\n")); 
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
                    appendLog("Limpieza de precios promedio completada");
                    
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
            SwingUtilities.invokeLater(() -> txtLog.setText("=== Nuevo proceso iniciado ===\n")); 
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