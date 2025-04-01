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
    private JComboBox<String> comboOpciones;

    public CargadorDatosGUI() {
        initialize();
    }

    private void initialize() {
        setTitle("Cargador de Datos SIP");
        setSize(600, 500);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout());
        Color azulOscuro = new Color(6, 20, 86); 
        Color blanco = Color.WHITE;

        LocalDate fechaActual = LocalDate.now();
        int anioActual = fechaActual.getYear();
        int mesActual = fechaActual.getMonthValue()-1;

        getRootPane().setBorder(BorderFactory.createLineBorder(azulOscuro, 4));

        JPanel panelConfig = new JPanel(new GridLayout(3, 2, 5, 5));
        panelConfig.setBackground(azulOscuro);
        panelConfig.setBorder(BorderFactory.createTitledBorder(
            BorderFactory.createLineBorder(blanco), 
            "Configuracion", 
            TitledBorder.LEFT, 
            TitledBorder.TOP, 
            new Font("Arial", Font.BOLD, 12), 
            blanco 
        ));

        
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

        JLabel lblOpcion = new JLabel("Opcion:");
        lblOpcion.setForeground(blanco);
        panelConfig.add(lblOpcion);

        String[] opciones = {
            "Todos los datos",
            "IPM (Indice de Precios al Mayoreo)",
            "IPP (Indice de Precios al Productor)",
            "Cobertura de Fuentes",
            "Precios Promedio IPC",
            "IPMC (Indice de Precios Materiales Construccion)",
            "Indices y ponderaciones",
            "Fuentes"
        };
        comboOpciones = new JComboBox<>(opciones);
        panelConfig.add(comboOpciones);

        add(panelConfig, BorderLayout.NORTH);

        JPanel panelBotones = new JPanel(new FlowLayout());
        panelBotones.setBackground(azulOscuro);

        btnLimpiar = new JButton("Limpiar Datos");
        btnCargar = new JButton("Cargar Datos");

        panelBotones.add(btnLimpiar);
        panelBotones.add(btnCargar);
        add(panelBotones, BorderLayout.SOUTH);

        txtLog = new JTextArea();
        txtLog.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(txtLog);
        add(scrollPane, BorderLayout.CENTER);

        btnLimpiar.addActionListener(e -> limpiarDatos());
        btnCargar.addActionListener(e -> cargarDatos());
    }

    private void limpiarDatos() {
        new Thread(() -> {
            SwingUtilities.invokeLater(() -> txtLog.setText("=== Nuevo proceso de limpieza iniciado ===\n")); 
            try {
                int anio = Integer.parseInt(txtAnio.getText());
                int mes = Integer.parseInt(txtMes.getText());
                String opcionSeleccionada = (String) comboOpciones.getSelectedItem();
                
                appendLog("Iniciando limpieza para: " + opcionSeleccionada);
                
                try (Connection conexionDestino = ConexionBD.obtenerConexionDestino()) {
                    
                    switch (opcionSeleccionada) {
                        case "Todos los datos":
                            ProcesadorDatos.limpiarIndices(conexionDestino, anio, mes);
                            ProcesadorDatos.limpiarCoberturaFuentes(conexionDestino);
                            ProcesadorDatos.limpiarIPM(conexionDestino);
                            ProcesadorDatos.limpiarIPMC(conexionDestino);
                            ProcesadorDatos.limpiarIPP(conexionDestino);
                            ProcesadorDatos.limpiarFuentes(conexionDestino);
                            ProcesadorDatos.limpiarPrecios(conexionDestino, anio, mes);
                            appendLog("Limpieza completa de todos los datos");
                            break;
                            
                        case "IPM (Indice de Precios al Mayoreo)":
                            ProcesadorDatos.limpiarIPM(conexionDestino);
                            appendLog("Limpieza de IPM completada");
                            break;
                            
                        case "IPP (Indice de Precios al Productor)":
                            ProcesadorDatos.limpiarIPP(conexionDestino);
                            appendLog("Limpieza de IPP completada");
                            break;
                            
                        case "Cobertura de Fuentes":
                            ProcesadorDatos.limpiarCoberturaFuentes(conexionDestino);
                            appendLog("Limpieza de cobertura fuentes completada");
                            break;
                            
                        case "Precios Promedio IPC":
                            ProcesadorDatos.limpiarPrecios(conexionDestino, anio, mes);
                            appendLog("Limpieza de precios promedio completada");
                            break;
                            
                        case "IPMC (Indice de Precios Materiales Construccion)":
                            ProcesadorDatos.limpiarIPMC(conexionDestino);
                            appendLog("Limpieza de IPMC completada");
                            break;
                            
                        case "Indices y ponderaciones":
                            ProcesadorDatos.limpiarIndices(conexionDestino, anio, mes);
                            appendLog("Limpieza de indices y ponderaciones completada");
                            break;
                            
                        case "Fuentes":
                            ProcesadorDatos.limpiarFuentes(conexionDestino);
                            appendLog("Limpieza de fuentes completada");
                            break;
                    }
                    
                    appendLog("Limpieza completada exitosamente");
                }
            } catch (NumberFormatException ex) {
                appendLog("Error: Anio y mes deben ser números validos");
            } catch (SQLException ex) {
                appendLog("Error al limpiar datos: " + ex.getMessage());
            }
        }).start();
    }

    private void cargarDatos() {
        new Thread(() -> { 
            SwingUtilities.invokeLater(() -> txtLog.setText("=== Nuevo proceso de carga iniciado ===\n")); 
            try {
                int anio = Integer.parseInt(txtAnio.getText());
                int mes = Integer.parseInt(txtMes.getText());
                String opcionSeleccionada = (String) comboOpciones.getSelectedItem();
                
                appendLog("Iniciando carga para: " + opcionSeleccionada);
                
                try (Connection conexionOrigen = ConexionBD.obtenerConexionOrigen();
                     Connection conexionDestino = ConexionBD.obtenerConexionDestino()) {

                    switch (opcionSeleccionada) {
                        case "Todos los datos":
                            ProcesadorExcel.cargarDesdeExcel(conexionDestino);
                            ProcesadorDatos.cargarFuentes(conexionOrigen, conexionDestino);
                            ProcesadorDatos.cargarIndices(conexionOrigen, conexionDestino, anio, mes);
                            appendLog("Carga completa de todos los datos");
                            break;
                            
                        case "IPM (Indice de Precios al Mayoreo)":
                            ProcesadorExcel.cargarArchivoEspecifico(conexionDestino, "Base_IPM.xlsx");
                            appendLog("Carga de IPM completada");
                            break;
                            
                        case "IPP (Indice de Precios al Productor)":
                            ProcesadorExcel.cargarArchivoEspecifico(conexionDestino, "EMPRESAS_IPP.xlsx");
                            appendLog("Carga de IPP completada");
                            break;
                            
                        case "Cobertura de Fuentes":
                            ProcesadorExcel.cargarArchivoEspecifico(conexionDestino, "Regiones.xlsx");
                            appendLog("Carga de cobertura fuentes completada");
                            break;
                            
                        case "Precios Promedio IPC":
                            ProcesadorExcel.cargarArchivoEspecifico(conexionDestino, "Precios_promedio_IPC_x_mes_region.xlsx");
                            appendLog("Carga de precios promedio completada");
                            break;
                            
                        case "IPMC (Indice de Precios Materiales Construccion)":
                            ProcesadorExcel.cargarArchivoEspecifico(conexionDestino, "Base_IPMC.xlsx");
                            appendLog("Carga de IPMC completada");
                            break;
                            
                        case "Indices y ponderaciones":
                            ProcesadorDatos.cargarIndices(conexionOrigen, conexionDestino, anio, mes);
                            appendLog("Carga de indices y ponderaciones completada");
                            break;
                            
                        case "Fuentes":
                            ProcesadorDatos.cargarFuentes(conexionOrigen, conexionDestino);
                            appendLog("Carga de fuentes completada");
                            break;
                    }
                    
                    appendLog("Proceso completado exitosamente");
                }
            } catch (NumberFormatException ex) {
                appendLog("Error: Anio y mes deben ser numeros validos");
            } catch (Exception ex) {
                appendLog("Error al cargar datos: " + ex.getMessage());
                ex.printStackTrace();
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