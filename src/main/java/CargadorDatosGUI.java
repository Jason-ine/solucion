package main.java;

import javax.swing.*;
import javax.swing.border.TitledBorder;
import java.awt.*;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.*;

public class CargadorDatosGUI extends JFrame {
    private JTextField txtAnio;
    private JTextField txtMes;
    private JButton btnLimpiar;
    private JButton btnCargar;
    private JTextArea txtLog;
    private JComboBox<String> comboOpciones;
    private JButton btnIniciarProgramacion;
    private JButton btnDetenerProgramacion;
    private ScheduledExecutorService scheduler;
    @SuppressWarnings("unused")
    private boolean programacionActiva = false;
    private static final Logger logger = Logger.getLogger(CargadorDatosGUI.class.getName());

    static {
        configureLogger();
    }

    private static void configureLogger() {
        try {
            Handler fileHandler = new FileHandler("SIPLoader.log", true);
            fileHandler.setFormatter(new SimpleFormatter());
            logger.addHandler(fileHandler);
            logger.setLevel(Level.ALL);
        } catch (IOException e) {
            System.err.println("Error configurando logger: " + e.getMessage());
        }
    }

    public CargadorDatosGUI(boolean autoStart) {
        initialize();
        if(autoStart) {
            iniciarProgramacionAutomatica();
        }
    }

    private void initialize() {
        setTitle("Cargador de Datos SIP");
        setSize(800, 500);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLayout(new BorderLayout());
        Color azulOscuro = new Color(6, 20, 86); 
        Color blanco = Color.WHITE;

        LocalDate fechaActual = LocalDate.now();
        int anioActual = fechaActual.getYear();
        int mesActual = fechaActual.getMonthValue();

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
            "Fuentes",
            "Precios recolectados en el mes"
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

        JPanel panelProgramacion = new JPanel(new FlowLayout());
        panelProgramacion.setBackground(azulOscuro);

        btnIniciarProgramacion = new JButton("Iniciar Programacion Automatica");
        btnIniciarProgramacion.addActionListener(e -> iniciarProgramacionAutomatica());
        
        btnDetenerProgramacion = new JButton("Detener Programacion");
        btnDetenerProgramacion.addActionListener(e -> detenerProgramacion());
        btnDetenerProgramacion.setEnabled(false);

        panelProgramacion.add(btnIniciarProgramacion);
        panelProgramacion.add(btnDetenerProgramacion);
        panelBotones.add(panelProgramacion);

        add(panelBotones, BorderLayout.SOUTH);

        txtLog = new JTextArea();
        txtLog.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(txtLog);
        add(scrollPane, BorderLayout.CENTER);

        btnLimpiar.addActionListener(e -> limpiarDatos());
        btnCargar.addActionListener(e -> cargarDatos());
    }

    private void iniciarProgramacionAutomatica() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
        }

        scheduler = Executors.newScheduledThreadPool(4);
        
        programacionActiva = true;
        btnIniciarProgramacion.setEnabled(false);
        btnDetenerProgramacion.setEnabled(true);
        programarTareaDiaria(1, 55, this::ejecutarLimpiezaPrecios, "Limpieza de Precios");
        programarTareaDiaria(2, 0, this::ejecutarCargaPrecios, "Carga de Precios");
        programarTareaDiaria(3, 5, this::ejecutarLimpiezaFuentes, "Limpieza de Fuentes");
        programarTareaDiaria(3, 10, this::ejecutarCargaFuentes, "Carga de Fuentes");
        programarTareaDiaria(3, 15, this::ejecutarLimpiezaIndices, "Limpieza de Indices");
        programarTareaDiaria(3, 20, this::ejecutarCargaIndices, "Carga de Indices");

        appendLog("Programacion automatica INICIADA con horarios fijos:");
        appendLog("1:55 - Limpiar precios");
        appendLog("2:00 - Carga precios");
        appendLog("3:05 - Limpiar fuentes");
        appendLog("3:10 - Cargar fuentes");
        appendLog("3:15 - Limpiar Indices y ponderaciones");
        appendLog("3:20 - Cargar Indices y ponderaciones");
    }

    private void detenerProgramacion() {
        if (scheduler != null && !scheduler.isShutdown()) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        programacionActiva = false;
        btnIniciarProgramacion.setEnabled(true);
        btnDetenerProgramacion.setEnabled(false);
        appendLog("Programacion automatica DETENIDA");
    }

    private void programarTareaDiaria(int hora, int minuto, Runnable tarea, String nombreTarea) {
        LocalTime horaTarea = LocalTime.of(hora, minuto);
        LocalTime ahora = LocalTime.now();
        
        long delayInicial;
        
        if (ahora.isBefore(horaTarea)) {
            delayInicial = ahora.until(horaTarea, ChronoUnit.MINUTES);
        } else {
            delayInicial = ahora.until(horaTarea, ChronoUnit.MINUTES) + TimeUnit.DAYS.toMinutes(1);
        }
        
        long delayInicialMs = TimeUnit.MINUTES.toMillis(delayInicial);
        
        scheduler.scheduleAtFixedRate(() -> {
            appendLog("EJECUTANDO TAREA PROGRAMADA: " + nombreTarea + " a las " + horaTarea);
            tarea.run();
        }, delayInicialMs, TimeUnit.DAYS.toMillis(1), TimeUnit.MILLISECONDS);
    }

    private void ejecutarLimpiezaPrecios() {
        SwingUtilities.invokeLater(() -> {
            comboOpciones.setSelectedItem("Precios recolectados en el mes");
            limpiarDatos();
        });
    }

    private void ejecutarCargaPrecios() {
        SwingUtilities.invokeLater(() -> {
            comboOpciones.setSelectedItem("Precios recolectados en el mes");
            cargarDatos();
        });
    }

    private void ejecutarLimpiezaFuentes() {
        SwingUtilities.invokeLater(() -> {
            comboOpciones.setSelectedItem("Fuentes");
            limpiarDatos();
        });
    }

    private void ejecutarCargaFuentes() {
        SwingUtilities.invokeLater(() -> {
            comboOpciones.setSelectedItem("Fuentes");
            cargarDatos();
        });
    }

    private void ejecutarLimpiezaIndices() {
        SwingUtilities.invokeLater(() -> {
            comboOpciones.setSelectedItem("Indices y ponderaciones");
            txtAnio.setText(String.valueOf(LocalDate.now().getYear()));
            txtMes.setText(String.valueOf(LocalDate.now().getMonthValue()));
            limpiarDatos();
        });
    }

    private void ejecutarCargaIndices() {
        SwingUtilities.invokeLater(() -> {
            comboOpciones.setSelectedItem("Indices y ponderaciones");
            txtAnio.setText(String.valueOf(LocalDate.now().getYear()));
            txtMes.setText(String.valueOf(LocalDate.now().getMonthValue()));
            cargarDatos();
        });
    }

    private void limpiarDatos() {
        new Thread(() -> {
            SwingUtilities.invokeLater(() -> txtLog.append("=== Nuevo proceso de limpieza iniciado ===\n")); 
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
                            ProcesadorDatos.limpiarPreciosRecolectado(conexionDestino,anio,mes);
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
                        case "Precios recolectados en el mes":
                            ProcesadorDatos.limpiarPreciosRecolectado(conexionDestino, anio, mes);
                            appendLog("Limpieza de precios recolectados completada");
                            break;
                    }
                    
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
            SwingUtilities.invokeLater(() -> txtLog.append("=== Nuevo proceso de carga iniciado ===\n")); 
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
                            ProcesadorDatos.cargarPrecios(conexionOrigen, conexionDestino, anio, mes);
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
                        case "Precios recolectados en el mes":
                            ProcesadorDatos.cargarPrecios(conexionOrigen, conexionDestino, anio, mes);
                            appendLog("Carga de precios recolectados completada");
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

    @Override
    public void dispose() {
        detenerProgramacion();
        super.dispose();
    }

    public static void main(String[] args) {
        // Configurar manejo global de errores
        Thread.setDefaultUncaughtExceptionHandler((thread, ex) -> {
            logger.log(Level.SEVERE, "Error no capturado en hilo: " + thread.getName(), ex);
        });

        // Determinar modo de inicio
        boolean autoStart = !System.getProperty("sun.java.command", "").contains("--manual");
        
        SwingUtilities.invokeLater(() -> {
            try {
                CargadorDatosGUI gui = new CargadorDatosGUI(autoStart);
                gui.setVisible(true);
                logger.info("Aplicacion iniciada" + (autoStart ? " en modo automatico" : ""));
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error al iniciar aplicación", e);
                JOptionPane.showMessageDialog(null, 
                    "Error critico al iniciar: " + e.getMessage(), 
                    "Error", 
                    JOptionPane.ERROR_MESSAGE);
                System.exit(1);
            }
        });
    }
}