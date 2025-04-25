package main.java;

import javax.swing.SwingUtilities;

public class Main {
    public static void main(String[] args) {
        boolean autoStart = !System.getProperty("sun.java.command", "").contains("--manual");
        
        SwingUtilities.invokeLater(() -> {
            new CargadorDatosGUI(autoStart).setVisible(true); 
        });
    }
}