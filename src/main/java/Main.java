package main.java;

import javax.swing.SwingUtilities;

public class Main {
    public static void main(String[] args) {
        SwingUtilities.invokeLater(() -> {
            new CargadorDatosGUI().setVisible(true);
        });
    }
}