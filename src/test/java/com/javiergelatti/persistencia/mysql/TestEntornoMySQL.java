package com.javiergelatti.persistencia.mysql;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.javiergelatti.persistencia.mysql.BaseDeDatosMySQL.ErrorBD;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;

public class TestEntornoMySQL {
    private static final String url = "jdbc:mysql://localhost/prueba";
    private static final String usr = "prueba";
    private static final String pwd = "prueba";
    private static BaseDeDatosMySQL bd = new BaseDeDatosMySQL(url, usr, pwd);

    @Test
    public void laUnicaFormaDeQueNoSeEjecutenLasPruebasEsSiMySqlNoEstaIniciado() {
        try {
            bd.conectar();
        } catch (ErrorBD e) {
            Throwable causa = e.getCause();
            assertTrue(causa instanceof CommunicationsException);
        }
    }

}
