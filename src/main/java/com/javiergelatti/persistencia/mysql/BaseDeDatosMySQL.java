package com.javiergelatti.persistencia.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Stack;

public class BaseDeDatosMySQL {

    private Connection conexion;
    private String urlBD;
    private String usuario;
    private String pass;

    private Stack<Savepoint> sps;
    private int ultimoId;

    public BaseDeDatosMySQL(String urlBD, String usuario, String pass) {
        this.urlBD = urlBD;
        this.usuario = usuario;
        this.pass = pass;

        this.sps = new Stack<Savepoint>();
    }

    public void conectar() {
        if (estáConectada()) return;

        try {
            iniciarConexion();
        } catch (SQLException e) {
            desconectar();
            throw new ErrorBD(e);
        } catch (ClassNotFoundException e) {
            throw new ErrorBD(e);
        }
    }

    private void iniciarConexion() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        conexion = DriverManager.getConnection(urlBD, usuario, pass);
    }
    
    public boolean estáConectada() {
        try {
            return (conexion != null && !conexion.isClosed());
        } catch (SQLException e) {
            return false;
        }
    }

    public PreparedStatement prepararSentencia(String sql) {
        conectar();
        try {
		    return conexion.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		} catch (SQLException e) {
		    throw new ErrorBD(e);
		}
    }

    public void ejecutar(String sql) {
        conectar();
        try (PreparedStatement stm = conexion.prepareStatement(sql)) {
            stm.executeUpdate();
        } catch (SQLException e) {
            throw new ErrorBD(e);
        }
    }
    
    public void ejecutarYCapturarId(String sql) {
        conectar();
        try (PreparedStatement stm = prepararStatementConCapturaDeId(sql)) {
            stm.executeUpdate();
            capturarId(stm);
        } catch (SQLException e) {
            throw new ErrorBD(e);
        }
    }

    private void capturarId(PreparedStatement stm) throws SQLException {
        try (ResultSet resultSetId = stm.getGeneratedKeys()) {
            resultSetId.next();
            ultimoId = resultSetId.getInt(1);
        }
    }

    private PreparedStatement prepararStatementConCapturaDeId(String sql) throws SQLException {
        return conexion.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
    }

    public ResultSet ejecutarConsulta(String sql) {
        conectar();
        try {
            PreparedStatement stm = prepararStatementConCapturaDeId(sql);
            return stm.executeQuery();
        } catch (SQLException e) {
            throw new ErrorBD(e);
        }
    }
    
    public int getUltimoId() {
        return ultimoId;
    }

    public void desconectar() {
        if (!estáConectada()) return;

        try {
            conexion.close();
            conexion = null;
        } catch (SQLException e) {
            throw new ErrorBD(e);
        }
    }

    public void iniciarTransacción() {
        conectar();
        try {
            conexion.setAutoCommit(false);
            Savepoint sp = conexion.setSavepoint();
            sps.push(sp);
        } catch (SQLException e) {
            throw new ErrorBD(e);
        }
    }

    public void finalizarTransacción() {
        try {
            if (sps.isEmpty()) {
                conexion.commit();
                conexion.setAutoCommit(true);
                return;
            }

            Savepoint sp = sps.pop();
            conexion.releaseSavepoint(sp);

            if (sps.isEmpty()) {
                finalizarTransacción();
            }
        } catch (SQLException e) {
            throw new ErrorBD(e);
        }
    }

    public void deshacerTransacción() {
        try {
            if (sps.isEmpty()) {
                conexion.rollback();
                conexion.setAutoCommit(true);
                return;
            }

            Savepoint sp = sps.pop();
            conexion.rollback(sp);

            if (sps.isEmpty()) {
                deshacerTransacción();
            }
        } catch (SQLException e) {
            throw new ErrorBD(e);
        }
    }

    public static class ErrorBD extends RuntimeException {
        public ErrorBD(Throwable causa) {
            super(causa);
        }
    }
}