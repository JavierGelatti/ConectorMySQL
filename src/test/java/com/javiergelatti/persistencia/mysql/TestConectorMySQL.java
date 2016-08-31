package com.javiergelatti.persistencia.mysql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.javiergelatti.persistencia.mysql.BaseDeDatosMySQL.ErrorBD;

public class TestConectorMySQL {

    private static final String url = "jdbc:mysql://localhost/prueba";
    private static final String usr = "prueba";
    private static final String pwd = "prueba";
    private static BaseDeDatosMySQL bd = new BaseDeDatosMySQL(url, usr, pwd);

    private final static String sqlCrearTabla = "CREATE TABLE IF NOT EXISTS `Prueba`"
        + "(`id` int(11) NOT NULL AUTO_INCREMENT,"
        + "PRIMARY KEY (`id`)) AUTO_INCREMENT=1";
    private final static String sqlEliminarTabla = "DROP TABLE IF EXISTS `Prueba`";

    @BeforeClass
    public static void setUpClass() throws Exception {
        try {
            bd.conectar();
        } catch (ErrorBD e) {
            e.printStackTrace();
        }

        Assume.assumeTrue(bd.estáConectada());
    }

    @Before
    public void setUp() throws Exception {
        bd.conectar();
    }
    
    @After
    public void tearDown() throws Exception {
        eliminarTabla();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        bd.desconectar();
    }

    @Test
    public void luegoDeCrearse_NoEstáConectada() throws Exception {
        bd = new BaseDeDatosMySQL(url, usr, pwd);

        assertFalse(bd.estáConectada());
    }

    @Test
    public void siSeDesonectó_NoEstáConectada() throws Exception {
        bd.desconectar();

        assertFalse(bd.estáConectada());
    }

    @Test
    public void luegoDeConectarse_EstáConectada() throws Exception {
        bd = new BaseDeDatosMySQL(url, usr, pwd);
        
        bd.conectar();
        
        assertTrue(bd.estáConectada());
    }

    @Test
    public void sePuedeEjecutarSQL() throws Exception {
        bd.ejecutar("CREATE TABLE IF NOT EXISTS `Prueba`"
            + "(`id` int(11) NOT NULL AUTO_INCREMENT,"
            + "PRIMARY KEY (`id`)) AUTO_INCREMENT=1");
        bd.ejecutar("DROP TABLE Prueba");
    }
    
    @Test
    public void siEstaDesconectada_SeConectaCuandoSeEjecutaSQL() throws Exception {
        bd.desconectar();
        
        bd.ejecutar("SHOW TABLES");
        
        assertTrue(bd.estáConectada());
    }

    @Test
    public void siSeGuardanDatos_SePuedenObtener() throws Exception {
        crearTabla();
        int id = 5;

        String sqlInsert = "INSERT INTO Prueba (id) VALUES (" + id + ")";
        String sqlSelect = "SELECT * FROM Prueba";
        bd.ejecutar(sqlInsert);
        
		ResultadoConsulta rs = bd.ejecutarConsulta(sqlSelect);
        assertFalse(rs.estaVacio());
        rs.forEach(registro -> {
        	assertEquals(id, registro.getInt("id"));
        });
    }

    @Test
    public void siSeIniciaYFinalizaUnaTransaccion_SeGuarda() throws Exception {
        crearTabla();
        int id = 5;

        String sqlInsert = "INSERT INTO Prueba (id) VALUES (" + id + ")";
        String sqlSelect = "SELECT * FROM Prueba";
        bd.iniciarTransacción();
        bd.ejecutar(sqlInsert);
        bd.finalizarTransacción();
        
		ResultadoConsulta rs = bd.ejecutarConsulta(sqlSelect);
        assertFalse(rs.estaVacio());
        rs.forEach(registro -> {
        	assertEquals(id, registro.getInt("id"));
        });
    }

    @Test
    public void siSeIniciaYDeshaceUnaTransaccion_NoSeGuarda() throws Exception {
        crearTabla();
        int id = 5;

        String sqlInsert = "INSERT INTO Prueba (id) VALUES (" + id + ")";
        String sqlSelect = "SELECT * FROM Prueba";
        bd.iniciarTransacción();
        bd.ejecutar(sqlInsert);
        bd.deshacerTransacción();
		ResultadoConsulta rs = bd.ejecutarConsulta(sqlSelect);
        boolean next = !rs.estaVacio();
		assertFalse(next);
    }

	@Test
    public void lasTransaccionesPuedenAnidarse() throws Exception {
        crearTabla();
        int id = 5;

        String sqlInsert = "INSERT INTO Prueba (id) VALUES (" + id + ")";
        String sqlSelect = "SELECT * FROM Prueba";
        bd.iniciarTransacción();
        bd.iniciarTransacción();
        bd.ejecutar(sqlInsert);
        bd.finalizarTransacción();
        bd.deshacerTransacción();
		ResultadoConsulta rs = bd.ejecutarConsulta(sqlSelect);
        assertTrue(rs.estaVacio());
    }
    
    @Test
    public void sePuedeObtenerElUltimoId() throws Exception {
        crearTabla();
        String sqlInsert = "INSERT INTO Prueba (id) VALUES (NULL)";
        String sqlSelect = "SELECT id FROM Prueba";

        bd.ejecutarYCapturarId(sqlInsert);
        ResultadoConsulta rs = bd.ejecutarConsulta(sqlSelect);
        rs.forEach(registro -> {
        	int id = registro.getInt(1);
        	assertEquals(id, bd.getUltimoId());
        });
    }
    
    @Test
    public void seObtieneElUltimoIdIngresado() throws Exception {
        crearTabla();
        String sqlInsert = "INSERT INTO Prueba (id) VALUES (NULL)";
        String sqlSelect = "SELECT LAST_INSERT_ID() FROM Prueba";
        
        bd.ejecutarYCapturarId(sqlInsert);
        bd.ejecutarYCapturarId(sqlInsert);
        bd.ejecutarYCapturarId(sqlInsert);

        ResultadoConsulta rs = bd.ejecutarConsulta(sqlSelect);
        rs.forEach(registro -> {
        	int id = registro.getInt(1);
        	assertEquals(id, bd.getUltimoId());
        });
    }

    @Test
    public void siNoSeInsertóNada_ElUltimoIdEsCero() throws Exception {
        BaseDeDatosMySQL bd = new BaseDeDatosMySQL(url, usr, pwd);
        assertEquals(0, bd.getUltimoId());
    }
    
    @Test
    public void sePuedeObtenerElUltimoIdMuchasVeces() throws Exception {
        crearTabla();
        String sqlInsert = "INSERT INTO Prueba (id) VALUES (NULL)";

        bd.ejecutarYCapturarId(sqlInsert);
        int id = bd.getUltimoId();

        assertEquals(id, bd.getUltimoId());
        assertEquals(id, bd.getUltimoId());
        assertEquals(id, bd.getUltimoId());
    }
    
    @Test
    public void sePuedeObtenerUnPreparedStatement() throws Exception {
        PreparedStatement sentencia = bd.prepararSentencia("SELECT * FROM prueba WHERE id = ?");
        Connection conexion = sentencia.getConnection();
        
        assertEquals(url, conexion.getMetaData().getURL());
    }
    
    @Test
    public void sePuedenObtenerLasClavesGeneradasAPartirDelStatement() throws Exception {
        crearTabla();
        String sqlInsert = "INSERT INTO Prueba (id) VALUES (?)";
        PreparedStatement sentencia = bd.prepararSentencia(sqlInsert);
        sentencia.setNull(1, Types.INTEGER);
        
        sentencia.execute();
        ResultSet clavesGeneradas = sentencia.getGeneratedKeys();
        
        clavesGeneradas.next();
        assertEquals(1, clavesGeneradas.getInt(1));
    }
    
    @Test
    public void siNoSeGeneraronClaves_SeRetornaUnResultSetVacio() throws Exception {
        crearTabla();
        String sqlInsert = "SELECT * FROM Prueba";
        PreparedStatement sentencia = bd.prepararSentencia(sqlInsert);
        
        sentencia.execute();
        ResultSet clavesGeneradas = sentencia.getGeneratedKeys();
        
        assertFalse(clavesGeneradas.first());
    }
    
    @Test
    public void elErrorBdGuardaLaCausa() throws Exception {
        Throwable causa = new Exception();
        
        RuntimeException errorBd = new ErrorBD(causa);
        
        assertEquals(causa, errorBd.getCause());
    }
    
    @Test
	public void testX() throws Exception {
		crearTabla();
		String sqlSelect = "SELECT * FROM Prueba";
		String sqlInsert = "INSERT INTO Prueba (id) VALUES (NULL)";
		bd.ejecutar(sqlInsert);
		bd.ejecutar(sqlInsert);
		bd.ejecutar(sqlInsert);
		
		AtomicInteger id = new AtomicInteger(1);
		ResultadoConsulta resultado = bd.ejecutarConsulta(sqlSelect);
		resultado.forEach(registro -> {
			assertEquals(id.intValue(), registro.getInt(1));
			id.getAndIncrement();
		});
	}
    
    @Ignore
    @Test
    public void lasTablasSeBloqueanCorrectamente() throws Exception {
        crearTabla();

        BaseDeDatosMySQL bd1 = new BaseDeDatosMySQL(url, usr, pwd);
        final BaseDeDatosMySQL bd2 = new BaseDeDatosMySQL(url, usr, pwd);

        bd1.conectar();
        bd2.conectar();

        String sqlInsert = "INSERT INTO Prueba (id) VALUES (2)";
        final String sqlSelect = "SELECT * FROM Prueba FOR UPDATE";
        bd1.prepararSentencia(sqlInsert).executeUpdate();

        HiloEspía r = new HiloEspía(bd2, sqlSelect);

        bd1.iniciarTransacción();
        ejecutarConsulta(bd1, sqlSelect);
        r.start();
        r.join(70);
        assertFalse(r.ejecutado);
        bd1.finalizarTransacción();
    }

	private void ejecutarConsulta(BaseDeDatosMySQL bd1, final String sqlSelect) {
		bd1.ejecutarConsulta(sqlSelect);
	}

    private static void eliminarTabla() throws SQLException {
        bd.ejecutar(sqlEliminarTabla);
    }

    private static void crearTabla() throws SQLException {
        bd.ejecutar(sqlCrearTabla);
    }

    private class HiloEspía extends Thread {
        private BaseDeDatosMySQL bd;
        private String sql;
        public boolean ejecutado = false;

        private HiloEspía(BaseDeDatosMySQL bd, String sql) {
            this.bd = bd;
            this.sql = sql;
        }

        @Override
        public void run() {
            bd.ejecutarConsulta(sql);
            ejecutado = true;
        }
    }
}
