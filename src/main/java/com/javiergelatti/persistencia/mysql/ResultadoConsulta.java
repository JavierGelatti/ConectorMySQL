package com.javiergelatti.persistencia.mysql;

import java.sql.ResultSet;
import java.sql.SQLException;

public class ResultadoConsulta {

	private ResultSet resultadoPosta;

	public ResultadoConsulta(ResultSet resultadoPosta) {
		this.resultadoPosta = resultadoPosta;
	}

	public void forEach(ConsumidorRegistro bloque) {
		try {
			while (resultadoPosta.next()) {
				bloque.consumir(resultadoPosta);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
	
	public static interface ConsumidorRegistro {
		void consumir(ResultSet registro) throws SQLException;
	}

	public boolean estaVacio() {
		try {
			return !resultadoPosta.first();
		} catch (SQLException e) {
			return true;
		}
	}
}
