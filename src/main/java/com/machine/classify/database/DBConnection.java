package com.machine.classify.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class DBConnection {
	private static final Log LOG = LogFactory.getLog(DBConnection.class);

	private static Connection connection;
	public static Connection getConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			try {
				connection = DriverManager.getConnection(
						"jdbc:mysql://localhost:3306/classify", "root", "root");
			} catch (SQLException e) {
				LOG.error("class not loaded", e);
			}
		} catch (ClassNotFoundException e) {
			LOG.error("not able to get  Connection", e);
		}

		return connection;
	}
}
