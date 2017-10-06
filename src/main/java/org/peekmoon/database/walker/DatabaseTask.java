package org.peekmoon.database.walker;

import java.sql.Connection;
import java.sql.SQLException;

public interface DatabaseTask {
	
	public void process(Connection conn, Row row) throws SQLException;
	
	public void process(Connection conn, Fragment fragment) throws SQLException;
}
