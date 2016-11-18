package org.peekmoon.database.walker.schema;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Do not support clob too long as clob value is stored in memory
 * but usable in majority of case
 */
public class CustomClob {
	
	private final String value;
	
	public CustomClob(Clob clob) throws SQLException {
		int length = (int)clob.length();
		if (length != 0) {
			this.value = clob.getSubString(1, length);
		} else {
			value = "";
		}
	}
	
	public Clob asClob(Connection conn) throws SQLException {
		Clob clob = conn.createClob();
		clob.setString(1, value);
		return clob;
	}
	

}
