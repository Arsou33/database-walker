package org.peekmoon.database.walker.schema;

import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Do not support blob too long as blob value is stored in memory
 * but usable in majority of case
 */
public class CustomBlob {
	
	private final byte[] value;
	
	// TODO : Check it is ok for null/empty value
	public CustomBlob(Blob blob) throws SQLException {
		int length = (int)blob.length();
		this.value = blob.getBytes(1, length);
	}
	
	public Blob asBlob(Connection conn) throws SQLException {
		Blob blob = conn.createBlob();
		blob.setBytes(1, value);
		return blob;
	}
	

}
