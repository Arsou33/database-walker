package com.onepoint.database.walker;

import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import com.onepoint.database.walker.schema.CustomClob;
import com.onepoint.database.walker.schema.Table;

public class DatabaseTaskInsert {

	public void insert(Connection conn, Fragment fragment) throws SQLException {
		for (Set<Row> partition : fragment.getInsertOrderedPartitions()) {
			if (partition.size()>1) throw new IllegalStateException("Row graph is not acyclic : " + partition);
			for (Row row : partition) {
				insert(conn, row);
			}
		}		
	}

	private void insert(Connection conn, Row row) throws SQLException {
		Table table = row.getTable();
		String sql = table.getSqlInsert();
		// TODO : mutualize preparedStatement
		List<Clob> clobs = new LinkedList<>();
		try (PreparedStatement stmt =  conn.prepareStatement(sql)) {
			System.out.println(sql);
			
			for (int i=0; i<row.getValues().size(); i++) {
				
				Object value = row.getValue(i);
				
				if (value instanceof CustomClob) {
					Clob clob = ((CustomClob)value).asClob(conn);
					clobs.add(clob);
					value = clob;
				}

				stmt.setObject(i+1, value);
			}		
			stmt.executeUpdate();
		} 
		finally {
			for (Clob clob : clobs) clob.free();
		}
	}
}
