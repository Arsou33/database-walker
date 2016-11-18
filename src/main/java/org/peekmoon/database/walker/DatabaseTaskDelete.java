package org.peekmoon.database.walker;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;
import java.util.stream.Collectors;

import org.peekmoon.database.walker.schema.Table;

public class DatabaseTaskDelete {
	
	public void delete(Connection conn, Fragment fragment) throws SQLException {
		for (Set<Row> partition : fragment.getDeleteOrderedPartitions()) {
			for (Row row : partition) {
				delete(conn, row);
			}
		}		
	}


	public void delete(Connection conn, Row row) throws SQLException {
		String sql = getSqlDelete(row.getTable());
		// TODO : mutualize preparedStatement
		try (PreparedStatement stmt =  conn.prepareStatement(sql)) {
			System.out.println(sql);
			
			int i=1;
			for (Object value : row.getPrimaryKeyValue().list()) {
				stmt.setObject(i++, value);
			}
			int result = stmt.executeUpdate();
			if (result != 1) {
				conn.rollback();
				throw new IllegalStateException(sql + " had deleted " + result + " row(s)");
			}
		}
		
	}
	
	
	public String getSqlDelete(Table table) {
		String whereClause;
		if (table.getPrimaryKey() != null) {
			whereClause = table.getPrimaryKey().getSqlWhere();
		} else {
			whereClause = table.getColumnNames().stream().map(columnName->columnName+"=?").collect(Collectors.joining(" AND ", " WHERE ", ""));
		}
		return "DELETE FROM " + table.getName() + whereClause;
	}
	

}
