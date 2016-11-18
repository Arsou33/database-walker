package com.onepoint.database.walker.schema;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Schema {
	
	private final SchemaFilter filter;
	private final List<Table> tables = new ArrayList<>();
	/**
	 * All Foreign keys of users
	 */
	private final Set<ForeignKey> fks = new HashSet<>(); 
	/** 
	 * List of foreignKey referencing primaryKey
	 */
	private final Map<PrimaryKey, List<ForeignKey>> fksByPk = new HashMap<>(); 
	
	private static String sqlTable = ""
			+ " SELECT tbl.table_name, col.column_name"
			+ " FROM user_tables tbl"
			+ " JOIN user_tab_cols col ON col.table_name = tbl.table_name"
			+ " WHERE virtual_column = 'NO' AND user_generated= 'YES'"
			+ " ORDER BY tbl.table_name, col.column_id" ;
	
	private static String sqlPrimaryKey = ""
			+ " SELECT cons.constraint_name, cols.table_name, cols.column_name, cols.position, cols_det.data_type"
			+ " FROM user_constraints  cons"
			+ " JOIN user_cons_columns cols ON cols.constraint_name = cons.constraint_name"
			+ " JOIN  user_tab_columns cols_det on cols_det.table_name = cols.table_name AND cols_det.column_name = cols.column_name"
			+ " WHERE cons.constraint_type = 'P'"
			+ " ORDER BY cols.table_name, cols.position"
			;   
	
	
	private static String sqlForeignKey = ""
			+ "	SELECT" 
			+ "	  c.constraint_name, c.r_constraint_name,"
			+ "	  a.table_name, a.column_name, a.position"
			+ "	FROM user_constraints c"
			+ "	JOIN user_cons_columns a ON a.constraint_name = c.constraint_name"
			+ "	WHERE c.constraint_type = 'R'"
			+ "	ORDER BY a.constraint_name, a.position"
			;
	

	
	public Schema(Connection conn, SchemaFilter filter) {
		this.filter = filter;
		String url = "unknown";
		try {
			url = conn.getMetaData().getURL();
			System.out.println("Reading tables...");
			fillTables(conn);
			System.out.println("Reading primary keys...");
			fillPrimaryKeys(conn);
			System.out.println("Reading foreign keys...");
			fillForeignKeys(conn);
			System.out.println("Read schema OK : ");
			tables.stream().forEach(table -> System.out.println(" " + table.getName()));
		} catch (SQLException e) {
			throw new IllegalStateException("Unable to extract references for " + url, e);
		}		
	}

	public List<Table> getTables() {
		return Collections.unmodifiableList(tables);
	}

	public Table getTable(String tableName) {
		for (Table table : tables) {
			if (table.is(tableName)) return table;
		}
		throw new IllegalStateException("Table not found " + tableName);
	}
	
	public int getTableIdx(Table table) {
		return tables.indexOf(table);
	}
	
	public List<ForeignKey> getFkList(PrimaryKey pk) {
		if (pk==null) return Collections.emptyList();
		List<ForeignKey> fks = fksByPk.get(pk);
		if (fks == null) fksByPk.put(pk, fks = new ArrayList<>());
		return fks;
	}

	private void fillTables(Connection conn) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			stmt.executeQuery(sqlTable);
			ResultSet results = stmt.getResultSet();
			Table table = new Table(this, "DUMMY ");
			while (results.next()) {
				String tableName = results.getString("TABLE_NAME");
				String columnName = results.getString("COLUMN_NAME");
				if (filter.ignoreColumn(tableName, columnName)) continue;
				if (!table.is(tableName)) {
					tables.add(table = new Table(this, tableName));
				}
				table.addColumn(columnName);
			}
		}
	}
	
	private void fillPrimaryKeys(Connection conn) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			stmt.executeQuery(sqlPrimaryKey);
			ResultSet results = stmt.getResultSet();
			Table table = null;
			while (results.next()) {
				if (results.getInt("POSITION") == 1) {
					table = getTable(results.getString("TABLE_NAME"));
					table.addPrimaryKey(results.getString("CONSTRAINT_NAME"));
				}
				table.addPkColumn(results.getString("COLUMN_NAME"));
			}
		}
	}

	private void fillForeignKeys(Connection conn) throws SQLException {
		try (Statement stmt = conn.createStatement()) {
			stmt.executeQuery(sqlForeignKey);
			ResultSet results = stmt.getResultSet();
			
			PrimaryKey pk = null;
			ForeignKey fk = null;
			while (results.next()) {
				if (results.getInt("POSITION") == 1) {
					String fkName = results.getString("CONSTRAINT_NAME");
					if (filter.ignoreForeignKey(fkName)) continue;
					pk = getPk(results.getString("R_CONSTRAINT_NAME"));
					fk = new ForeignKey(fkName,	getTable(results.getString("TABLE_NAME")), pk);
					fks.add(fk);
					getFkList(pk).add(fk);
				} 
				fk.addColumn(results.getString("COLUMN_NAME"));
			}
			
		} 
	}

	private PrimaryKey getPk(String pkName) {
		for (Table table : tables) {
			if (table.havePrimaryKey(pkName)) return table.getPrimaryKey();
		}
		throw new IllegalStateException("Unable to find pk for table " + pkName);
	}

}
