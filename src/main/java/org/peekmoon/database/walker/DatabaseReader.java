package org.peekmoon.database.walker;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.peekmoon.database.walker.schema.CustomBlob;
import org.peekmoon.database.walker.schema.CustomClob;
import org.peekmoon.database.walker.schema.ForeignKey;
import org.peekmoon.database.walker.schema.Key;
import org.peekmoon.database.walker.schema.Schema;
import org.peekmoon.database.walker.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseReader {
	
	private final static Logger log = LoggerFactory.getLogger(DatabaseReader.class);

	private final Schema schema;
	
	public DatabaseReader(Schema schema) {
		this.schema = schema;
	}
	
	/**
	 * Provide all lines that depends on row table/keyValue
	 * 
	 * @param conn
	 * @param table
	 * @param keyValue
	 * @return la liste des lignes dépendantes
	 * @throws SQLException
	 */
	public Fragment extract(Connection conn, Table table, KeyValue keyValue) throws SQLException {
		Fragment extractedRows = new Fragment(table.getSchema());
		List<Row> firstRow = read(conn, table.getPrimaryKey(), keyValue);
		if (firstRow.size() != 1) throw new IllegalStateException("First row for " + table + keyValue + " return : " + firstRow.size());
		parcours(conn, firstRow.get(0), 0, extractedRows);
		return extractedRows;
	}

	
	// TODO : mutualize preparedStatement ?
	private void parcours(Connection conn, Row row, int niveau, Fragment fragment) throws SQLException  {
		niveau++;
		if (log.isDebugEnabled()) {
			StringBuilder logString = new StringBuilder();
			for (int i=0; i<niveau; i++) logString.append("-");
			logString.append('>').append(row);
			log.debug(logString.toString());
		}
		
		fragment.add(row);
		for (ForeignKey fk : schema.getFkList(row.getPrimaryKey())) {
			List<Row> rows = read(conn, fk, row.getPrimaryKeyValue());
			for (Row childRow : rows) {
				if (!fragment.contains(childRow)) {
					parcours(conn, childRow, niveau, fragment);
					childRow.addParent(row);
					fragment.add(childRow, row);
				}
				else {
					Optional<Row> optExistingChildRow = fragment.getRows().stream().filter(r -> r.equals(childRow)).findFirst();
					if (optExistingChildRow.isPresent()) {
						Row existingChildRow = optExistingChildRow.get();
						existingChildRow.addParent(row);
						fragment.add(existingChildRow, row);
					}
				}
			}
		}		
	}

	private List<Row> read(Connection conn, Key key, KeyValue values) throws SQLException {
		int i=1;
		try (PreparedStatement stmt = conn.prepareStatement( key.getSqlSelect())) {
			for (Object value : values.list()) {
				stmt.setObject(i++, value);
			}
			ResultSet resultSet = stmt.executeQuery();
			List<Row> rows = new ArrayList<>();
			while (resultSet.next()) {
				Row row = new Row(key.getTable(), resultSet.getRowId("ROWID"));
				for (int noCol=2; noCol<=resultSet.getMetaData().getColumnCount(); noCol++) {
					Object columnData = resultSet.getObject(noCol);
					
					if (columnData instanceof Clob) {
						columnData = new CustomClob(resultSet.getClob(noCol));
					} else if (columnData instanceof Blob) {
						columnData = new CustomBlob(resultSet.getBlob(noCol));
					}
					row.add(columnData);
				}
				rows.add(row);
			}
			return rows;
		}
	}
	

}
