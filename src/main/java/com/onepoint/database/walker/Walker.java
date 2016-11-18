package com.onepoint.database.walker;

import java.sql.Connection;
import java.sql.SQLException;

import com.onepoint.database.walker.schema.KeyValue;
import com.onepoint.database.walker.schema.Table;

public class Walker {
	
	public Fragment extract(Connection conn, Table table, KeyValue... values) throws SQLException {
		DatabaseReader reader = new DatabaseReader(table.getSchema());
		Fragment fragment = new Fragment(table.getSchema());
		for (KeyValue value : values) {
			fragment.add(reader.extract(conn, table, value));
		}
		return fragment;
	}

	public void insert(Connection conn, Fragment fragment) throws SQLException {
		DatabaseTaskInsert inserter = new DatabaseTaskInsert();
		inserter.insert(conn, fragment);
		
	}

	public void delete(Connection conn, Fragment fragment) throws SQLException {
		DatabaseTaskDelete delete = new DatabaseTaskDelete();
		delete.delete(conn, fragment);		
	}
	
	
	

}
