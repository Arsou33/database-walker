package org.peekmoon.database.walker;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.peekmoon.database.walker.schema.Table;

public class Walker {

	public Fragment extract(DataSource ds, Table table, KeyValue...values) throws SQLException {
		try (Connection conn = ds.getConnection()) {
			return extract(conn, table, values);
		}
	}

	public Fragment extract(Connection conn, Table table, KeyValue... values) throws SQLException {
		DatabaseReader reader = new DatabaseReader(table.getSchema());
		Fragment fragment = new Fragment(table.getSchema());
		for (KeyValue value : values) {
			fragment.add(reader.extract(conn, table, value));
		}
		return fragment;
	}

	public void insert(DataSource ds, Fragment fragment) throws SQLException {
		try (Connection conn = ds.getConnection()) {
			insert(conn, fragment);
		}
	}

	public void insert(Connection conn, Fragment fragment) throws SQLException {
		DatabaseTaskInsert inserter = new DatabaseTaskInsert();
		inserter.process(conn, fragment, null);
		
	}

	public void insert(DataSource ds, Fragment fragment, RowFilter conditions) throws SQLException {
		try (Connection conn = ds.getConnection()) {
			insert(conn, fragment, conditions);
		}
	}

	public void insert(Connection conn, Fragment fragment, RowFilter conditions) throws SQLException {
		DatabaseTaskInsert inserter = new DatabaseTaskInsert();
		inserter.process(conn, fragment, conditions);
	}

	public void delete(DataSource ds, Fragment fragment) throws SQLException {
		try (Connection conn = ds.getConnection()) {
			delete(conn, fragment);
		}
	}

	public void delete(Connection conn, Fragment fragment) throws SQLException {
		DatabaseTaskDelete delete = new DatabaseTaskDelete();
		delete.process(conn, fragment, null);
	}

	public void delete(DataSource ds, Fragment fragment, RowFilter conditions) throws SQLException {
		try (Connection conn = ds.getConnection()) {
			delete(conn, fragment, conditions);
		}
	}

	public void delete(Connection conn, Fragment fragment, RowFilter conditions) throws SQLException {
		DatabaseTaskDelete delete = new DatabaseTaskDelete();
		delete.process(conn, fragment, conditions);		
	}

	public void execute(DatabaseTask task, Connection conn, Fragment fragment) throws SQLException {
		task.process(conn, fragment, null);
	}

}
