package org.peekmoon.database.walker;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.peekmoon.database.walker.schema.Schema;
import org.peekmoon.database.walker.schema.Table;

public class Fragment {
	
	private final Schema schema; 
	private final Map<Row, Set<Row>> rowsChildGraph = new HashMap<>(); // Child to parent relations
	private final Map<Row, Set<Row>> rowsParentGraph = new HashMap<>(); // Parent to child relations

	public Fragment(Schema schema) {
		this.schema = schema;
	}
	
	public Schema getSchema() {
		return schema;
	}
	
	public Set<Row> getRows() {
		return Collections.unmodifiableSet(rowsChildGraph.keySet());
	}

	public Set<Row> getRowsFromTable(String tableName) {
		return getRowsFromTable(schema.getTable(tableName));
	}
	
	public Set<Row> getRowsFromTable(Table table) {
	    Set<Row> rows = rowsChildGraph.keySet().stream()
		        .filter(row -> row.getTable().equals(table))
		        .collect(Collectors.toSet());
		return Collections.unmodifiableSet(rows);
	}

	public int getNbRows() {
		return rowsChildGraph.keySet().size();
	}

	void add(Row row) {
		rowsChildGraph.computeIfAbsent(row, c -> new HashSet<>());
		rowsParentGraph.computeIfAbsent(row, c -> new HashSet<>());
	}
	
	void add(Row childRow, Row row) {
		rowsChildGraph.get(childRow).add(row);
		rowsParentGraph.get(row).add(childRow);
	}

	void add(Fragment fragment) {
		for (Entry<Row, Set<Row>> childEntry : fragment.rowsChildGraph.entrySet()) {
			rowsChildGraph.computeIfAbsent(childEntry.getKey(), c -> new HashSet<>()).addAll(childEntry.getValue());
		}
		for (Entry<Row, Set<Row>> parentEntry : fragment.rowsParentGraph.entrySet()) {
			rowsParentGraph.computeIfAbsent(parentEntry.getKey(), c -> new HashSet<>()).addAll(parentEntry.getValue());
		}
	}
	
	Set<Row> getChilds(Row row) {	
		return Collections.unmodifiableSet(rowsParentGraph.get(row));
	}

	boolean contains(Row row) {
		return rowsChildGraph.keySet().contains(row);
	}
	
	/**
	 * Implementing : https://en.wikipedia.org/wiki/Kosaraju%27s_algorithm
	 * @return Ordered list of strong connected rows
	 */
	public List<Set<Row>> getDeleteOrderedPartitions() {
		KosarajuAlgo algo = new KosarajuAlgo(rowsChildGraph, rowsParentGraph);
		return algo.process();
	}
	
	public List<Set<Row>> getInsertOrderedPartitions() {
		KosarajuAlgo algo = new KosarajuAlgo(rowsParentGraph, rowsChildGraph);
		return algo.process();
	}




	
	

}
