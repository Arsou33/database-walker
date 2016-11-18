package com.onepoint.database.walker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.onepoint.database.walker.schema.Schema;

import java.util.Set;

public class Fragment {
	
	private final Schema schema; 
	private final Map<Row, Set<Row>> rowsChildGraph = new HashMap<>(); // Child to parent relations
	private final Map<Row, Set<Row>> rowsParentGraph = new HashMap<>(); // Parent to child relations

	public Fragment(Schema schema) {
		this.schema = schema;
	}

	public boolean contains(Row row) {
		return rowsChildGraph.keySet().contains(row);
	}
	
	public void add(Row row) {
		rowsChildGraph.computeIfAbsent(row, c -> new HashSet<>());
		rowsParentGraph.computeIfAbsent(row, c -> new HashSet<>());
	}
	
	public void add(Row childRow, Row row) {
		rowsChildGraph.get(childRow).add(row);
		rowsParentGraph.get(row).add(childRow);
	}

	public void add(Fragment fragment) {
		for (Entry<Row, Set<Row>> childEntry : fragment.rowsChildGraph.entrySet()) {
			rowsChildGraph.computeIfAbsent(childEntry.getKey(), c -> new HashSet<>()).addAll(childEntry.getValue());
		}
		for (Entry<Row, Set<Row>> parentEntry : fragment.rowsParentGraph.entrySet()) {
			rowsParentGraph.computeIfAbsent(parentEntry.getKey(), c -> new HashSet<>()).addAll(parentEntry.getValue());
		}
	}

	public int getNbRows() {
		return rowsChildGraph.keySet().size();
	}

	public Schema getSchema() {
		return schema;
	}
	
	/**
	 * Implementing : https://en.wikipedia.org/wiki/Kosaraju%27s_algorithm
	 * @return Ordered list of strong connected rows
	 */
	List<Set<Row>> getDeleteOrderedPartitions() {
		KosarajuAlgo algo = new KosarajuAlgo(rowsChildGraph, rowsParentGraph);
		return algo.process();
	}
	
	List<Set<Row>> getInsertOrderedPartitions() {
		KosarajuAlgo algo = new KosarajuAlgo(rowsParentGraph, rowsChildGraph);
		return algo.process();
	}




	
	

}
