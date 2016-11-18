package com.onepoint.database.walker;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class KosarajuAlgo {
	
	private final Map<Row, Set<Row>> rowsChildGraph;
	private final Map<Row, Set<Row>> rowsParentGraph;
	
	private final Deque<Row> l = new LinkedList<>();
	private final Set<Row> visited = new HashSet<>();
	
	private final List<Set<Row>> components = new ArrayList<>();
	
	public KosarajuAlgo(Map<Row, Set<Row>> rowsChildGraph, Map<Row, Set<Row>> rowsParentGraph) {
		int childSize = rowsChildGraph.size();
		int parentSize = rowsParentGraph.size();
		if (childSize != parentSize) {
			throw new IllegalArgumentException("Graphs have not the same nomber of vertices "
					+ "child=" + childSize	+ " parent=" + parentSize);
		}
		this.rowsChildGraph = rowsChildGraph;
		this.rowsParentGraph = rowsParentGraph;
	}
	
	public List<Set<Row>> process() {
	
		// First pass
		for (Row childRow : rowsChildGraph.keySet()) {
			visit(childRow);
		}
		
		// Second pass
		for (Row row : l) {
			assign(row, row);
		}
		
		return components;
	}
			
	
	private void visit(Row row) {
		if (visited.add(row)) {
			for (Row parent : rowsChildGraph.get(row)) {
				visit(parent);
			}
			l.push(row);
		};
	}
	
	private void assign(Row row, Row rootRow) {
		
		if (!isAssign(row)) {
			getComponent(rootRow).add(row);
			for (Row child : rowsParentGraph.get(row)) {
				assign(child, rootRow);
			}
		}
	}

	private boolean isAssign(Row row) {
		return components.stream().anyMatch(rows->rows.contains(row));
	}
	
	private Set<Row> getComponent(Row row) {
		return components.stream()
				.filter(rows->rows.contains(row))
				.findFirst()
				.orElseGet(()-> {
					Set<Row> newComponent = new HashSet<>();
					components.add(newComponent);
					return newComponent;
				});
	}
	

}
