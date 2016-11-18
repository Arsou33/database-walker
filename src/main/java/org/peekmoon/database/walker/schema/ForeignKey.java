package org.peekmoon.database.walker.schema;

public class ForeignKey extends Key {

	private PrimaryKey pk;
	
	public ForeignKey(String name, Table table, PrimaryKey pk) {
		super(name, table);
		this.pk = pk;
	}

	public PrimaryKey getPrimary() {
		return pk;
	}


}
