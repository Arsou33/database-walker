package org.peekmoon.database.walker.schema;

public interface SchemaFilter {
	
	/**
	 * Filtre les éléments du schéma qui sont pris en compte dans l'outils. 
	 * Si toutes les colonnes d'une table sont ignorées, la table est ignorée
	*/
	boolean ignoreColumn(String tableName, String columnName);
	
	boolean ignoreForeignKey(String keyName);

}
