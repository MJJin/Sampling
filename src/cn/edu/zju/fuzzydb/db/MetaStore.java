package cn.edu.zju.fuzzydb.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

/**
 * meta store maintains the schema information
 * @author wusai
 *
 */
public class MetaStore {
	
	/**
	 * the singleton instance
	 */
	private static MetaStore instance = null;
	
	/**
	 * record the dimension columns
	 */
	private static ArrayList<String> dimensionColumns;
	
	/**
	 * record the quantity columns
	 */
	private static String quantityColumn;
	
	/**
	 * 
	 * @return the singleton instance
	 */
	public static MetaStore getInstance(){
		if(instance == null)
			instance = new MetaStore();
		return instance;
	}
	
	/**
	 * set the dimension columns
	 */
	public static void setDimensionColumns(ArrayList<String> columns){
		if(dimensionColumns == null){
			dimensionColumns = new ArrayList<String>();
		}
		for(String column: columns){
			if(!dimensionColumns.contains(column))
				dimensionColumns.add(column);
		}
	}
	
	/**
	 * set the quantity column
	 */
	public static void setQuantityColumns(String column){
		quantityColumn = column;
	}
	
	/**
	 * get the quantity column
	 */
	public static String getQuantityColumns(){
		return quantityColumn;
	}
	/**
	 * set the dimension columns
	 */
	public static void setDimensionColumns(String[] columns){
		if(dimensionColumns == null){
			dimensionColumns = new ArrayList<String>();
		}
		for(String column: columns){
			if(!dimensionColumns.contains(column))
				dimensionColumns.add(column);
		}
			
	}
	
	/**
	 * set a dimension column
	 */
	public static void setDimensionColumns(String column){
		if(dimensionColumns == null){
			dimensionColumns = new ArrayList<String>();
		}
		dimensionColumns.add(column);
	}
	
	public static ArrayList<String> getDimensionColumns(){
		return dimensionColumns;
	}
	
	
	/**
	 * get the number of columns in a table
	 */
	public int getColumnNumber(String table) throws SQLException{
		//Connection con = DBConnection.getConnection();
		Statement stmt = DBConnection.getConnection().createStatement();
		ResultSet rs = stmt.executeQuery("show columns from base");
		System.out.println("show rows; " + rs.getRow());
		
		int columnCount = 0;
		while(rs.next()){
			columnCount++;
			//System.out.println("++++++++++++++++++++:"+columnCount);
		}
		return columnCount;
	}
	
	/**
	 * 
	 * @param table
	 * @return the column names of a table
	 * @throws SQLException 
	 */
	public String[] getColumns(String table) throws SQLException{
		
		Connection con = DBConnection.getConnection();
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("show columns from " + table);
		ArrayList<String> tname = new ArrayList<String>();
		while(rs.next()){
			tname.add(rs.getString(1));
		}
		String[] str = new String[tname.size()];
		tname.toArray(str);
		return str;
	}
	
	/**
	 * @param db
	 * @return all tables in a database
	 * @throws SQLException 
	 */
	public String[] getAllTables() throws SQLException{
		Connection con = DBConnection.getConnection();
		Statement stmt = con.createStatement();
		ResultSet rs = stmt.executeQuery("show tables");
		
		ArrayList<String> tname = new ArrayList<String>();
		while(rs.next()){
			tname.add(rs.getString(1));
		}
		return (String[])tname.toArray();
	}
	
	/**
	 * 
	 * @param column
	 * @return check whether it is a dimension table
	 */
	public boolean isDimensionColumn(String column){
		if(dimensionColumns.contains(column))
			return true;
		else
			return false;
	}
	
	/**
	 * 
	 * @param column
	 * @return check whether we need to compute aggregations for the column
	 */
	public boolean isQuantityColumn(String column){
		if(!dimensionColumns.contains(column))
			return true;
		else
			return false;
	}
	
}
