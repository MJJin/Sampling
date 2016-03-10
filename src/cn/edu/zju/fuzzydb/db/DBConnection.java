package cn.edu.zju.fuzzydb.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import cn.edu.zju.fuzzydb.file.GroupPredicate;
import cn.edu.zju.fuzzydb.file.Property;

/**
 * a connection to the embbeded H2 database
 * @author wusai
 *
 */
public class DBConnection {
	
	private static Connection conn;
	
	private static Connection memoryconn;
	
	public static Connection getConnection(){
		if(conn==null){
			try {
				Class.forName("org.h2.Driver");
				conn = DriverManager.
				        getConnection("jdbc:h2:./"+Property.dbName, "sa", "");
				//init();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	   
			
		}
		return conn;
	}
	
	public static Connection getMemoryConnection(){
		if(memoryconn == null){
			try{
				Class.forName("org.h2.Driver");
				memoryconn = DriverManager.getConnection("jdbc:h2:mem:./"+Property.dbName, "sa", "");
				//init();
			}
			catch(ClassNotFoundException e){
				e.printStackTrace();
			}
			catch(SQLException e){
				e.printStackTrace();
			}
		}
		return memoryconn;
	}
	
	public static void close(){
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void init() throws SQLException{
		String sql = "create table " + " if not exists " + Property.configTable +
	                 "(groupid int,  grouppredicate VARCHAR(1024), groupTable VARCHAR(100), selectpredicate TEXT, sampleSize int)";
		System.out.println(sql);
		Statement stmt = DBConnection.getConnection().createStatement();
		try{
			stmt.executeUpdate(sql);
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		
		sql = "create table " +  " if not exists " + Property.progressTable +
				"(tablename VARCHAR(100) primary key, start long, end long)";
		try{
			stmt.executeUpdate(sql);
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		stmt.close();
	}
	
	
	
	public static void createNewSampleTable(GroupPredicate gp){
		
		//first check whether the table with the same name exists
		ArrayList<String> tnames = new ArrayList<String>();
		String testTableName = Integer.toString(gp.getIndexCode());
		//System.out.println("creat: "+testTableName + "  "+ gp.getIndexCode());
		String sql = "show tables";
		Statement stmt,memorystmt;
		try {
			stmt = DBConnection.getConnection().createStatement();
			ResultSet rs =stmt.executeQuery(sql);	
			while(rs.next()){
				tnames.add(rs.getString(1));
			}
			rs.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		//update the table name;
		while(tnames.contains(testTableName)){
			testTableName += "x";
		}
		//gp.setTableName(testTableName);
		
		sql = "insert into " + Property.configTable +" values (" +
				gp.getIndexCode() + ",'" + gp.toString() + "','" + gp.getTable() + "','"+gp.toString()+"',0)";
	
		try {
			stmt = DBConnection.getConnection().createStatement();
			//System.out.println(sql);
			stmt.executeUpdate(sql);
			//create an empty table by copying the schema information from base table
			sql = "create table if not exists " + gp.getTable()  + " as select * from " + Property.baseTable + " limit 0";
			stmt.executeUpdate(sql);
			sql = "select * from " + Property.baseTable + " limit 0";
			ResultSet res = stmt.executeQuery(sql);
			
			memorystmt = getMemoryConnection().createStatement();
			sql = "create memory table if not exists " + gp.getTable()  + "_mem (";
			ResultSetMetaData metadata =  res.getMetaData();
			int cols = metadata.getColumnCount();
			
			for(int j = 1; j <= cols;j++){
				sql += metadata.getColumnName(j);
				sql += " ";
				sql += metadata.getColumnTypeName(j);
				int precision = metadata.getPrecision(j);
				if(precision != 0){
					sql += "(";
					sql += String.valueOf(precision);
					sql += ")";
				}
				sql += ",";
			}
			
			sql = sql.substring(0, sql.length()-1) + ")";
			//System.out.println(sql);
			//System.out.println("the create sql : "+sql);
			memorystmt.executeUpdate(sql);
			memorystmt.close();
			stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
