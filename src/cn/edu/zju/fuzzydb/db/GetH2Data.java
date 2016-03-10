package cn.edu.zju.fuzzydb.db;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;

import cn.edu.zju.fuzzydb.file.Property;

public class GetH2Data {
	private static long cursor = 0;
	private static final int stepLength = 10000;
	
	public static ResultSet getResultSet(){
		ResultSet rs = null;
		try{
			Statement stmt = DBConnection.getConnection().createStatement();
			String sql = "select * from " + Property.baseTable + " limit "+cursor + ","+stepLength;
			System.out.println("Get h2 data: " + sql);
			cursor += stepLength;
			rs = stmt.executeQuery(sql);
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		return rs;
	}
	
}
