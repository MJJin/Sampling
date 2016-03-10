package cn.edu.zju.fuzzydb.db;

import org.apache.commons.math3.distribution.*;

import java.sql.SQLException;
import java.sql.Statement;

import cn.edu.zju.fuzzydb.db.DBConnection;

public class CreateData {
	public void Create() throws SQLException
	{
		Statement stmt = DBConnection.getConnection().createStatement();
		stmt.execute("create table if not exists base(id int,city VARCHAR(10),quarter int,year int,brand varchar(10),age varchar(5),sale double)");
		String[] city = new String[]{"HZ","SH","BJ","SZ","GZ","XM","DL","XA","LS","HEB","FZ"}; //11
		int[] id = new int[]{0,1,2,3,4,5,6};
		int[] quarter = new int[]{0,1,2,3};
		int[] year = new int[]{2009,2010,2011,2012,2013};
		String[] brand = new String[]{"zju","dlut","pku","thu","nju","hzu"};
		String[] age = new String[]{"child","adult","old"};
		NormalDistribution normal = new NormalDistribution(1000,250);
		for(int i = 0;i < 2000000;i++)
		{
			String sql = "insert into base(id,city,quarter,year,brand,age,sale) values(" + id[i%7] +",'"+city[i%11] + "',"+quarter[i%4]+
					","+year[i%5]+",'"+brand[i%6]+"','"+age[i%3]+"',"+normal.sample()+")";
			//System.out.println(sql);
			stmt.executeUpdate(sql);
		}
		stmt.close();
	}
}
