package cn.edu.zju.fuzzydb.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * using h2 in embbeded mode
 * @author wusai
 *
 */
public class H2Test {
	
	public static void test(String[] args){
		try {
			Class.forName("org.h2.Driver");
	   
			Connection conn = DriverManager.
			        getConnection("jdbc:h2:~/test", "sa", "");
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
