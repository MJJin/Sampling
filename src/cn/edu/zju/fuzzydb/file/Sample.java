package cn.edu.zju.fuzzydb.file;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

import cn.edu.zju.fuzzydb.estimator.AverageEstimator;
import cn.edu.zju.fuzzydb.file.PrefixTree;
import cn.edu.zju.fuzzydb.db.*;
public class Sample {
	public static void main(String[] args) throws SQLException
	{
		long stime,etime;
		showtables();
		CreateData cd = new CreateData();
		try
		{
			cd.Create();
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		stime = System.currentTimeMillis();
		PrefixTree mytree = new PrefixTree();
		etime = System.currentTimeMillis();
		System.out.println("read time: " + (etime - stime));
		String[] columns = new String[]{"id","city","quarter","year","brand","age"};
		MetaStore.setDimensionColumns(columns);
		MetaStore.setQuantityColumns("sale");
		//mytree.initSampling("id");
		
		Map<String, String> conditions = new HashMap<String,String>();
		conditions.put("id", "1");
		conditions.put("city", "HZ");
		//conditions.put("quarter", "0");
		//mytree.findSampleFromMemory(conditions );
		stime = System.currentTimeMillis();
		mytree.test("id",0.95,50,conditions);
		etime = System.currentTimeMillis();
		showestimator(mytree.getAverageEstimator());
		System.out.println("Total count : " + mytree.getCount() + " total time: " + (etime-stime));
		
		
		
		
		Map<String,String> newCondition = new HashMap<String,String>();
		newCondition.put("id", "1");
		newCondition.put("quarter", "0");
		newCondition.put("city", "HZ");
		stime = System.currentTimeMillis();
		mytree.test("id",0.99, 5, newCondition);
		etime = System.currentTimeMillis();
		showestimator(mytree.getAverageEstimator());
		System.out.println("Total count : " + mytree.getCount() + " total time: " + (etime-stime));
		
		
		Map<String,String> newCondition1 = new HashMap<String,String>();
		newCondition1.put("quarter", "2");
		//newCondition1.put("quarter", "0");
		newCondition1.put("city", "SH");
		stime = System.currentTimeMillis();
		mytree.test("id",0.95, 10, newCondition1);
		etime = System.currentTimeMillis();
		showestimator(mytree.getAverageEstimator());
		System.out.println("Total count : " + mytree.getCount() + " total time: " + (etime-stime));

		
		showtables();
		showmemorytables();
		showMemoryInformation();
		showLruNodes();
		//showMemoryData(4);
	}
	
	public static void showMemoryInformation(){
		int mb = 1024 * 1024;
		Runtime runtime = Runtime.getRuntime();
		System.out.println("#### Heap utilization statistics [MB] ####");
		System.out.println("Used Memory: " + (runtime.totalMemory() - runtime.freeMemory()) / mb);
		System.out.println("Free Memory: " + runtime.freeMemory() / mb);
		System.out.println("Total Memory: " + runtime.totalMemory()/mb);
		System.out.println("Max Memory:" + runtime.maxMemory()/mb);
	}
	
	public static void showestimator(AverageEstimator estimator){
		System.out.println("The estimator: " + "\nthe current errorRate: " + estimator.getCurrentErrorRate() 
				+ "\n the count: " + estimator.getCount() + "\nthe estimator: " + estimator.getEstimator());
	}
	
	public static void showtables() {
		try
		{
			Statement stmt = DBConnection.getConnection().createStatement();
			Statement stmttable = DBConnection.getConnection().createStatement();
			ResultSet restable = stmt.executeQuery("show tables");
			System.out.println("Show tables:");
			while(restable.next()){
				System.out.print(restable.getString(1) + "  ");
				//if(!restable.getString(1).equalsIgnoreCase("base"))
				//{
					String sql = "TRUNCATE table " + restable.getString(1);
					stmttable.executeUpdate(sql);
					sql = "drop table " + restable.getString(1);
					stmttable.executeUpdate(sql);
				//}
			}
			stmt.close();
			restable.close();
			stmttable.close();
		}
		catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	public static void showmemorytables() {
		try
		{
			Statement stmt = DBConnection.getMemoryConnection().createStatement();
			ResultSet res = stmt.executeQuery("show tables");
			System.out.println("Show Memory tables:");
			while(res.next()){
				System.out.print(res.getString(1) + " ");
			}
			stmt.close();
			res.close();
		}
		catch(SQLException e)
		{
			e.printStackTrace();
		}
		
	}
	
	public static void showMemoryData(int columns){
		try{
			Statement stmt = DBConnection.getMemoryConnection().createStatement();
			Statement datastmt = DBConnection.getMemoryConnection().createStatement();
			ResultSet rs = stmt.executeQuery("show tables");
			while(rs.next()){
				String tablename = rs.getString(1);
				System.out.println("\nThe data in "+tablename+" is:");
				String sql = "select * from " + tablename;
				ResultSet datars = datastmt.executeQuery(sql);
				while(datars.next()){
					System.out.print(" (");
					for(int i = 1;i < columns;i++){
						System.out.print(datars.getString(i) + ",");
					}
					System.out.print(datars.getString(columns) + ") ");
				}
			}
		}
		catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	public static void showLruNodes(){
		System.out.println("The nodes:");
		for(HashMap.Entry<String,PrefixTreeNode> entry:PrefixTree.getLruNodes().entrySet()){
			System.out.print(entry.getKey()+"  ");
		}
	}
}
