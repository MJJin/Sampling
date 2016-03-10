package cn.edu.zju.fuzzydb.file;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Queue;
import java.util.LinkedList;

import cn.edu.zju.fuzzydb.db.DBConnection;
import cn.edu.zju.fuzzydb.db.MetaStore;
import cn.edu.zju.fuzzydb.estimator.AverageEstimator;
import cn.edu.zju.fuzzydb.replacement.LRUCache;
import cn.edu.zju.fuzzydb.db.GetH2Data;
/**
 * prefix tree is used to maintain samples in memory
 * and find the corresponding sample files from disk
 * @author wusai
 *
 */
public class PrefixTree {
	
	/**
	 * prefix root represents the original database
	 */
	private PrefixTreeNode root;
	
	/**
	 * current node that user is focusing on
	 */
	private PrefixTreeNode currentNode;
	
	/**
	 * database cursor that points to the base table
	 */
	private ResultSet baseResult;
	
	
	private AverageEstimator averageEstimator;
	
	private Queue<PrefixTreeNode> nodesInDiskTable;
	
	private static LRUCache lruNodes;
	
	private int Count;
	
	public int getCount(){
		return Count;
	}
	
	public static LRUCache getLruNodes(){
		return lruNodes;
	}
	
	public PrefixTree() throws SQLException{
		Count = 0;
		GroupPredicate gp = new GroupPredicate();
		root = new PrefixTreeNode(gp);
		nodesInDiskTable = new LinkedList<PrefixTreeNode>();
		currentNode = root;
		lruNodes = new LRUCache();
		DBConnection.init();
		//GroupPredicate gp = root.getPredicate();
		DBConnection.createNewSampleTable(gp);
		try {
			Connection conn = DBConnection.getConnection();
			conn.setAutoCommit(false);
			Statement stmt = DBConnection.getConnection().createStatement();
			stmt.setFetchSize(Property.sampleBatchSize);
			long stime = System.currentTimeMillis();
			baseResult = stmt.executeQuery("select * from base");
			long etime = System.currentTimeMillis();
			System.out.println("read time: " +(etime - stime));
			Sample.showMemoryInformation();
			/*while(baseResult.next()){
				System.out.println(baseResult.getString(1));
			}*/
			//stmt.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public ResultSet getbase(){
		return baseResult;
	}
	
	public AverageEstimator getAverageEstimator(){
		return averageEstimator;
	}
	
	/**
	 * 
	 * @param Groupcolumn
	 * @param confidence
	 * @param errorRate
	 */
	public void expand(String Groupcolumn, double confidence, double errorRate){
		if(currentNode == root){
			//sample the original table
			//initSampling(Groupcolumn,conditions);
		}
		else{
			//first try to use memory table to answer
			
			/*for(int i = 0;i < 10;i++){
				performSampling("city");
			}*/
			
			//if cannot meet the requirement, start sampling and splitting the tree node
			
		}
	}
	
	/**
	 * try to use memory table to answer
	 */
	public void findSampleFromMemory(Map<String,String> conditions){
		PrefixTreeNode searchNode = root;
		Queue<PrefixTreeNode> candidateNodes = new LinkedList<PrefixTreeNode>();
		candidateNodes.add(searchNode);
		while(candidateNodes.size() != 0){
			//System.out.println("$$$$$$$$$" + candidateNodes.size() + " " + conditions.toString());
			searchNode = candidateNodes.poll();
			if(searchNode != null && searchNode.isLeafNode()){
				//System.out.println("flush table: " + searchNode.getPredicate().getTable());
				if(searchNode.isInMemory() == false){
					nodesInDiskTable.add(searchNode);
					continue;
				}
				try{
					Statement stmt = DBConnection.getMemoryConnection().createStatement();
					String sql = "select * from " + searchNode.getPredicate().getTable() + "_mem where ";
					for(Map.Entry<String, String> entry : conditions.entrySet()){
						//System.out.println("the conditions : " + entry.getKey() + " " + entry.getValue());
						sql += entry.getKey();
						sql += "='";
						sql += entry.getValue();
						sql += "' and ";
					}
					sql = sql.substring(0, sql.length()-5);
					//System.out.println(sql);
					ResultSet rs = stmt.executeQuery(sql);
					ResultSetMetaData metadata =  rs.getMetaData();
					int cols = metadata.getColumnCount();
					//System.out.print("The find result:\n ");
					while(rs.next()){
						/*System.out.print("(");
						for(int i = 1 ; i < cols;i++){
							System.out.print(rs.getString(i)+",");
						}
						System.out.print(rs.getString(cols) + ")  ");*/
						averageEstimator.addSample(Double.valueOf(rs.getString(MetaStore.getQuantityColumns().trim())));
						if(averageEstimator.getIsEnough() == true){
							return;
						}
					}
				}
				catch(SQLException e){
					e.printStackTrace();
				}
				lruNodes.get(searchNode.getPredicate().getTable());
			}
			if(searchNode.isLeafNode() == false){
				for(PrefixTreeNode node:searchNode.getChildren()){
					GroupPredicate gp = node.getPredicate();
					int len = gp.getDimensionNumber();
					if(!conditions.containsKey(gp.getDimensionAt(len-1))){
						candidateNodes.add(node);
						continue;
					}
					//System.out.println("Vaules : " + conditions.get(gp.getDimensionAt(len-1)).trim() + "  " +gp.getValueAt(len-1) );
					if(conditions.get(gp.getDimensionAt(len-1)).trim().equals(gp.getValueAt(len-1).trim())){
						candidateNodes.add(node);
						break;
					}
				}
			}
		}
	}
	
	public void findSampleFromDisk(Map<String,String> conditions){
		PrefixTreeNode searchNode = null;
		while(nodesInDiskTable.size() != 0){
			searchNode = nodesInDiskTable.poll();
			String sql = "select * from "+ searchNode.getPredicate().getTable() + " where ";
			for(Map.Entry<String, String> entry : conditions.entrySet()){
				sql += entry.getKey();
				sql += "='";
				sql += entry.getValue();
				sql += "' and ";
			}
			sql = sql.substring(0, sql.length()-5);
			System.out.println(sql);
			try{
				Statement stmt = DBConnection.getConnection().createStatement();
				ResultSet rs = stmt.executeQuery(sql);
				ResultSetMetaData metadata =  rs.getMetaData();
				int cols = metadata.getColumnCount();
				System.out.println("The find result in memory:");
				while(rs.next()){
					System.out.print("(");
					for(int i = 1 ; i < cols;i++){
						System.out.print(rs.getString(i)+",");
					}
					System.out.print(rs.getString(cols) + ")  ");
					averageEstimator.addSample(Double.valueOf(rs.getString(MetaStore.getQuantityColumns().trim())));
					if(averageEstimator.getIsEnough() == true){
						nodesInDiskTable.clear();
						return;
					}
				}
			}
			catch(SQLException e){
				e.printStackTrace();
			}
		}
	}
	
	public void test(String group,double confidence, double errorRate,Map<String,String> conditions){
		averageEstimator = new AverageEstimator(confidence,errorRate);
		findSampleFromMemory(conditions);
		if(averageEstimator.getIsEnough() == false){
			findSampleFromDisk(conditions);
		}
		try{
			while(averageEstimator.getIsEnough() == false && !baseResult.isAfterLast()){
				//System.out.println("current: " + averageEstimator.getCurrentErrorRate() + " errorrate: "+ averageEstimator.getErrorRate());
				performSampling(group,conditions);
				//baseResult = GetH2Data.getResultSet();
			}
		}
		catch(SQLException e){
			e.printStackTrace();
		}
	}
	
	/*public void initSampling(String group,Map<String,String> conditions){
		GroupPredicate gp = root.getPredicate();
		DBConnection.createNewSampleTable(gp);
		System.out.println("init "+ gp.getIndexCode());
		performSampling(group,conditions);
	}*/
	
	/**
	 * split the nodes that have been inserted
	 */
	public void splitNodes(HashMap<String,PrefixTreeNode> nodes){
		for(HashMap.Entry<String, PrefixTreeNode> entry : nodes.entrySet()){
			PrefixTreeNode node = entry.getValue();
			if(node.getSampleCount() > Property.maxNodeSampleSize && node.getIsNeedSplit() == false && node.getPredicate().getDimensionNumber() < MetaStore.getDimensionColumns().size()){
				for(String column:MetaStore.getDimensionColumns()){
					if(!node.getPredicate().isContained(column)){
						node.split(column);
						break;
					}
				}
			}
		}
	}
	
	/**
	 * sampling the database,if the sample satisfy the conditions,add it to the estimator.
	 */
	public void performSampling(String group,Map<String,String> conditions){
		
		//currentNode.split(group);
		ArrayList<String> sqlCmd = new ArrayList<String>();
		ArrayList<Boolean> isInMemory = new ArrayList<Boolean>();
		HashMap<String,PrefixTreeNode> nodes = insert(sqlCmd,isInMemory,conditions);
		//System.out.println("Size: " + sqlCmd.size());
		try{
			Statement memorystmt = DBConnection.getMemoryConnection().createStatement();
			Statement stmt = DBConnection.getConnection().createStatement();
			for(int i = 0;i < sqlCmd.size();i++){
				//System.out.println(i);
				if(isInMemory.get(i) == true)
				{
					//System.out.println("perform: in memory");
					memorystmt.addBatch(sqlCmd.get(i));
				}
				else
				{
					//System.out.println("perform: in disk");
					stmt.addBatch(sqlCmd.get(i));
				}
			}
			Count += sqlCmd.size();
			memorystmt.executeBatch();
			stmt.executeBatch();
			sqlCmd.clear();
			memorystmt.close();
			stmt.close();
		}
		catch(SQLException e){
			e.printStackTrace();
		}
		for(Map.Entry<String, PrefixTreeNode> entry:nodes.entrySet()){
			PrefixTreeNode node= lruNodes.remove(entry.getKey());
			if(node != null)
				lruNodes.put(entry.getKey(), node);
		}
		splitNodes(nodes);
	}
	
	
	/**
	 * insert the tuple referred by current baseResult into the prefix tree
	 * this may be slow due to the comparisons
	 */
	private HashMap<String,PrefixTreeNode> insert(ArrayList<String> sqlCmd,ArrayList<Boolean> isInMemory,Map<String,String> conditions){
		HashMap<String,PrefixTreeNode> nodesNeedExamine = new HashMap<String,PrefixTreeNode>();
		try {
			int sample_count = 0;
			while(sample_count < Property.sampleBatchSize && baseResult.next()){
				
				boolean flag = true;
				for(Map.Entry<String, String> entry:conditions.entrySet()){
					if(!baseResult.getString(entry.getKey().trim()).equals(entry.getValue().trim())){
						flag = false;
						break;
					}
				}
			
				PrefixTreeNode searchNode = root;
				while(searchNode.isLeafNode() == false)
				{
					ArrayList<PrefixTreeNode> children = searchNode.getChildren();
					for(PrefixTreeNode node : children){
						GroupPredicate gp = node.getPredicate();
						int size = gp.getDimensionNumber();
						String columnName = gp.getDimensionAt(size-1);
						String value = baseResult.getString(columnName); //all values are considered as string values temporarily
						//System.out.println(value + " "+ columnName);
						if(value.equals(gp.getValueAt(size-1))){
							searchNode = node;
							break;
						}
					}
					//System.out.println("has child!  "+searchNode.isLeafNode()+" "+searchNode.getPredicate().toString());
				}
				if(searchNode.isLeafNode()){
					//insert into the corresponding memory table
					String insert = "insert into ";
					if(searchNode.isInMemory()){
						insert += searchNode.getPredicate().getTable() + "_mem values (";
					}
					else{
						insert += searchNode.getPredicate().getTable() + " values (";
					}
					ResultSetMetaData metadata = baseResult.getMetaData();
					int cols = metadata.getColumnCount();
					for(int j=1; j<=cols; j++){
						int type = metadata.getColumnType(j);
						switch(type){
						case java.sql.Types.BLOB:
						case java.sql.Types.CHAR:
						case java.sql.Types.DATE:
						case java.sql.Types.VARCHAR:
						case java.sql.Types.TIMESTAMP: insert += "'" + baseResult.getString(j) + "',"; break;
						default: insert += baseResult.getString(j) + ","; 
						}
					}
					insert = insert.substring(0, insert.length()-1) + ")";
					searchNode.getPredicate().sampleCountIncrease();
					sqlCmd.add(insert);
					isInMemory.add(searchNode.isInMemory());
					//System.out.println(insert + " " +searchNode.getPredicate().toString());
				}
				searchNode.increase();
				if(!nodesNeedExamine.containsKey(searchNode.getPredicate().getTable())){
					nodesNeedExamine.put(searchNode.getPredicate().getTable(),searchNode);
				}
				
				//System.out.println("count: "+ sample_count + " "+ sqlCmd.size());
				sample_count++;
				if(flag == true){
					averageEstimator.addSample(Double.valueOf(baseResult.getString(MetaStore.getQuantityColumns().trim())));
					if(averageEstimator.getIsEnough() == true){
						return nodesNeedExamine;
					}
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return nodesNeedExamine;
	}
	
	
}
