package cn.edu.zju.fuzzydb.file;


import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import cn.edu.zju.fuzzydb.file.PrefixTree;
import cn.edu.zju.fuzzydb.db.DBConnection;
import cn.edu.zju.fuzzydb.db.MetaStore;

/**
 * prefixtree node maintains the samples in an in-memory
 * h2 table 
 * @author wusai
 *
 */
public class PrefixTreeNode {
	
	private GroupPredicate gp;

	private ArrayList<PrefixTreeNode> Child;
	
	private boolean isLeaf = true;
	
	private boolean isNeedSplit = false;
	
	private int sampleCount = 0;
	
	private boolean isInMemory = true;
	
	public PrefixTreeNode(GroupPredicate predicate){
		gp = predicate;
	}
	
	
	public boolean isLeafNode(){
		return isLeaf;
	}
	
	/**
	 * split the tree node by a new group column
	 * @param groupColumn
	 */
	public void split(String groupColumn){     //split the tuples in gp.tables by groupColumn,and every value correspond to a child
		if(!gp.isContained(groupColumn)){      //if not contain,continue
			//verify if this is a correct group column
			try {
				String[] columns = MetaStore.getInstance().getColumns(Property.baseTable);
				for(int i=0; i<columns.length; i++){
					//System.out.println("***************************************************  " + i);
					if(columns[i].equals(groupColumn.toUpperCase())){  //should also check whether this is a dimension column
						//sort original memory table by group columns
						String sql = "select * from " + gp.getTable() + "_mem order by " + groupColumn;
						//Connection conn = DBConnection.getMemoryConnection();
						//conn.setAutoCommit(false);
						Statement stmt = DBConnection.getMemoryConnection().createStatement();
						Statement updateStmt = DBConnection.getMemoryConnection().createStatement();
						
						stmt.setFetchSize(Property.sampleBatchSize);
						ResultSet rs = stmt.executeQuery(sql);           //reduce the networking time
						ResultSetMetaData metadata =  rs.getMetaData();  //get the structure of the table
						String currentGroupValue = "";
						String currentTable = "";
						while(rs.next()){
							String groupValue = rs.getString(groupColumn); //get the value of the split column
							//System.out.println("groupvalue: " + groupValue);
							//System.out.println("group:"+groupValue);
							if(groupValue.equals(currentGroupValue)){
								//still data in the old group
								String insert = "insert into " + currentTable + " values (";
								int cols = metadata.getColumnCount();
								for(int j=1; j<=cols; j++){
									int type = metadata.getColumnType(j);
									switch(type){
									case java.sql.Types.BLOB:
									case java.sql.Types.CHAR:
									case java.sql.Types.DATE:
									case java.sql.Types.VARCHAR:
									case java.sql.Types.TIMESTAMP: insert += "'" + rs.getString(j) + "',"; break;
									default: insert += rs.getString(j) + ","; 
									
									}
								}
								insert = insert.substring(0, insert.length()-1) + ")";
								updateStmt.addBatch(insert);	
								//System.out.println("split :" + insert);
							}
							else{
								//insert data into previous table in batch
								updateStmt.executeBatch();
								
								//create new child nodes
								currentGroupValue = groupValue;

								GroupPredicate childgp = gp.extendColumn(groupColumn, currentGroupValue);
								childgp.setStartOffset(gp.getStartOffset());
								childgp.setEndOffset(gp.getEndOffset());
								PrefixTreeNode child = new PrefixTreeNode(childgp);
								
								//Sample.showMemoryInformation();
								//System.out.println(child.toString());
								PrefixTree.getLruNodes().put(child.getPredicate().getTable(), child);
								
								if(Child == null)
									Child = new ArrayList<PrefixTreeNode>();
								
								this.Child.add(child);
								currentTable = childgp.getTable()+"_mem";
								
								//create the corresponding in-memory and disk tables
								DBConnection.createNewSampleTable(childgp);
								
								//insert data into the memory table. only when the memory is full,
								//will we use the disk table.
								String insert = "insert into " + currentTable + " values (";
								
								int cols = metadata.getColumnCount();
								for(int j=1; j<=cols; j++){
									int type = metadata.getColumnType(j);
									switch(type){
									case java.sql.Types.BLOB:
									case java.sql.Types.CHAR:
									case java.sql.Types.DATE:
									case java.sql.Types.VARCHAR:
									case java.sql.Types.TIMESTAMP: insert += "'" + rs.getString(j) + "',"; break;
									default: insert += rs.getString(j) + ","; 
									}
								}
								insert = insert.substring(0, insert.length()-1) + ")";
								//System.out.println("split: "+insert);
								updateStmt.addBatch(insert);
							}
						}
						
						updateStmt.executeBatch();
						updateStmt.close();
						rs.close();
						stmt.close();
						
					}
				}
				//drop current memory table
				/*Statement stmt = DBConnection.getMemoryConnection().createStatement();
				String sql = "drop table if exists " + gp.getTable() + "_mem";
				stmt.executeUpdate(sql);
				stmt.close();*/
				//does not drop the disk table
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			isLeaf = false;
			isNeedSplit = false;
			sampleCount = 0;
		}
	}
	
	/**
	 * flush the in-memory table to disk table
	 */
	public void flush(){	
		try {
			//String sql = "insert into " + gp.getTable() + " select * from " + gp.getTable()+"_mem";
			String sql = "select * from " + gp.getTable()+"_mem";
			Statement stmtmemory = DBConnection.getMemoryConnection().createStatement();
			ResultSet rs = stmtmemory.executeQuery(sql);
			//delete current in-memory table
			
			Statement stmt = DBConnection.getConnection().createStatement();
			ResultSetMetaData metadata = rs.getMetaData();
			int cols = metadata.getColumnCount();
			System.out.println("In flush!");
			while(rs.next()){
				String insert = "insert into " + gp.getTable() + " values(";
				for(int i = 1;i <= cols;i++){
					int type = metadata.getColumnType(i);
					switch(type){
					case java.sql.Types.BLOB:
					case java.sql.Types.CHAR:
					case java.sql.Types.DATE:
					case java.sql.Types.VARCHAR:
					case java.sql.Types.TIMESTAMP: insert += "'" + rs.getString(i) + "',"; break;
					default: insert += rs.getString(i) + ",";
					}
				}
				insert = insert.substring(0, insert.length()-1) + ")";
				//System.out.println("flush : " + insert);
				stmt.addBatch(insert);
			}
			stmt.executeBatch();
			stmt.close();
			System.out.println("flush over");
			//sql = "delete from " + gp.getTable()+"_mem";
			//stmt.executeUpdate(sql);
			//sql = "update table " + Property.progressTable + " set end=" + gp.getEndOffset() + " where tablename='" +
			//		gp.getTable() + "'";
			//stmt.executeUpdate(sql);
			stmtmemory.close();
			isInMemory = false;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public GroupPredicate getPredicate(){
		return gp;
	}
	public void setPredicate(GroupPredicate grouppredicate){
		gp = grouppredicate;
	}
	
	public ArrayList<PrefixTreeNode> getChildren(){
		return Child;
	}
	
	public void increase(){
		sampleCount++;
	}
	
	public int getSampleCount(){
		return sampleCount;
	}
	
	public void setIsNeedSplit(boolean needsplit){
		isNeedSplit = needsplit;
	}
	
	public boolean getIsNeedSplit(){
		return isNeedSplit;
	}


	public boolean isInMemory() {
		// TODO Auto-generated method stub
		return isInMemory;
	}
}
