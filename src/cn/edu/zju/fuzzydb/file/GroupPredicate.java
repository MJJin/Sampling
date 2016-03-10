package cn.edu.zju.fuzzydb.file;

import java.util.ArrayList;

/**
 * Group predicate records how the data are partitioned using group by
 * operators. It is  partitioned by the first group column, then second column
 * and consecutive ones. The order is very important.
 * @author wusai
 *
 */
public class GroupPredicate {

	/**
	 * record all dimension columns in order
	 */
	private ArrayList<String> dimensions;
	
	private ArrayList<String> values;
	
	private long startOffset;
	
	private long endOffset;
	
	private int sample_count;
	
	private String table;
	
	public GroupPredicate(){
		dimensions = new ArrayList<String>();
		values = new ArrayList<String>();
		startOffset = 0;
		endOffset = 0;
		sample_count = 0;
	}
	
	public void sampleCountIncrease(int increase){
		sample_count += increase;
	}
	public void sampleCountIncrease(){
		sample_count ++;
	}
	
	
	public long getStartOffset(){
		return startOffset;
	}
	
	public long getEndOffset(){
		return endOffset;
	}
	
	public void setStartOffset(long start){
		startOffset = start;
	}
	
	public void setEndOffset(long end){
		endOffset = end;
	}
	
	public void parse(String indexCode){
		String data = indexCode;
		int idx = data.indexOf('@');
		table = data.substring(0, idx);
		data = data.substring(idx+1, data.length());
		String[] content = data.split("_");
		for(int i=0; i<content.length; i+=2){
			dimensions.add(content[i]);
			values.add(content[i+1]);
		}
			
	}
	
	public String getTable(){
		//System.out.println("outer Indexcode" + getIndexCode());
		if(table == null || table.length()==0){
			//System.out.println("inner Indexcode: "+getIndexCode());
			table = "sample_" + Integer.toString(getIndexCode());
			return table;
		}
		else return table;
	}
	
	public int getDimensionNumber(){
		return dimensions.size();
	}
	
	public String getDimensionAt(int idx){
		return dimensions.get(idx);
	}
	
	public String getValueAt(int idx){
		return values.get(idx);
	}
	
	public void addDimensionColumn(String column, String value){
		dimensions.add(column);
		values.add(value);
	}
	
	public GroupPredicate extendColumn(String column, String value){
		GroupPredicate newgp = new GroupPredicate();
		for(int i=0; i<dimensions.size(); i++){
			newgp.addDimensionColumn(dimensions.get(i), values.get(i));
		}
		newgp.addDimensionColumn(column, value);
		return newgp;
	}
	
	public void RemoveColumn(String column){
		dimensions.remove(column);
	}
	
	/**
	 * change the order of two columns will result in a repartitioning of the data
	 * @param column1
	 * @param column2
	 */
	public void ExchangeOrder(String column1, String column2){
		int idx1 = dimensions.indexOf(column1);
		int idx2 = dimensions.indexOf(column2);
		String v1 = values.get(idx1);
		String v2 = values.get(idx2);
		if(idx1!=-1 && idx2!=-1){
			dimensions.add(idx1, column2);
			values.add(idx1, v2);
			dimensions.remove(idx1+1);
			values.remove(idx1+1);
			
			dimensions.add(idx2, column1);
			values.add(idx2, v1);
			dimensions.remove(idx2+1);
			values.remove(idx2+1);
		}

	}
	
	public void setTableName(String tname){
		table = tname;
	}
	
	public int getIndexCode(){
		String code = table + "_";
		for(int i=0; i<dimensions.size(); i++){
			code +=  dimensions.get(i) + "_" + values.get(i);
		}
		//System.out.println("code : " + code);
		return Math.abs(code.hashCode());
	}
	
	public boolean isEqual(GroupPredicate gp){
		int size = gp.getDimensionNumber();
		
		if(size == dimensions.size()){
			for(int i=0; i<size; i++){
				if(!dimensions.get(i).equals(gp.getDimensionAt(i)) || !values.get(i).equals(gp.getValueAt(i)))
					return false;
			}
			return true;
		}
		else return false;
	}
	
	public String toString(){
		String reval = getIndexCode() + "@";
		for(int i=0; i<dimensions.size(); i++){
			reval +=  dimensions.get(i) + "_" + values.get(i);
		}
		return reval;
	}
	
	public boolean isContained(String groupColumn){
		return dimensions.contains(groupColumn);
	}

}
