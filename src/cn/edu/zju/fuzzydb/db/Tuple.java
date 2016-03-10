package cn.edu.zju.fuzzydb.db;

import java.util.ArrayList;
/**
 * a database record, no strong type checking.
 * @author wusai
 *
 */
public class Tuple {
	private ArrayList<String> values;
	public String getValue(int i){
		return values.get(i);
	}
	public void setValue(int i,String value){
		values.set(i, value);
	}
	
}
