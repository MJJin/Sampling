package cn.edu.zju.fuzzydb.replacement;

import java.util.LinkedHashMap;
import java.util.Map;
import cn.edu.zju.fuzzydb.file.PrefixTreeNode;
import cn.edu.zju.fuzzydb.file.Property;



public class LRUCache extends LinkedHashMap<String,PrefixTreeNode> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Runtime runtime; 
	//static final int MAXIMUM_CAPACITY = 1 << 29;
	
    public LRUCache() {
        super((int) Math.ceil(Property.maxLruNodes / 0.75) + 1, 0.75f, true);
        runtime = Runtime.getRuntime();
    }


	@Override
    protected boolean removeEldestEntry(Map.Entry<String,PrefixTreeNode> eldest) {
    	if(runtime.totalMemory() - runtime.freeMemory() >= runtime.maxMemory() * 0.8){
    		if(eldest != null)
    		{
    			//System.out.println("removeEldestEntry :" + eldest.getKey().toString() +  " " + eldest.getValue().toString());
    			eldest.getValue().flush();
    	
    		}
    	}
    	return (runtime.totalMemory() - runtime.freeMemory() >= runtime.maxMemory() * 0.8);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String,PrefixTreeNode> entry : entrySet()) {
            sb.append(String.format("%s:%s ",entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }
}