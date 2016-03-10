package cn.edu.zju.fuzzydb.file;

/**
 * the default settings
 * @author wusai
 *
 */
public class Property {
	
	public static String dbName = "h2db";
	
	/**
	 * maintain how samples are maintained in different db tables.
	 */
	public static String configTable = "config";
	
	/**
	 * the table records the sampling progress
	 */
	public static String progressTable = "progress";
	
	/**
	 * original data (after joining the dimensional tables with the fact table)
	 */
	public static String baseTable = "base";
	
	/**
	 * fetch the samples batch
	 */
	public static int sampleBatchSize = 5000;
	
	/**
	 * the max dimension columns
	 */
	public static int maxDimensionColumn = 6;
	
	/**
	 * if the table of prefixNode contains samples greater than maxNodeSampleSize, it will split
	 */
	public static int maxNodeSampleSize = 100000;
	
	public static int maxLruNodes = 1000;
	
	public static int minEstimatorCountSize = 100;

}
