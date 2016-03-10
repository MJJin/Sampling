package cn.edu.zju.fuzzydb.estimator;

import java.util.Map;

import org.apache.commons.math3.distribution.*;

import cn.edu.zju.fuzzydb.file.Property;

public class AverageEstimator {
	/**
	 *count how many samples have scanned
	 */
	private int Count;
	/**
	 * the Confidence that user set
	 */
	private double Confidence;
	
	/**
	 * the Interval that user set
	 */
	private double errorRate;

	/**
	 * after scanning Count samples, the current estimator
	 */
	private double estimator;
	/**
	 * after scanning Count samples, the current errorRate
	 */
	private double currentErrorRate;
	
	private double factor;
	
	private boolean isEnough;
	
	private Map<String,String> conditions;
	
	
	/**
	 * the constructor
	 * @param count
	 * @param confidence
	 * @param errorrate
	 */
	public AverageEstimator(double confidence,double errorrate){
		this.Confidence = confidence;
		this.Count = 0;
		this.errorRate = errorrate;
		estimator = 0;
		isEnough = false;
		NormalDistribution normal = new NormalDistribution(0,1);
		factor = normal.inverseCumulativeProbability((1 + Confidence)/2);
	}
	
	public void setConditions(Map<String,String> condition){
		for(Map.Entry<String, String> entry:condition.entrySet()){
			conditions.put(entry.getKey(), entry.getValue());
		}
	}
	
	public Map<String,String> getConditions(){
		return conditions;
	}
	
	public void setCount(int count){
		Count = count;
	}
	
	public int getCount(){
		return Count;
	}
	
	public double getConfidence() {
		return Confidence;
	}

	public void setConfidence(double confidence) {
		Confidence = confidence;
	}
	public double getErrorRate() {
		return errorRate;
	}

	public void setErrorRate(double errorrate) {
		errorRate = errorrate;
	}
	
	public double getCurrentErrorRate(){
		return currentErrorRate;
	}
	
	public double getEstimator(){
		return estimator;
	}
	
	public void addSample(double sample){
		estimator = (estimator * Count + sample) /(Count + 1);
		Count = Count + 1;
		currentErrorRate = factor * estimator / Math.sqrt(Count);
		//System.out.println("add sample: " + currentErrorRate +" "+errorRate + " "+Count );
		if(currentErrorRate < errorRate && Count > Property.minEstimatorCountSize){
			isEnough = true;
			System.out.print("true");
			return;
		}
	}
	
	public void batchAddSample(double sample){
		return;
	}
	
	public void addSample(double sample[],int count){
		for(int i = 0;i < count/Property.sampleBatchSize+1;i++){
			double sum = 0;
			for(int j = 0; j < Property.sampleBatchSize && j < count-(i*Property.sampleBatchSize);j++){
				sum += sample[i*Property.sampleBatchSize + j];
			}
			estimator = (estimator * Count + sum)/(Count + Property.sampleBatchSize);
			currentErrorRate = factor * estimator / Math.sqrt(Count);
			if(currentErrorRate < errorRate && Count > Property.sampleBatchSize){
				isEnough = true;
				return;
			}
		}
	}
	
	public boolean getIsEnough(){
		return isEnough;
	}
}
