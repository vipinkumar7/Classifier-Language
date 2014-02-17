package com.machine.classify.learn.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.machine.classify.model.AttributeClassData;
import com.machine.classify.utils.COL;

/**
 * 
 * @author Vipin Kumar
 * 
 * This class is Responsible for calculating the P(X|C)  **** probability of a feature given its belong to class C 
 *
 */
public class ConditionalProbabilityReducer  extends Reducer<Text, Text, DBOutputWritable, NullWritable>{

	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<Text> values,Context ctx)
			throws IOException, InterruptedException {
		
		Map<String, Double>  probab=new HashMap<String, Double>();
		initializeProbability(probab);
		Iterator<Text> it=values.iterator();
		if(it!=null&&it.hasNext())
		{
			evaluateProbability(it.next().toString(), probab);
		ctx.write(new DBOutputWritable(key.toString(),
				probab.get(COL.English.toString()),probab.get(COL.French.toString()),probab.get(COL.German.toString())),NullWritable.get());
		}
		
	}
	
	public void initializeProbability(Map<String, Double>  probab)
	{
		probab.put(COL.English.toString(), 0.0);
		probab.put(COL.French.toString(), 0.0);
		probab.put(COL.German.toString(), 0.0);
	}
	/**
	 * 
	 * @param textRow
	 * @param probab
	 */
	public void evaluateProbability(String textRow,Map<String, Double>  probab)
	{
		String splitarr[] = textRow.split("\t");
		for (int i = 1; i < splitarr.length-1; i+=2) {
			probab.put(splitarr[i].trim(),Double.valueOf(splitarr[i+1]) );
		}
		
		for(Entry<String, Double> entry:probab.entrySet())
			probab.put(entry.getKey(), getProbability(entry.getKey(), entry.getValue()));
	}
	
	/**
	 * 
	 * @param Class
	 * @param noOfTimes
	 * @return
	 */
	public Double getProbability(String Class,Double noOfTimes)
	{
		
		return Math.log((noOfTimes+1.0)/((double)AttributeClassData.getAttributeTotal(Class)+(double)AttributeClassData.attributeTotal));
		 
		
	}
}
