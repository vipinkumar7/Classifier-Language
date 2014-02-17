package com.machine.classify.chisquare.hadoop;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.machine.classify.model.AttributeClassData;
import com.machine.classify.model.ClassesStats;
import com.machine.classify.utils.COL;;

/**
 * 
 * @author Vipin Kumar
 *
 */
public class ChiSquareReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>{

	
	
	/*
	 *      |  C    |   NotC  | Total
	 * A    |   A   |   B      | A+B
	 * 
	 * NotA |   C   |    D     | C+D
	 * 
	 * Total|  A+C  |    B+D   | N
	 */
	private Text valuesWithouAttribute=new Text(); 
	
	private StringBuilder builder=new StringBuilder();
	
	
	/*
	 * (non-Javadoc)
	 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
	 */
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> ctx, Reporter reporter) throws IOException {
	        
	       
	        String Class;
	        int A_Has, A_NotHas, NotA_NotC, NotA_C, A_NotC, C_A;
	        double chisquareCriticalValue=6.63;
	        double chisquareSum=0.0;
	       // double previousSum=0.0; not needed for now
	        boolean push =false;
	        
	        
		if(values.hasNext())
		{
			String splitarr[] = values.next().toString().split("\t");
			/*traversing through all the classes in which particular attribute found*/
			
			Map<String,Integer> classesValues=getAllClassesForAttribute(splitarr);
			
			/*calculate the A_Has. ( times  the attribute found in different classes)*/
            A_Has = 0;
            for(Integer count : classesValues.values()) {
            	A_Has+=count;
            }
            //also the A_NotHas. ( times  the attribute not found in different classes)
            A_NotHas = ClassesStats.getTotal() - A_Has;
            
			 for(Map.Entry<String, Integer> corresClasses : classesValues.entrySet()) {
				
				Class=corresClasses.getKey();
				
		            
		            C_A = corresClasses.getValue(); //C_A  have the attribute and belong on the specific class
		            NotA_C = ClassesStats.getClassTotal(Class)-C_A; //NotA_C  do not have the particular attribute BUT they belong to the specific class
	                
		            NotA_NotC = A_NotHas - NotA_C; //NotA_NotC  don't have the attribute and don't belong to the specific class
		            A_NotC = A_Has - C_A; //A_NotC   have the attribute and don't belong to the specific class
				
		            //calculate the chisquare 
	               chisquareSum = ClassesStats.getTotal()*
	             Math.pow(C_A*NotA_NotC-A_NotC*NotA_C, 2)/((C_A+NotA_C)*(C_A+A_NotC)*(A_NotC+NotA_NotC)*(NotA_C+NotA_NotC));
	                
	                 //if the score is larger than the critical value then add it in the list
	                if(chisquareSum>=chisquareCriticalValue) {
	                  push=true;
	                  break;
	                   /* if(chisquareSum>previousSum) {
	                        previousSum=chisquareSum;
	                    }*/
	                }
			}
			 
			 if(push)
			 {
				 valuesWithouAttribute.set(builder.toString());
				 recordAttribuesCounts(classesValues);
				 ctx.collect(key, valuesWithouAttribute);
			 }
		}
	}
	
	/**
	 * 
	 * @param splitarr
	 * @return
	 */
	public Map<String, Integer> getAllClassesForAttribute(String[] splitarr)
	{
		Map<String,Integer> classesValues=new HashMap<String, Integer>();
		builder.delete(0, builder.length());
		for (int i = 1; i < splitarr.length-1; i+=2) {
			classesValues.put(splitarr[i], Integer.valueOf(splitarr[i+1]));
			builder.append(splitarr[i]+"\t"+splitarr[i+1]+"\t");
		}
		return classesValues;
		
	}
	
	/**
	 * 
	 * @param oneAttribute
	 */
	public void recordAttribuesCounts(Map<String, Integer> oneAttribute)
	{
		/*increment the feature count for any feature   */
		if(oneAttribute.size()>0)
			AttributeClassData.totalattributes.incrementAndGet();
		
		 for(Map.Entry<String, Integer> corresClasses : oneAttribute.entrySet()) {
			 if(corresClasses.getKey().equals(COL.English.toString()))
			 {
				 AttributeClassData.englishAttributes.addAndGet(corresClasses.getValue());
			 }
			 else if(corresClasses.getKey().equals(COL.French.toString()))
			 {
				 AttributeClassData.frenchAttributes.addAndGet(corresClasses.getValue());
			 }
			 else if(corresClasses.getKey().equals(COL.German.toString()))
			 {
				 AttributeClassData.germanAttributes.addAndGet(corresClasses.getValue());
			 }
		 }
	}

	
	
}
