package com.machine.classify.main;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.machine.classify.database.DBConnection;
import com.machine.classify.model.ClassesStats;
import com.machine.classify.utils.COL;

/**
 * 
 * @author Vipin Kumar
 *
 */
public class Classify {

	private static final Log LOG = LogFactory.getLog(Classify.class);
	private static Connection connection;
	
	/**
	 * probabilities of different classes
	 */
	private Map<String, Double> ClassProbability=new HashMap<String, Double>();
	
	/**
	 * get the different class total from database
	 */
	public void initialize()
	{
		ClassesStats.getClassesData();
		
	}
	
	public void predictOutput(String text)
	{
		Map<String, Integer>  tokens=new HashMap<String, Integer>();
		/*counts the tokens */
		countAttributes(text, tokens);
		
		evaluateClassProbability();
		
		 	String Class;
	        String attribute;
	        Integer noOfOccurrence=0;
	        Double classprob;
	        
	        String maxScoreCategory = null;
	        Double maxScore=Double.NEGATIVE_INFINITY;
	        
	        
	        for(Map.Entry<String, Double> entry : ClassProbability.entrySet()) {
	        	Class = entry.getKey();
	        	classprob = entry.getValue(); 
	            
	           
	            for(Map.Entry<String, Integer> entry1 : tokens.entrySet()) {
	            	attribute = entry1.getKey();
	            	
	                Double attributeProb=getAttributeProb(attribute, Class);
	                if(attributeProb==null)
	                	continue;
	                
	                noOfOccurrence = entry1.getValue(); //get its occurrences in text
	                
	                classprob += noOfOccurrence*attributeProb; 
	            }
	           
	            
	            if(classprob>maxScore) {
	                maxScore=classprob;
	                maxScoreCategory=Class;
	            }
	        }
	        
	       System.out.println("Detected :"+   "  "+ text + " as " + maxScoreCategory);
		
	}
	
	/**
	 * tokenize to get count of each word
	 * @param text
	 * @param tokens
	 */
	public void countAttributes(String text,Map<String, Integer>  tokens)
	{
		StringTokenizer tokenizer=new StringTokenizer(text);
		
		while(tokenizer.hasMoreTokens())
		{
			String token=tokenizer.nextToken();
			if(tokens.containsKey(token))
			{
				int sum=tokens.get(token);
				tokens.put(token, ++sum);
				System.out.println(token+  "  "+ sum);
			}
			else
				tokens.put(token, 1);
			
		}
	}
	
	/**
	 * calculate the probability of each class P(C)
	 */
	public void evaluateClassProbability()
	{
		for(COL col:COL.values())
		{
	ClassProbability.put(col.toString(),calculateClassProbability(ClassesStats.getClassTotal(col.toString())) );
		}
	}
	
	public Double calculateClassProbability(int typeTotal)
	{
		double d=Math.log((double)typeTotal/ClassesStats.getTotal());
		return d;
	}
	
	
	/**
	 * get P(X|C)  conditional probability of  attribute from database
	 * @param attribute
	 * @return
	 */
	public Double getAttributeProb(String attribute,String Class)
	{
		Connection conn=getConnection();
		Double value = null;
		try {
			Statement stmt=conn.createStatement();
			
			ResultSet rstmt=stmt.executeQuery("SELECT * FROM classify.attribute_probability  where attribute= \""+attribute+"\"");
			while(rstmt.next())
			{
				value=rstmt.getDouble(Class.toLowerCase());
			}
			
		} catch (SQLException e) {
			LOG.error("sql exception in creating statement", e);
		}
		return value;
	}
	
	
	public Connection getConnection()
	{
		if(connection==null)
		connection=DBConnection.getConnection();
		
			return connection;
	}
	
}
