package com.test.classify;

import org.junit.Before;
import org.junit.Test;

import com.machine.classify.main.Classify;


public class InitialTest {

	private Classify classify=new Classify();
	@Before
	public void initialize()
	{
		classify.initialize();
	}
	
	@Test
	public void testEnglish()
	{
		/*testing english*/
		
		classify.predictOutput("disgusting food");
	}
	
	@Test
	public void testGerman()
	{
		
		/*testing german*/
		
		classify.predictOutput("Lustige Geschichten und drollige Bilder mit ");
	}
	
	@Test
	public void tetsFrench()
	{
		classify.predictOutput("La Négritude ");
	}
}
