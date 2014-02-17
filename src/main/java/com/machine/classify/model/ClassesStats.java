package com.machine.classify.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.machine.classify.database.DBConnection;
import com.machine.classify.utils.COL;


/**
 * 
 * @author Vipin Kumar
 * total no of documents for each Class (English ,French..)
 */
public class ClassesStats {
	
	
	
	private static final Log LOG = LogFactory.getLog(ClassesStats.class);
	
	/*these variable are till jobs are running independently when get combined we can remove these*/
	public  static int englishTotal;
	public  static int frenchtotal;
	public  static int germanTotal;
	
	public static void intsertDataForClasses()
	{
		
			try {
				
				Connection conn=DBConnection.getConnection();
				PreparedStatement stmt=conn.prepareStatement("insert into classes values (default,?,?,?)");
				stmt.setInt(1, english.get());
				stmt.setInt(2, french.get());
				stmt.setInt(3, german.get());
				
				stmt.executeUpdate();
			}  catch (SQLException e) {
				LOG.error("not able to get Connection", e);
			}
		
	}
	
	
	public static void getClassesData()
	{
		Connection conn=DBConnection.getConnection();
		try {
			Statement stmt=conn.createStatement();
			
			ResultSet rstmt=stmt.executeQuery("SELECT * FROM classify.classes");
			while(rstmt.next())
			{
			englishTotal=rstmt.getInt("english");
			frenchtotal=rstmt.getInt("french");
			germanTotal=rstmt.getInt("german");
			}
			
		} catch (SQLException e) {
			LOG.error("sql exception in creating statement", e);
		}
	}
	
	
	
	
	
	/**
	 * counts for no of documents from each language
	 */
	public final static AtomicInteger english = new AtomicInteger(0);
	public final static AtomicInteger french = new AtomicInteger(0);
	public final static AtomicInteger german = new AtomicInteger(0);
	
	
	public static int  getTotal()
	{
		return englishTotal+frenchtotal+germanTotal;
	}
	
	public static int getClassTotal(String colname) {

		switch (COL.valueOf(colname)) {

		case English:
			return englishTotal;
		case French:
			return frenchtotal;
		case German:
			return germanTotal;
		}
		return -1;

	}
}
