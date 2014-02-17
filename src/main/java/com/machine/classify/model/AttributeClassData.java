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
 *
 */
public class AttributeClassData {
	private static final Log LOG = LogFactory.getLog(AttributeClassData.class);

	
	/**
	 * need to be removed once all the jobs running together
	 */
	public  static int englishAttributeTotal;
	public  static int frenchAttributetotal;
	public  static int germanAttributeTotal;
	public  static int attributeTotal;
	
	public static void intsertDataForAttributes()
	{
			try {
				Connection conn=DBConnection.getConnection();
				PreparedStatement stmt=conn.prepareStatement("insert into  classify.attributes values (default,?,?,?,?)");
				stmt.setInt(1, englishAttributes.get());
				stmt.setInt(2, frenchAttributes.get());
				stmt.setInt(3, germanAttributes.get());
				stmt.setInt(4, totalattributes.get());
				stmt.executeUpdate();
			}  catch (SQLException e) {
				LOG.error("not able to get Connection", e);
			}
	}
	
	
	public static void getAttributeData()
	{
		Connection conn=DBConnection.getConnection();
		try {
			Statement stmt=conn.createStatement();
			
			ResultSet rstmt=stmt.executeQuery("SELECT * FROM classify.attributes");
			while(rstmt.next())
			{
			englishAttributeTotal=rstmt.getInt("english");
			frenchAttributetotal=rstmt.getInt("french");
			germanAttributeTotal=rstmt.getInt("german");
			attributeTotal=rstmt.getInt("totalattribute");
			}
			
		} catch (SQLException e) {
			LOG.error("sql exception in creating statement", e);
		}
	}
	
	
	public final static AtomicInteger totalattributes = new AtomicInteger(0);
	
	/**
	 * counts for no of feature present in each language
	 */
	public final static AtomicInteger englishAttributes = new AtomicInteger(0);
	public final static AtomicInteger frenchAttributes = new AtomicInteger(0);
	public final static AtomicInteger germanAttributes = new AtomicInteger(0);
	
	
	public static int getAttributeTotal(String colname) {

		switch (COL.valueOf(colname)) {

		case English:
			return englishAttributeTotal;
		case French:
			return frenchAttributetotal;
		case German:
			return germanAttributeTotal;
		}
		return -1;

	}
	
}
