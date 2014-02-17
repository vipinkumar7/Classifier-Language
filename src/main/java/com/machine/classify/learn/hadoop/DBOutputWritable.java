package com.machine.classify.learn.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

/**
 * 
 * @author Vipin Kumar
 * Class is responsible for pushing calculated probabilities to databases
 *
 */
public class DBOutputWritable implements Writable, DBWritable {
	
	private final String attribute;
	private final Double english;
	private final Double french;
	private final Double german;

	public DBOutputWritable(String attribute, Double english,Double french,Double german) {
		this.attribute = attribute;
		this.english=english!=null?english:0.0;;
		this.french=french!=null?french:0.0;
		this.german=german!=null?german:0.0;
	}

	public void readFields(DataInput in) throws IOException {

	}

	public void readFields(ResultSet rs) throws SQLException {

	}

	public void write(DataOutput out) throws IOException {
	}

	public void write(PreparedStatement ps) throws SQLException {
		ps.setString(1, attribute);
		ps.setDouble(2,english);
		ps.setDouble(3, french);
		ps.setDouble(4, german);
	}

}
