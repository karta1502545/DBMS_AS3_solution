package org.vanilladb.core.query.algebra;


import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.*;

public class ExplainScan implements Scan {
	
	private boolean isBeforeFirst;
	private String explainRecord;
	private Scan s;
	private Schema schema;
	
	public ExplainScan(Scan s, Schema sche, String exp)
	{
		this.s = s;
		this.schema = sche;
		int total = 0;
		explainRecord = "\n";
		explainRecord += exp;
		s.beforeFirst();
		while(s.next())
			total++;
		s.close();
		
		explainRecord += "Actual #recs: " + total;
	}
	
	@Override
	public void beforeFirst() {
		isBeforeFirst = true;
	}

	@Override
	public boolean next() {
		if(isBeforeFirst)
		{
			isBeforeFirst = false;
			return true;
		}
		else
		{
			return false;
		}
	}

	@Override
	public void close() {
		s.close();
	}

	@Override
	public boolean hasField(String fldName) {
		
		return fldName.equals("query-plan");
	}
	
	@Override
	public Constant getVal(String fldName)
	{
		if(fldName.equals("query-plan")) {
			return new VarcharConstant(explainRecord);
		}
		else
			return new VarcharConstant("no " + fldName);
//			throw new RuntimeException("field " + fldName + " not found.");
	}
}
