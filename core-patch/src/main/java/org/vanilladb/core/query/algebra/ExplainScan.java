package org.vanilladb.core.query.algebra;


import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.*;

public class ExplainScan implements Scan {
	
	boolean isBeforeFirst;
	String explainString;
	Schema schema;
	Scan s;
	
	public ExplainScan(Scan s, Schema sche, String exp)
	{
		this.s = s;
		int total = 0;
		explainString = "\n" + exp;
		this.schema = schema;
		s.beforeFirst();
		while(s.next())
			total++;
		s.close();
		
		explainString += "\n" + "Actual #recs: " + total;
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
		
		return fldName == "query-plan";
	}
	
	@Override
	public Constant getVal(String fldName)
	{
		if(fldName.equals("query-plan"))
			return new VarcharConstant(explainString);
		else
			return new VarcharConstant("no" + fldName);
		
	}
}
