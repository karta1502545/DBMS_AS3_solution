package org.vanilladb.core.query.algebra;


import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.query.parse.BadSyntaxException;
import org.vanilladb.core.query.planner.BadSemanticException;
import org.vanilladb.core.sql.*;

public class ExplainScan implements Scan {
	
	private boolean isBeforeFirst;
	private String explainRecord;
	private Scan s;
	
	
	public ExplainScan(Scan s, String exp)
	{
		this.s = s;
		explainRecord = "\n" + exp + "Actual #recs: " + actualRun();
		
	}
	
	private int actualRun()
	{
		int total = 0;
		
		s.beforeFirst();
		while(s.next())
			total++;
		s.close();
		
		return total;
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
		/// dummy method
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
