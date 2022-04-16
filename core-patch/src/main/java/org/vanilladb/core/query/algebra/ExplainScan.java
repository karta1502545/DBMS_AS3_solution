package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.VarcharConstant;

public class ExplainScan implements Scan {
	private String result;
	private int numRecs = 0;
	private boolean IsbeforeFirst = true;
	private Schema schema;
	
	public ExplainScan(Plan p, Scan s, Schema schema) {
		this.schema = schema;
		result = "\n"+p.toString();
		
		s.beforeFirst();
		while (s.next())
			numRecs++;
		s.close();
		this.result = result + "Actual #recs: " + numRecs;
	}
	
	@Override
	public void beforeFirst() {
		IsbeforeFirst = true;
	}

	@Override
	public boolean next() {
		if (IsbeforeFirst) {
			IsbeforeFirst = !IsbeforeFirst;
			return true;
		} else {
			return false;
		}
		
	}

	@Override
	public void close() {
		// nothing to do
	}

	/**
	 * Returns the value of the specified field, as a Constant.
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		if(fldName.equals("query-plan"))
			return new VarcharConstant(result);
		else
			throw new RuntimeException("is not query-plan");
	}

	@Override
	public boolean hasField(String fldName) {
		return this.schema.hasField(fldName);
	}
}
