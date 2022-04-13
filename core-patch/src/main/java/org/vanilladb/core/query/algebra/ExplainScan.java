package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;

public class ExplainScan implements Scan {
	private Scan s;
	private ExplainPlan explainPlan;
	private String queryData = ""; // The result of the explain query
	

	/**
	 * Creates an explain scan having the specified underlying scan.
	 * 
	 * @param s
	 *            the scan of the underlying query
	 * @param explainPlan
	 * 			  the explainPlan to derive the explain query's final result from
	 */
	public ExplainScan(Scan s, ExplainPlan explainPlan) {
		this.s = s;
		this.explainPlan = explainPlan;
	}

	@Override
	public Constant getVal(String fldName) {
		if(hasField(fldName))
			return new VarcharConstant(queryData, Type.VARCHAR(500));
		else 
			throw new RuntimeException("field " + fldName + " not found.");
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();
		queryData = "";
	}

	@Override
	public boolean next() {
		if(queryData.isEmpty()) {
			int recordsAccessed = 0; // Clear records
			while(s.next()) recordsAccessed++; // Keep invoking next() to count records accessed.
			StringBuilder sb = new StringBuilder();
			explainPlan.explain(sb, 0); // Guarantees to make sb have length greater than 0, because there is at least the explain plan.
			sb.append("Actual #recs: ")
			.append(recordsAccessed)
			.append('\n');
			queryData = sb.toString();
			return true;
		}
		else return false;
	}

	@Override
	public void close() {
		s.close();
	}

	@Override
	public boolean hasField(String fldName) {
		return fldName.contentEquals("query-plan");
	}
	
	

}
