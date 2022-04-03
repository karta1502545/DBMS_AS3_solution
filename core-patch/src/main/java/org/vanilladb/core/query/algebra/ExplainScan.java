package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;

public class ExplainScan implements Scan {
	private Scan s;
	private ExplainPlan explainPlan;
	private String queryData = ""; // The result of the explain query
	private static int recordsAccessed;
	

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
		return Constant.newInstance(Type.VARCHAR(500), queryData.getBytes());
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();
	}

	@Override
	public boolean next() {
		if(queryData.isEmpty()) {
			recordsAccessed = 0; // Clear records
			while(s.next()); // Keep invoking next() to count records accessed.
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
		return fldName == "query-data";
	}
	
	public static void addRecordCount(int c) {
		if(c < 0) throw new IllegalArgumentException();
		recordsAccessed += c;
	}
	
	

}
