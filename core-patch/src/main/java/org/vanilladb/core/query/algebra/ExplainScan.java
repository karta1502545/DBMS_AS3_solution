package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class ExplainScan implements Scan {
	private Scan s;
	private String explain_msg ;
	private boolean before_first;
	
	public ExplainScan(Scan s, String explain_msg) {
		this.s = s;
		this.explain_msg = explain_msg ;
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();
		before_first = true ;
	}

	@Override
	public boolean next() {
		if (before_first) {
			before_first = false ;
			return true ;
		}
		return false ;
	}

	@Override
	public void close() {
		s.close();
	}
	
	@Override
	public Constant getVal(String fldName) {
		if (hasField(fldName)) {
			int actual_recs = 0 ;
			StringBuilder sb = new StringBuilder();
			while (s.next()) {
				actual_recs++ ;
			}
			sb.append(explain_msg).append("Actual #recs: " + actual_recs + "\n") ;
			return new VarcharConstant(sb.toString()) ;
		}
		else
			throw new RuntimeException("field " + fldName + " not found in explain.");
	}

	@Override
	public boolean hasField(String fldName) {
		return fldName.equals("query-plan") ;
	}
}
