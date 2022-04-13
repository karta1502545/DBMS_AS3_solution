package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.VarcharConstant;

public class ExplainScan implements Scan {
	private Scan s;
	private long record_num = 0L;
	private boolean hasExplained = false;
	
	public ExplainScan(Scan s) {
		this.s = s;
	}

	@Override
	public Constant getVal(String fldName) {
		String explain = "";
		// = "->" + ProductPlan + ": (#blks=" + this.blockAccess + ", #records=" + this.recordsOutput + ")\n";
		// let other ascendent do first
//		System.out.println(s);
		while(s.next()) {
			record_num++;
		}
//		explain = explain + "Actual #recs: " + record_num;
		return new VarcharConstant(explain, Type.VARCHAR(500));
	}

	@Override
	public void beforeFirst() {
		s.beforeFirst();

	}

	@Override
	public boolean next() {
		if(!this.hasExplained) {
			this.hasExplained = true;
			return true;
		}
		else {
			return false;
		}
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		s.close();
	}

	@Override
	public boolean hasField(String fldName) {
		// TODO Auto-generated method stub
		if(fldName == "plan-query")
			return true;
		else
			return false;
	}

}