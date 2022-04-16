// [AS3] ExplainScan
package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;
import static org.vanilladb.core.sql.Type.VARCHAR;

public class ExplainScan implements Scan {
	private Scan s;
	private String planTree;
	private long actualRecords = 0;
	private boolean scanned = false;
	
	public ExplainScan(Scan s, String planTree) {
		this.s = s;
		this.planTree = planTree;
		
		// calculate actually accessed records
		s.beforeFirst();
		while (s.next()) {
			actualRecords++;
		}
		s.beforeFirst();
		this.planTree += "\nActual #recs: " + actualRecords;
	}
	
	@Override
	public Constant getVal(String fldName) {
		if (hasField(fldName))
			return new VarcharConstant(planTree, VARCHAR(500));
		else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	@Override
	public void beforeFirst() {
		scanned = false;
	}

	@Override
	public boolean next() {
		if (!scanned) {
			scanned = true;
			return true;
		} else {
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
}