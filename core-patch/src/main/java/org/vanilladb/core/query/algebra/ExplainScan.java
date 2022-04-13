package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class ExplainScan implements Scan {
	private Schema schema;
	private String explainRes;
	private int totalRecs;
	private boolean isFirst;

	public ExplainScan(Scan s, String explainRes, Schema schema) {
		this.explainRes = "\n" + explainRes;
		this.schema = schema;
		
		s.beforeFirst();
		while (s.next()) totalRecs++;
		s.close();
		
		this.explainRes = this.explainRes + "\nActual #recs: " + this.totalRecs;
		this.isFirst = true;
	}

	@Override
	public void beforeFirst() {
		isFirst = true;
	}

	@Override
	public boolean next() {
		if(isFirst) {
			isFirst = false;
			return true;
		}
		
		return isFirst;
	}

	@Override
	public void close() {

	}

	@Override
	public Constant getVal(String fldName) {
		if (fldName.equals("query-plan"))
			return new VarcharConstant(this.explainRes);
		else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	@Override
	public boolean hasField(String fldName) {
		return schema.hasField(fldName);
	}
}
