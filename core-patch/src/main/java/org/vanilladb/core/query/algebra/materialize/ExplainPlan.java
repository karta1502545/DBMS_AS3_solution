package org.vanilladb.core.query.algebra.materialize;

import java.util.Set;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.remote.jdbc.RemoteResultSet;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class ExplainPlan implements Plan {
	
	private Schema schema = new Schema();
	private Transaction tx;
	private UpdateScan us;
//	public String rec;
	
//	public ExplainPlan(Transaction tx) {
//		this.tx = tx;
//		this.schema.addField("query-plan", Type.VARCHAR(64));
//		TempTable result = new TempTable(schema, tx);
//		us = result.open();
//		this.rec = "\n";
//	}
	public ExplainPlan(Transaction tx, String xStr) {
		this.tx = tx;
		this.schema.addField("query-plan", Type.VARCHAR(500));
		TempTable result = new TempTable(schema, tx);
		us = result.open();
		Constant c = Constant.newInstance(
			Type.newInstance(java.sql.Types.VARCHAR, 500)
			, (xStr).getBytes());
		us.insert();
		us.setVal("query-plan", c);
	}
	
//	public ExplainPlan(Plan p, String nextPlan, long blks, long recs, String lastPlans) {
//		this.schema.addField("query-plan", Type.VARCHAR(64));
//		this.us = (UpdateScan) p.open();
//		this.rec = nextPlan + "(#blks= " + blks +", #recs=" + recs + ")\n" + lastPlans;
//		Constant c = Constant.newInstance(
//				Type.newInstance(java.sql.Types.VARCHAR, 64)
//				, (nextPlan + "(#blks= " + blks +", #recs=" + recs + ")\n").getBytes());
//		us.beforeFirst();
//		us.insert();
//		us.setVal("query-plan", c);
//	}
//	public ExplainPlan(Plan p, String xStr) {
//		this.schema.addField("query-plan", Type.VARCHAR(64));
//		this.us = (UpdateScan) p.open();
//		Constant c = Constant.newInstance(
//			Type.newInstance(java.sql.Types.VARCHAR, 500)
//			, (xStr).getBytes());
//		us.insert();
//		us.setVal("query-plan", c);
//	}

	@Override
	public Scan open() {
		// TODO Auto-generated method stub
		return us;
	}

	@Override
	public long blocksAccessed() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Schema schema() {
		// TODO Auto-generated method stub
		return schema;
	}

	@Override
	public Histogram histogram() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long recordsOutput() {
		// TODO Auto-generated method stub
		return 0;
	}

}
