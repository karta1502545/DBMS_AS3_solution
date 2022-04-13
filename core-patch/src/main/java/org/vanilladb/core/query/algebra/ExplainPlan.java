package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

public class ExplainPlan implements Plan {
	
	private Plan plan;
	
	public ExplainPlan(Plan plan) {
		this.plan = plan;
	}

	@Override
	public Scan open() {
		return new ExplainScan(plan.open(), schema(), plan.toString());
	}

	@Override
	public long blocksAccessed() {
		return plan.blocksAccessed();
	}

	@Override
	public Schema schema() {
		Schema schema = new Schema();
		schema.addField("query-plan", Type.VARCHAR(500));
		return schema;
	}

	@Override
	public Histogram histogram() {
		return plan.histogram();
	}

	@Override
	public long recordsOutput() {
//		return (long) histogram().recordsOutput();
		return 1;
	}

}
