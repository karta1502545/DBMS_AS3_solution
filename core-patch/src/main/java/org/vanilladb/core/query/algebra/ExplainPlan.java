package org.vanilladb.core.query.algebra;


import static org.vanilladb.core.sql.Type.VARCHAR;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

public class ExplainPlan implements Plan {
	
	private Plan p;
	private Schema schema = new Schema();
	
	public ExplainPlan(Plan p) {
		this.p = p;
	}

	
	@Override
	public Scan open() {
		Scan s = p.open();
        schema.addField("query-plan", VARCHAR(500));
		return new ExplainScan(s, schema.fields(), explainOutput(0));
	}

	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	@Override
	public Schema schema() {
		return schema;
	}

	@Override
	public Histogram histogram() {
		return p.histogram();
	}

	@Override
	public long recordsOutput() {
		return (long) p.recordsOutput();
	}

	@Override
	public String explainOutput(int h) {
		String str = "\n" + p.explainOutput(h) + String.format("Actual: #recs = %d", p.recordsOutput());
		return str;
	}

}
