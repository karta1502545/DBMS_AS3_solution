package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

public class ExplainPlan implements Plan {

	private Plan p;
	private Schema schema = new Schema();
	private Histogram hist;

	public ExplainPlan(Plan p) {
		this.p = p;
		this.schema.addField("query-plan", Type.VARCHAR(500));
	}

	@Override
	public Scan open() {
		Scan s = p.open();
		return new ExplainScan(s);
	}

	// since it doesn't do any file manipulation
	@Override
	public long blocksAccessed() {
		return 0;
	}

	@Override
	public Schema schema() {
		return schema;
	}

	@Override
	public Histogram histogram() {
		hist = new Histogram();
		return hist;
	}

	// since it doesn't do any file manipulation
	@Override
	public long recordsOutput() {
		return 0;
	}

}