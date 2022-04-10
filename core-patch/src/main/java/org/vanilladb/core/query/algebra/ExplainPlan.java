package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class ExplainPlan implements Plan{
	private Plan p;
	private Transaction tx;
	private Histogram hist;
	private Schema schema = new Schema();
	
	
	
	public ExplainPlan(Plan p, Transaction tx){
		this.p = p;
		this.tx =tx;
		
	}
	
	@Override
	public Scan open() {
		Scan s = p.open();
		return new ExplainScan(s, schema, p.toString());
	}
	
	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}
	
	
	@Override
	public Schema schema() {
		schema.addField("query-plan", Type.VARCHAR(500));
		return schema;
	}
	
	@Override
	public Histogram histogram() {
		return hist;
	}
	
	@Override
	public long recordsOutput() {
		
		return (long)histogram().recordsOutput();
	}
}
