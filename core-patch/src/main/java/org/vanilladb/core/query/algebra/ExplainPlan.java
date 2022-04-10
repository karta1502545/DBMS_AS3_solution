package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Type;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class ExplainPlan implements Plan{
	private Plan p;
	private Schema schema = new Schema();
	
	
	
	public ExplainPlan(Plan p){
		schema.addField("query-plan", Type.VARCHAR(500));
		this.p = p;
	}
	
	@Override
	public Scan open() {
		Scan s = p.open();
		return new ExplainScan(s, schema, p.recordData());
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
		
		return (long)histogram().recordsOutput();
	}
	
	@Override
	public String recordData()
	{
		return p.recordData();
	}
		
	
}
