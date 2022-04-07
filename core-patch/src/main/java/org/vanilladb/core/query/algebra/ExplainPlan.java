package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class ExplainPlan implements Plan{
	private Plan p;
	private Transaction tx;
	public ExplainPlan(Plan p, Transaction tx){
		this.p = p;
		this.tx =tx;
	}
	@Override
	public Scan open() {
		
	}
	@Override
	public long blocksAccessed() {
		
	}
	@Override
	public Schema schema() {
		
	}
	@Override
	public Histogram histogram() {
		
	}
	@Override
	public long recordsOutput() {
		
	}
}
