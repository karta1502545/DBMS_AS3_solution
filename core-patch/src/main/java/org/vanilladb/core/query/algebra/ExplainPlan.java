package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;


public class ExplainPlan implements Plan{
	private Plan p;

	/**
	 * Creates a leaf node in the query tree corresponding to the specified
	 * table.
	 * 
	 * @param tblName
	 *            the name of the table
	 * @param tx
	 *            the calling transaction
	 */
	public ExplainPlan(Plan p) {
		this.p=p;
	}

	/**
	 * Creates a table scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		return new ExplainScan(p.open(),p.blocksAccessed(), schema(), p.toString());
		//return new SortScan(runs, comp);
		//return new ExplainScan(ti, tx);
	}

	/**
	 * Estimates the number of block accesses for the table, which is obtainable
	 * from the statistics manager.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Determines the schema of the table, which is obtainable from the catalog
	 * manager.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		Schema schema= new Schema();
		schema.addField("query-plan", Type.VARCHAR(500));
		return schema;
	}

	/**
	 * Returns the histogram that approximates the join distribution of the
	 * field values of query results.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		return p.histogram();
	}

	@Override
	public long recordsOutput() {
		//return (long) histogram().recordsOutput();
		return 1;
	}


}
