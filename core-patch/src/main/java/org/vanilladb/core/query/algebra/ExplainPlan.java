package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

public class ExplainPlan implements Plan {
	private Plan p;
	private Schema schema = new Schema();

	/**
	 * Creates a new explain node in the query tree, having the specified
	 * subquery.
	 * 
	 * @param p
	 *            the subquery
	 */
	public ExplainPlan(Plan p) {
		this.p = p;
		schema.addField("query-plan", Type.VARCHAR(500));
	}

	/**
	 * Creates an explain scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		Scan s = p.open();
		return new ExplainScan(s, this);
	}

	/**
	 * Estimates the number of block accesses in the projection, which is the
	 * same as in the underlying query.
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Returns the schema of the projection, which is just { query-plan: VARCHAR(500) }
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
		return schema;
	}

	/**
	 * Returns the underlying subquery's histogram.
	 * 
	 * @see Plan#histogram()
	 */
	@Override
	public Histogram histogram() {
		// Use the child plan's histogram.
		return p.histogram();
	}

	@Override
	public long recordsOutput() {
		return (long) histogram().recordsOutput();
	}
	
	@Override
	public void explain(StringBuilder sb, int numIndents) {
		ExplainPlan.explainNode(
			this, 
			sb, 
			numIndents
		);
		p.explain(sb, numIndents); // Recurse to next layer. Because ExplainPlan is always the root, the number of indents is 0.
	}
	
	@Override
	public StringBuilder addOptionalInfo(StringBuilder sb) {
		// No additional info is added for ExplainPlan
		return sb;
	}
	/**
	 * Helper method to explain a single plan node.
	 * The string is prefixed by tabs and suffixed by a newline.
	 * The string contains the plan's class name and the 
	 * @param p the plan node to explain
	 * @param sb the StringBuilder to append the result to
	 * @param numIndents the number of indents to prefix the string with
	 */
	public static void explainNode(Plan p, StringBuilder sb, int numIndents) {
		for(int i = 0; i < numIndents; ++i) sb.append('\t');
		sb.append("->")
		.append(p.getClass().getSimpleName())
		.append(' ');
		p.addOptionalInfo(sb)
		.append(" (#blks=")
		.append(p.blocksAccessed())
		.append(", #recs=")
		.append(p.recordsOutput())
		.append(")\n");
	}
	
	
	
	

}
