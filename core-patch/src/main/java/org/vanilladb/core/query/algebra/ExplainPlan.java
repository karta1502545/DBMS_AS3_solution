/*******************************************************************************
 * Copyright 2016, 2017 vanilladb.org contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
//AS3: Sheep Added
package org.vanilladb.core.query.algebra;

import static org.vanilladb.core.sql.Type.VARCHAR;

import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.storage.metadata.statistics.Histogram;

/**
 * The {@link Plan} class corresponding to the <em>explain</em> relational
 * algebra operator.
 */
public class ExplainPlan implements Plan {
	/**
	 * Returns a histogram that, for each field, approximates the value
	 * distribution of products from the specified histograms.
	 * 
	 * @return a histogram that, for each field, approximates the value
	 *         distribution of the products
	 */
	public static Histogram productHistogram(Histogram hist1) {
		return hist1;
	}

	private Plan p;
	private Schema schema = new Schema();
	private Histogram hist;

	/**
	 * Creates a new product node in the query tree, having the two specified
	 * subqueries.
	 * 
	 * @param p
	 *            the left-hand subquery
	 */
	public ExplainPlan(Plan p) {
		this.p = p;
		schema.addField("query-plan", VARCHAR);
		hist = productHistogram(p.histogram());
	}

	/**
	 * Creates a product scan for this query.
	 * 
	 * @see Plan#open()
	 */
	@Override
	public Scan open() {
		return new ExplainScan(this.toString());
	}

	/**
	 * Estimates the number of block accesses in the explain. The formula is:
	 * 
	 * <pre>
	 * B(explain(p1) = B(p1)
	 * </pre>
	 * 
	 * @see Plan#blocksAccessed()
	 */
	@Override
	public long blocksAccessed() {
		return p.blocksAccessed();
	}

	/**
	 * Returns the schema of the explain, which is the union of the schemas of
	 * the underlying queries.
	 * 
	 * @see Plan#schema()
	 */
	@Override
	public Schema schema() {
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
		return hist;
	}

	/**
	 * Returns an estimate of the number of records in the query's output table.
	 * 
	 * @see Plan#recordsOutput()
	 */
	@Override
	public long recordsOutput() {
		return (long) histogram().recordsOutput();
	}

	@Override
	public String toString() {
		String c = p.toString();
		String[] cs = c.split("\n");
		StringBuilder sb = new StringBuilder();
		sb.append("-----------------------------------------------------------\n");
		for (String child : cs)
			sb.append(child).append("\n");
		sb.append("Actual #recs: " + String.valueOf(recordsOutput()));
		return sb.toString();
	}
}
