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
package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;

import java.nio.charset.StandardCharsets;
import java.util.Collection;

/**
 * The scan class corresponding to the <em>project</em> relational algebra
 * operator. All methods except hasField delegate their work to the underlying
 * scan.
 */
public class ExplainScan implements Scan {
	private Scan s;
	private ExplainTree explainTree;
	private String explainString;
	private boolean executed;

	/**
	 * Creates a project scan having the specified underlying scan and field
	 * list.
	 *
	 * @param s
	 *            the underlying scan
	 */
	public ExplainScan(Scan s, ExplainTree et) {
		this.s = s;
		this.explainTree = et;
	}

	@Override
	public void beforeFirst() { this.executed = false; }

	@Override
	public boolean next() {
		if (this.executed)
			return false;

		explainString = generateExplainString();
		this.executed = true;
		return true;
	}


	@Override
	public void close() {
		s.close();
	}

	@Override
	public Constant getVal(String fldName) {
		if (fldName.equals("query-plan"))
			return Constant.newInstance(
					Type.VARCHAR,
					explainString.getBytes()
			);
		else if (hasField(fldName))
			return s.getVal(fldName);
		else
			throw new RuntimeException("field " + fldName + " not found.");
	}

	/**
	 * Returns true if the specified field is in the projection list.
	 * 
	 * @see Scan#hasField(String)
	 */
	@Override
	public boolean hasField(String fldName) { return s.hasField(fldName); }

	private static String indentHierarchy(int dep) {
		StringBuilder result = new StringBuilder();
		for(int i = 0; i < dep; i++)
			result.append("\t");
		return result.toString();
	}

	private static String getRecursiveExplainString(int dep, ExplainTree et) {
		StringBuilder result = new StringBuilder(String.format(
				"-> %s %s (#blks=%d, #recs=%d)\n",
				et.getPlanType(),
				et.getDetails() == null ? "" : et.getDetails(),
				et.getBlocksAccessed(),
				et.getOutputRecords()
		));
		dep += 1;
		for(ExplainTree e : et.getChildren())
			result.append(indentHierarchy(dep)).append(getRecursiveExplainString(dep, e));
		return result.toString();
	}

	private String generateExplainString() {
		String result = "\n" + getRecursiveExplainString(0, this.explainTree);

		long actualRecs = getActualRecordCount();

		result += String.format("Actual #recs: %d\n", actualRecs);
		return result;
	}

	private long getActualRecordCount() {
		long cnt = 0;
		s.beforeFirst();

		while (s.next())
			cnt++;

		return cnt;
	}
}
