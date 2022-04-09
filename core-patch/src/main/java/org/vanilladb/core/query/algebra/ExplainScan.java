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
	public void beforeFirst() {
		s.beforeFirst();
	}

	@Override
	public boolean next() {
		return s.next();
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
					getExplainString().getBytes()
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

	private String getExplainString() {
		String ans = getRecursiveExplainString(this.explainTree);
		ans += String.format("Actual #recs: %d\n", this.explainTree.getOutputRecords());
		return ans;
	}

	private static String getRecursiveExplainString(ExplainTree et) {
		String ans = String.format(
				"-> %s %s (#blks=%d, #recs=%d)\n",
				et.getPlanType(),
				et.getDetails(),
				et.getBlocksAccessed(),
				et.getOutputRecords()
		);
		for(ExplainTree e : et.getChildren())
			ans += "\t" + getRecursiveExplainString(e);
		return ans;
	}
}
