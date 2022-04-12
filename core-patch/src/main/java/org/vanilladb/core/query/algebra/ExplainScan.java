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
import org.vanilladb.core.sql.VarcharConstant;

import org.vanilladb.core.sql.Constant;

/**
 * The scan class corresponding to the <em>explain</em> relational algebra
 * operator.
 */
public class ExplainScan implements Scan {
	private String info;

	/**
	 * Creates a product scan having the two underlying scans.
	 * @param info
	 */
	public ExplainScan(String info) {
		this.info = info;
	}

	/**
	 * Positions the scan before its first record. In other words, the LHS scan
	 * is positioned at its first record, and the RHS scan is positioned before
	 * its first record.
	 * Do nothing in ExplainScan
	 * 
	 * @see Scan#beforeFirst()
	 */
	@Override
	public void beforeFirst() {
	}

	/**
	 * Return true only for the first calling, otherwise return false
	 * Since there is just one record in ExplainScan
	 * 
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		if(this.info == "")
			return false;
		return true;
	}

	/**
	 * Closes both underlying scans.
	 * Do nothing in ExplainScan
	 * 
	 * @see Scan#close()
	 */
	@Override
	public void close() {
	}

	/**
	 * Returns the value of the specified field. The value is obtained from
	 * whichever scan contains the field.
	 * When fldName=query-plan, return the info
	 * 
	 * @see Scan#getVal(java.lang.String)
	 */
	@Override
	public Constant getVal(String fldName) {
		Constant output = new VarcharConstant(this.info);
		this.info = "";
		return output;
	}

	
	@Override
	public boolean hasField(String fldName) {
		return false;
	}
}
