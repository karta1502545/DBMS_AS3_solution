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
import org.vanilladb.core.sql.VarcharConstant;
import java.util.Collection;

/**
 * The scan class corresponding to the <em>select</em> relational algebra
 * operator. All methods except next delegate their work to the underlying scan.
 */
public class ExplainScan implements Scan {
	private Scan s;
	private Constant ex;
	private Collection<String> fieldList;
	private Boolean next;

	/**
	 * Creates a select scan having the specified underlying scan and predicate.
	 *
	 * @param s
	 *            the scan of the underlying query
	 * @param pred
	 *            the selection predicate
	 */
	public ExplainScan(Scan s, String ex, Collection<String> fieldList) {
		this.s = s;
        this.ex = new VarcharConstant(ex, Type.VARCHAR);
		this.fieldList = fieldList;
		this.next = true;
	}

	// Scan methods

	@Override
	public void beforeFirst() {
		s.beforeFirst();
	}

	/**
	 * Move to the next record satisfying the predicate. The method repeatedly
	 * calls next on the underlying scan until a suitable record is found, or
	 * the underlying scan contains no more records.
	 *
	 * @see Scan#next()
	 */
	@Override
	public boolean next() {
		return next;
	}

    @Override
	public Constant getVal(String fldName) {
		//return ex;
		if(hasField(fldName))
			{this.next = false; return ex;}
		throw new RuntimeException("field " + fldName + " not found.");
	}


	@Override
	public void close() {
		s.close();
	}

	@Override
	public boolean hasField(String fldName) {
		return fieldList.contains(fldName);
	}

}
