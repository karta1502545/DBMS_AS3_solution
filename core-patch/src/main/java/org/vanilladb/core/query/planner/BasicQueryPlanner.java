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
package org.vanilladb.core.query.planner;

import java.util.ArrayList;
import java.util.List;


import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ProductPlan;
import org.vanilladb.core.query.algebra.ProjectPlan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.UpdateScan;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.algebra.materialize.ExplainPlan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The simplest, most naive query planner possible.
 */
public class BasicQueryPlanner implements QueryPlanner {

	/**
	 * Creates a query plan as follows. It first takes the product of all tables
	 * and views; it then selects on the predicate; and finally it projects on
	 * the field list.
	 */
	@Override
	public Plan createPlan(QueryData data, Transaction tx) {
		// Step 1: Create a plan for each mentioned table or view
		String xStr = "";
		
		List<Plan> plans = new ArrayList<Plan>();
		for (String tblname : data.tables()) {
			String viewdef = VanillaDb.catalogMgr().getViewDef(tblname, tx);
			if (viewdef != null) 
				plans.add(VanillaDb.newPlanner().createQueryPlan(viewdef, tx));
			else {
				Plan tp = new TablePlan(tblname, tx);
				plans.add(tp);
				xStr = "->TablePlan on (" + tblname + ")" + "(#blks= " + tp.blocksAccessed() +", #recs=" + tp.recordsOutput() + ")" + xStr;
				xStr = "\n" + xStr;
			}
		}
		// Step 2: Create the product of all table plans
		
		Plan p = plans.remove(0);
		
		xStr = xStr.replace("\n", "\n\t");
		
		for (Plan nextplan : plans) {
			p = new ProductPlan(p, nextplan);
			xStr = "\n->ProductPlan (#blks= " + p.blocksAccessed() +", #recs=" + p.recordsOutput() + ")" + xStr;
		}
		// Step 3: Add a selection plan for the predicate
		p = new SelectPlan(p, data.pred());
		xStr = xStr.replace("\n", "\n\t\t");
		xStr = "\n\t->SelectPlan pred:(" + data.pred().toString() + ") (#blks=" + p.blocksAccessed() + ", #recs=" + p.recordsOutput() + ")" + xStr;
		
		// Step 4: Add a group-by plan if specified
		String sStr = "";
		String gbStr = "";
		if (data.groupFields() != null) {
			xStr = xStr.replace("\n", "\n\t");
			p = new GroupByPlan(p, data.groupFields(), data.aggregationFn(), tx);
			gbStr = "\n\t->GroupByPlan: (#blks=" + p.blocksAccessed() + ", #recs=" + p.recordsOutput() + ")";
			Plan sp = new SortPlan(p, new ArrayList<String>(data.groupFields()), tx);
			sStr = "\n\t\t->SortPlan: (#blks=" + p.blocksAccessed() + ", #recs=" + p.recordsOutput() + ")";		
			xStr = xStr.replace("\n", "\n\t");
		}
		// Step 5: Project onto the specified fields
		p = new ProjectPlan(p, data.projectFields());
		String pStr = "\n->ProjectPlan: (#blks=" + p.blocksAccessed() + ", #recs=" + p.recordsOutput() + ")";
		String rec = "\nActual #recs: " + p.recordsOutput();
		// Step 6: Add a sort plan if specified
		if (data.sortFields() != null) {
			p = new SortPlan(p, data.sortFields(), data.sortDirections(), tx);
			if (data.groupFields() == null) {
				sStr = "\n\t->SortPlan: (#blks=" + p.blocksAccessed() + ", #recs=" + p.recordsOutput() + ")";
				xStr = xStr.replace("\n", "\n\t");
			}
		}
		xStr = pStr + gbStr + sStr + xStr + rec;
//		System.out.print(xStr);
		ExplainPlan xP = new ExplainPlan(tx, xStr);
		return data.explain() ? xP : p;
	}
}
