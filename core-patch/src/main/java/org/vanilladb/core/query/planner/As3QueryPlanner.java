package org.vanilladb.core.query.planner;

import java.util.ArrayList;
import java.util.List;


import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.ProductPlan;
import org.vanilladb.core.query.algebra.ProjectPlan;
import org.vanilladb.core.query.algebra.SelectPlan;
import org.vanilladb.core.query.algebra.TablePlan;
import org.vanilladb.core.query.algebra.ExplainPlan;
import org.vanilladb.core.query.algebra.materialize.GroupByPlan;
import org.vanilladb.core.query.algebra.materialize.SortPlan;
import org.vanilladb.core.query.parse.QueryData;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.tx.Transaction;


public class As3QueryPlanner implements QueryPlanner {

	@Override
	public Plan createPlan(QueryData data, Transaction tx) {
		// Step 1: Create a plan for each mentioned table or view
		List<Plan> plans = new ArrayList<Plan>();
		for (String tblname : data.tables()) {
			String viewdef = VanillaDb.catalogMgr().getViewDef(tblname, tx);
			if (viewdef != null)
				plans.add(VanillaDb.newPlanner().createQueryPlan(viewdef, tx));
			else
				plans.add(new TablePlan(tblname, tx));
		}
		// Step 2: Create the product of all table plans
		Plan p = plans.remove(0);
		for (Plan nextplan : plans)
			p = new ProductPlan(p, nextplan);
		// Step 3: Add a selection plan for the predicate
		p = new SelectPlan(p, data.pred());
		// Step 4: Add a group-by plan if specified
		if (data.groupFields() != null) {
			p = new GroupByPlan(p, data.groupFields(), data.aggregationFn(), tx);
		}
		// Step 5: Project onto the specified fields
		p = new ProjectPlan(p, data.projectFields());
		// Step 6: Add a sort plan if specified
		if (data.sortFields() != null)
			p = new SortPlan(p, data.sortFields(), data.sortDirections(), tx);

		return p;
	}
	
	public Plan ExplainCreatePlan(QueryData data, Transaction tx) {
		// Step 1: Create a plan for each mentioned table or view
		List<Plan> plans = new ArrayList<Plan>();
		for (String tblname : data.tables()) {
		String viewdef = VanillaDb.catalogMgr().getViewDef(tblname, tx);
		if (viewdef != null)
			plans.add(VanillaDb.newPlanner().createQueryPlan(viewdef, tx));
		else
			plans.add(new TablePlan(tblname, tx));
		}
		// Step 2: Create the product of all table plans
		Plan p = plans.remove(0);
		for (Plan nextplan : plans)
			p = new ProductPlan(p, nextplan);
		// Step 3: Add a selection plan for the predicate
		p = new SelectPlan(p, data.pred());
		// Step 4: Add a group-by plan if specified
		if (data.groupFields() != null) {
			p = new GroupByPlan(p, data.groupFields(), data.aggregationFn(), tx);
		}
		// Step 5: Project onto the specified fields
		p = new ProjectPlan(p, data.projectFields());
		// Step 6: Add a sort plan if specified
		if (data.sortFields() != null)
			p = new SortPlan(p, data.sortFields(), data.sortDirections(), tx);
		
		p = new ExplainPlan(p, tx);
		
		
		return p;
	}
}