package org.vanilladb.core.query.algebra;

import org.vanilladb.core.query.algebra.Plan;
import org.vanilladb.core.query.algebra.Scan;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.Type;
import org.vanilladb.core.storage.metadata.statistics.Histogram;
import org.vanilladb.core.storage.tx.Transaction;

public class ExplainPlan implements Plan {
    private Plan p;
    private Schema schema;

    public ExplainPlan(Plan p) {
        this.p = p;
        schema = new Schema();
        schema.addField("query-plan", Type.VARCHAR(500));
    }

    private int getActualRecs() {
        Scan s = p.open();
        s.beforeFirst();
        int recs = 0;
        while (s.next()) {
            recs += 1;
        }
        return recs;
    }

    @Override
    public String getExplainString(int level) {
        return "\n" + p.getExplainString(0) + String.format("\nActual #recs: %d", getActualRecs());
    }

    @Override
    public Scan open() {
        String explain_string = getExplainString(0);
        ExplainScan s = new ExplainScan(schema.fields(), explain_string);
        return s;
    }

    @Override
    public long blocksAccessed() {
        return p.blocksAccessed();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Histogram histogram() {
        return null;
    }

    @Override
    public long recordsOutput() {
        return 0;
    }
}
