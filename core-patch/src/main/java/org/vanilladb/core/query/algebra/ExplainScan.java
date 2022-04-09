package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

public class ExplainScan implements Scan {
    private Scan s;
    private Constant explain_val ;
    private boolean before_first;

    public ExplainScan(Scan s, String explain_msg) {
        this.s = s;
        this.explain_val = new VarcharConstant(explain_msg);
    }

    @Override
    public void beforeFirst() {
        before_first = true ;
    }

    @Override
    public boolean next() {
        if (before_first) {
            before_first = false ;
            return true ;
        }
        return false ;
    }

    @Override
    public void close() {
        s.close();
    }

    @Override
    public Constant getVal(String fldName) {
        if (hasField(fldName)) {
            return explain_val ;
        } else
            throw new RuntimeException("field " + fldName + " not found in explain.");
    }

    @Override
    public boolean hasField(String fldName) {
        return fldName.equals("query-plan") ;
    }
}
