package org.vanilladb.core.query.algebra;

import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.VarcharConstant;

import java.util.Collection;

public class ExplainScan implements Scan {

    private Collection<String> fieldList;
    private String explain_string;
    private int counter;

    public ExplainScan(Collection<String> fieldList, String es) {
        this.fieldList = fieldList;
        explain_string = es;
        counter = 0;
    }

    @Override
    public void beforeFirst() {
        counter = 0;
    }

    @Override
    public boolean next() {
        if (counter > 0) {
            return false;
        }
        counter++;
        return true;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean hasField(String fldName) {
        return fieldList.contains(fldName);
    }

    @Override
    public Constant getVal(String fldName) {
        if (hasField(fldName))
            return new VarcharConstant(explain_string);
        else
            throw new RuntimeException("field " + fldName + " not found.");
    }
}
