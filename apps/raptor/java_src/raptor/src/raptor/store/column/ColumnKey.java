// -------------------------------------------------------------------
//
// Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
//
// -------------------------------------------------------------------

package raptor.store.column;

public class ColumnKey {
    private String table;
    private byte[] key;

    public ColumnKey() {
        table = "";
        key = new byte[0];
    }

    public String getTable() {
        return table;
    }

    public byte[] getKey() {
        return key;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public void setKey(byte[] key) {
        this.key = key;
    }
}
