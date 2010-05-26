package raptor.store.column;

import com.sleepycat.bind.tuple.*;

public class ColumnKeyTupleBinding extends TupleBinding<ColumnKey> {
    public void objectToEntry(ColumnKey ck, TupleOutput to) {
        try {
            to.writeString(ck.getTable());
            to.writeString(new String(ck.getKey(), "UTF-8"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    public ColumnKey entryToObject(TupleInput ti) {
        ColumnKey ck = new ColumnKey();
        try {
            String table = ti.readString();
            byte[] key = (ti.readString()).getBytes("UTF-8");
            ck.setTable(table);
            ck.setKey(key);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return ck;
    }
}
