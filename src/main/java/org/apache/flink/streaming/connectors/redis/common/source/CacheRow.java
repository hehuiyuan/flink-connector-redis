package org.apache.flink.streaming.connectors.redis.common.source;

import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;
import java.io.Serializable;

public class CacheRow implements Serializable {
    private final Object[] fields;

    public CacheRow(int arity) {
        this.fields = new Object[arity];
    }

    public int getArity() {
        return this.fields.length;
    }

    @Nullable
    public Object getField(int pos) {
        return this.fields[pos];
    }

    public void setField(int pos, @Nullable Object value) {
        this.fields[pos] = value;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < this.fields.length; ++i) {
            if (i > 0) {
                sb.append(",");
            }

            sb.append(StringUtils.arrayAwareToString(this.fields[i]));
        }

        return sb.toString();
    }

    public static CacheRow fromRow(Row row) {
        int len = row.getArity();
        CacheRow cacheRow = new CacheRow(len);
        for (int i = 0; i < len; i++) {
            cacheRow.setField(i, row.getField(i));
        }
        return cacheRow;
    }

    public static Row toRow(CacheRow cacheRow) {
        if (cacheRow == null) {
            return null;
        }
        int len = cacheRow.getArity();
        Row row = new Row(len);
        for (int i = 0; i < len; i++) {
            row.setField(i, cacheRow.getField(i));
        }
        return row;
    }
}
