package org.apache.flink.streaming.connectors.redis.common.source;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;
import java.io.Serializable;

public class CacheGenericRowData implements Serializable {
    private final Object[] fields;

    public CacheGenericRowData(int arity) {
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

    public static CacheGenericRowData fromRowData(GenericRowData row) {
        int len = row.getArity();
        CacheGenericRowData cacheRow = new CacheGenericRowData(len);
        for (int i = 0; i < len; i++) {
            cacheRow.setField(i, row.getField(i));
        }
        return cacheRow;
    }

    public static GenericRowData toRowData(CacheGenericRowData cacheRow) {
        if (cacheRow == null) {
            return null;
        }
        int len = cacheRow.getArity();
        GenericRowData row = new GenericRowData(len);
        for (int i = 0; i < len; i++) {
            row.setField(i, cacheRow.getField(i));
        }
        return row;
    }
}
