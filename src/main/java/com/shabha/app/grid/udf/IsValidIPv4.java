package com.shabha.app.grid.udf;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.http.conn.util.InetAddressUtils;

public class IsValidIPv4 extends EvalFunc<Boolean> {
    
    @Override
    public Boolean exec(Tuple input) throws IOException {
        try {
            if (input == null || input.size() < 1) {
                throw new IOException("Not enough arguments to " + this.getClass().getName() + ": got " + input.size() + ", expected at least 1");
            }

            if (input.get(0) == null) {
                return false;
            }
            String rawTuple = input.get(0).toString();
            return isValidIPv4Address(rawTuple);
        } catch (ExecException e) {
            throw new IOException(e);
        }
    }
    
    public Boolean isValidIPv4Address(String tuple) {
        if (tuple.length() == 0) {
            return false;
        }
        return InetAddressUtils.isIPv4Address(tuple);
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.BOOLEAN));
    }

}
