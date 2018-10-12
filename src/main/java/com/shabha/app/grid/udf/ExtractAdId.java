package com.shabha.app.grid.udf;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class ExtractAdId extends EvalFunc<Long> {

    private static final Pattern AD_ID_PATTERN = Pattern.compile("(hpstream-.*-c)(\\d*)", Pattern.CASE_INSENSITIVE);

    @Override
    public Long exec(Tuple input) throws IOException {
        try {
            if (input == null || input.size() < 1) {
                throw new IOException("Not enough arguments to " + this.getClass().getName() + ": got " + input.size() + ", expected at least 1");
            }

            if (input.get(0) == null) {
                return -1L;
            }
            String rawTuple = input.get(0).toString();
            return extractAdId(rawTuple);
        } catch (ExecException e) {
            throw new IOException(e);
        }
    }

    public Long extractAdId(String tuple)  {
        if (tuple.length() <= 2 || !tuple.contains("hpstream")) {
            return -1L;
        }
        String adId = "";
        Matcher matcher = AD_ID_PATTERN.matcher(tuple);
        while (matcher.find()) {
            adId = matcher.group(2);
        }
        if (adId == "") {
            return -1L;
        }
        return Long.valueOf(adId);
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.LONG));
    }

}
