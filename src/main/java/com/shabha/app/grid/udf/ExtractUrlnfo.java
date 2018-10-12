package com.shabha.app.grid.udf;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class ExtractUrlnfo extends EvalFunc<String> {
    
    private static final Pattern UA_PATTERN = Pattern.compile("(user-agent=\\[)([^\\]]*)", Pattern.CASE_INSENSITIVE);
   
    @Override
    public String exec(Tuple input) throws IOException {
        try {
            if (input == null || input.size() < 1) {
                throw new IOException("Not enough arguments to " + this.getClass().getName() + ": got " + input.size() + ", expected at least 1");
            }
            if (input.get(0) == null) {
                return "";
            }
            String rawTuple = input.get(0).toString();
            return extractUserAgent(rawTuple);
        } catch (ExecException e) {
            throw new IOException(e);
        }
    }
    
    
    
    public String extractUserAgent(String tuple)  {
        if (tuple.length() <= 2) {
            return "";
        }
        String userAgent = "";
        Matcher matcher = UA_PATTERN.matcher(tuple);
        while (matcher.find()) {
            userAgent = matcher.group(2);
        }
        return userAgent;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }

}
