package com.shabha.app.grid.udf;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.base.Strings;


public class ExtractBid extends EvalFunc<String> {

    private static final Logger LOG = LoggerFactory.getLogger(ExtractBid.class);
    private static final Pattern BID_PATTERN = Pattern.compile("bid=\\[(.*?)\\]", Pattern.CASE_INSENSITIVE);
    


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
            return extractBid(rawTuple);
        } catch (ExecException e) {
            throw new IOException(e);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public String extractBid(String tuple) throws Exception  {
        if (tuple.length() <= 2 || !tuple.contains("hpstream") || !tuple.contains("bid")) {
            return "";
        }
        String decryptedBid = "";
        Matcher matcher = BID_PATTERN.matcher(tuple);
        while (matcher.find()) {
            decryptedBid = matcher.group(1);
        }
        return decryptedBid;
    }

    
    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
}
