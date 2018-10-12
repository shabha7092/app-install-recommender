package com.shabha.app.grid.udf;

import java.io.IOException;

import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.logicalLayer.schema.Schema;
    
    public class FilterClickBag extends EvalFunc<Tuple> {
        
        private static IsValidIPv4 ipValidation = new IsValidIPv4();
        private static Tuple result = TupleFactory.getInstance().newTuple(2);
        private static BagFactory bagFactory = BagFactory.getInstance();
           
        @Override
        public Tuple exec(Tuple input) throws IOException {
            try {
                if (input == null || input.size() < 2) {
                    throw new IOException("Not enough arguments to " + this.getClass().getName() + ": got " + input.size() + ", expected at least 1");
                }
                Tuple conversionTuple = (Tuple) input.get(0);
                DataBag clickBag = (DataBag) input.get(1);             
                return filterClickBag(conversionTuple, clickBag);
            } catch (ExecException e) {
                throw new IOException(e);
            }
        }
        
        public Tuple filterClickBag(Tuple conversionTuple, DataBag clickBag) throws ExecException  {
            DataBag filteredClickBag = bagFactory.newDefaultBag();
            String[] conversionArray =  conversionTuple.toString().replace("(", "").replace(")", "").split(",");
            Iterator<Tuple> iterator = clickBag.iterator();
            while (iterator.hasNext()) {
                Tuple clickTuple = iterator.next();
                String[] clickArray = clickTuple.toString().replace("(", "").replace(")", "").split(",");                
                if (conversionArray[3].equals(clickArray[8]) && Long.valueOf(conversionArray[2]).longValue() == Long.valueOf(clickArray[5]).longValue() 
                        && Long.valueOf(clickArray[6]).longValue() < Long.valueOf(conversionArray[0]).longValue()
                        && ipValidation.isValidIPv4Address(conversionArray[1]) == ipValidation.isValidIPv4Address(clickArray[7])) {
                    filteredClickBag.add(clickTuple);
                }
            }
            result.set(0, conversionTuple);
            result.set(1, filteredClickBag);
            return result;
        }

        @Override
        public Schema outputSchema(Schema input) {
            return new Schema(new Schema.FieldSchema(null, DataType.TUPLE));
        }
        
}
