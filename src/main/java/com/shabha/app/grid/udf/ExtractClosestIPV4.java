package com.shabha.app.grid.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import mikera.matrixx.Matrix;
import mikera.vectorz.Vector;

public class ExtractClosestIPV4 extends EvalFunc<Tuple> {

    @Override
    public Tuple exec(Tuple input) throws IOException {
        try {
            if (input == null || input.size() < 2) {
                throw new IOException("Not enough arguments to " + this.getClass().getName() + ": got " + input.size() + ", expected at least 1");
            }
            Tuple conversionTuple = (Tuple) input.get(0);
            DataBag clickBag = (DataBag) input.get(1);             
            return extractClosestIPV4(conversionTuple, clickBag);
        } catch (ExecException e) {
            throw new IOException(e);
        }
    }

    public Tuple extractClosestIPV4(Tuple conversionTuple, DataBag clickBag) throws ExecException  {

        List<Tuple> list = new ArrayList<Tuple>(); 
        Matrix clickMatrix = new Matrix((int) clickBag.size(), 4);
        Matrix weightMatrix = new Matrix(1, 4);     
        double[] weights = {1000, 100, 10, 1};
        Matrix distanceMatrix = null;
        int index = 0;
        int minIndex = -1;

        Vector wightVector = Vector.create(weights);
        weightMatrix.add(wightVector);
        String[] conversionIpArray = conversionTuple.get(1).toString().split("\\.");
        double[] formatConversionIpArray = Arrays.stream(conversionIpArray).mapToDouble(Double::parseDouble).toArray();    
        Vector conversionVector = Vector.create(formatConversionIpArray);

        Iterator<Tuple> iterator = clickBag.iterator();          
        while (iterator.hasNext()) {
            Tuple clickTuple = iterator.next();
            String[] clickIpArray = clickTuple.get(7).toString().split("\\."); 
            double[] formatclickIpArray = Arrays.stream(clickIpArray).mapToDouble(Double::parseDouble).toArray();          
            Vector clickVector = Vector.create(formatclickIpArray);
            clickMatrix.setRow(index, clickVector);
            list.add(clickTuple);           
            index++;
        }  

        clickMatrix.sub(conversionVector);
        clickMatrix.square();        
        distanceMatrix = clickMatrix.innerProduct(weightMatrix.toMatrixTranspose());
        distanceMatrix.sqrt();
        minIndex = findMinIndex(distanceMatrix.asDoubleArray());   
        if (list.size() > 0) {
            minIndex = findMinIndex(Arrays.copyOfRange(distanceMatrix.asDoubleArray(), 0, list.size()));  
            return list.get(minIndex);
        }
        return null;
    }

    public int findMinIndex(double[] values) {
        OptionalDouble minimun = DoubleStream.of(values).min();
        return (int) DoubleStream.of(values).boxed().collect(Collectors.toList()).indexOf(minimun.getAsDouble());
    }    

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.TUPLE));
    }

}
