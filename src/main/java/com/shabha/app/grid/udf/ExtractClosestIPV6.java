package com.shabha.app.grid.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalDouble;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import mikera.matrixx.Matrix;
import mikera.vectorz.Vector;


public class ExtractClosestIPV6 extends EvalFunc<Tuple> {
    
    @Override
    public Tuple exec(Tuple input) throws IOException {
        try {
            if (input == null || input.size() < 2) {
                throw new IOException("Not enough arguments to " + this.getClass().getName() + ": got " + input.size() + ", expected at least 1");
            }
            Tuple conversionTuple = (Tuple) input.get(0);
            DataBag clickBag = (DataBag) input.get(1);             
            return extractClosestIPV6(conversionTuple, clickBag);
        } catch (ExecException e) {
            throw new IOException(e);
        }
    }

    public Tuple extractClosestIPV6(Tuple conversionTuple, DataBag clickBag) throws ExecException  {
        
        List<Tuple> list = new ArrayList<Tuple>(); 
        Matrix editDistanceMatrix = null;
        Matrix weightMatrix = null;  
        Matrix distanceMatrix = null;
        double[] weights = {1000, 100, 10, 1, 0.1, 0.01, 0.001, 0.0001};
        int index = 0;
        int minIndex = -1;
            
        String[] conversionIpArray = conversionTuple.get(1).toString().split("\\:");
        weightMatrix = new Matrix(1, conversionIpArray.length);
        Vector wightVector = Vector.create(Arrays.copyOfRange(weights, 0, conversionIpArray.length));
        weightMatrix.add(wightVector);
        editDistanceMatrix = new Matrix((int) clickBag.size(), conversionIpArray.length);
        
        Iterator<Tuple> iterator = clickBag.iterator();          
        while (iterator.hasNext()) {
            Tuple clickTuple = iterator.next();
            String[] clickIpArray = clickTuple.get(7).toString().split("\\:"); 
            if (clickIpArray.length != conversionIpArray.length) {
                continue;
            }
            int[] editDistance = new int[clickIpArray.length];
            for (int i = 0; i < clickIpArray.length; i++) {
                editDistance[i] = StringUtils.getLevenshteinDistance(conversionIpArray[i], clickIpArray[i]);
            }           
            double[] formatEditDistance = Arrays.stream(editDistance).mapToDouble((i) -> (double) i).toArray();
            Vector editDistanceVector = Vector.create(formatEditDistance);
            editDistanceMatrix.setRow(index, editDistanceVector);
            list.add(clickTuple);
            index++;
        }  
                
        distanceMatrix = editDistanceMatrix.innerProduct(weightMatrix.toMatrixTranspose());
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
