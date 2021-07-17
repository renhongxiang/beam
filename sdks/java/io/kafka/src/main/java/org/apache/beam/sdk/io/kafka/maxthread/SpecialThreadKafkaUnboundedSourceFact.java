/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.beam.sdk.io.kafka.maxthread;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaUnboundedSource;
import org.apache.beam.sdk.io.kafka.KafkaUnboundedSourceFacture;

/**
 *
 * @author renhongxiang
 */
public class SpecialThreadKafkaUnboundedSourceFact extends KafkaUnboundedSourceFacture{
    
    private int maxSplit = 40;
    
    public static void init(int splits){
        SpecialThreadKafkaUnboundedSourceFact splitFact = new SpecialThreadKafkaUnboundedSourceFact();
        splitFact.setMaxSplit(splits);
        KafkaUnboundedSourceFacture.setInstance(splitFact);
    }
    
    @Override
    public <K,V> KafkaUnboundedSource<K,V> createSource(KafkaIO.Read<K,V> spec, int id){
        SpecialThreadKafkaUnboundedSource source = new SpecialThreadKafkaUnboundedSource<>(spec, id);
        source.setMaxSplits(this.getMaxSplit());
        return source;
    }

    public int getMaxSplit() {
        return maxSplit;
    }

    public void setMaxSplit(int maxSplit) {
        this.maxSplit = maxSplit;
    }

    
    
}
