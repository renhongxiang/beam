/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.beam.sdk.io.kafka.maxthread;

import java.util.List;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaUnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 *
 * @author renhongxiang
 */
public class SpecialThreadKafkaUnboundedSource<K, V> extends KafkaUnboundedSource<K, V> {

    private int maxSplits = -1;

    public SpecialThreadKafkaUnboundedSource(KafkaIO.Read<K, V> spec, int id) {
        super(spec, id);
    }

    @Override
    public List<KafkaUnboundedSource<K, V>> split(int desiredNumSplits, PipelineOptions options)
            throws Exception {
        int split = this.getMaxSplits();
        if(split <= 0){
            return super.split(desiredNumSplits, options);
        }else{
            return super.split(split, options);
        }
    }

    public int getMaxSplits() {
        return maxSplits;
    }

    public void setMaxSplits(int maxSplits) {
        this.maxSplits = maxSplits;
    }

    
}
