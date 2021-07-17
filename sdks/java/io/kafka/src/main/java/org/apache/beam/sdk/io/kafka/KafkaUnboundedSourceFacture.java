/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.beam.sdk.io.kafka;

/**
 *
 * @author renhongxiang
 */
public class KafkaUnboundedSourceFacture {
    static private KafkaUnboundedSourceFacture instance = new KafkaUnboundedSourceFacture();
    
    public <K,V> KafkaUnboundedSource<K,V> createSource(KafkaIO.Read<K,V> spec, int id){
        return new KafkaUnboundedSource<>(spec, id);
    }

    public static KafkaUnboundedSourceFacture getInstance() {
        if(instance == null){
            instance = new KafkaUnboundedSourceFacture();
        }
        return instance;
    }

    public static void setInstance(KafkaUnboundedSourceFacture instance) {
        KafkaUnboundedSourceFacture.instance = instance;
    }
    
    
}
