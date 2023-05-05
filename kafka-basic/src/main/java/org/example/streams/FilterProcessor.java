package org.example.streams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * packageName :  org.example.streams
 * fileName : SimpleProcessor
 * author :  ddh96
 * date : 2023-05-04
 * description : kafka stream Global ktable join 예제
 * ===========================================================
 * DATE                 AUTHOR                NOTE
 * -----------------------------------------------------------
 * 2023-05-04                ddh96             최초 생성
 */
public class FilterProcessor implements Processor<String, String> {

    private ProcessorContext processorContext;

    @Override
    public void init(ProcessorContext context) {
        this.processorContext = context;
    }

    @Override
    public void process(String key, String value) {

        if (value.length() > 5) {
            processorContext.forward(key,value);
        }
        processorContext.commit();
    }

    @Override
    public void close() {

    }
}
