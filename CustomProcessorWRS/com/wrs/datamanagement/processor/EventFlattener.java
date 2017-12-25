package com.wrs.datamanagement.processor;

import com.github.wnameless.json.flattener.JsonFlattener;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;

import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;
import com.wrs.datamanagement.lib.errors.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bmangalam on 5/20/17.
 */
public abstract class EventFlattener extends SingleLaneRecordProcessor {

    private static final Logger logger = LoggerFactory.getLogger(EventFlattener.class);

    public abstract String getAttribute();

    @Override
    /** {@inheritDoc} */
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();

        return issues;
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        // Clean up any open resources.
        super.destroy();
    }

    @Override
    protected void process(Record record, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
        ConcurrentHashMap<String, Field> outputMap = new ConcurrentHashMap<>();
        Field wField = record.get(getAttribute());
        String json = wField.getValueAsString();
        Map<String, Object> flattenJson = JsonFlattener.flattenAsMap(json);

        for(Map.Entry<String, Object> entry: flattenJson.entrySet())
        {
            String key = entry.getKey();
            Field value = Field.create(entry.getValue().toString());
            outputMap.put(key, value);
        }
        record.set(Field.create(outputMap));
        singleLaneBatchMaker.addRecord(record);
    }
}

