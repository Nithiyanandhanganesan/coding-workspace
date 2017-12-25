package com.wrs.datamanagement.processor;

import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import com.wrs.datamanagement.service.EventService;

import com.wrs.datamanagement.util.DBManager;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 * Created by bmangalam on 4/19/17.
 */
public abstract class EventProcessor extends SingleLaneRecordProcessor {
    private static final Logger logger = LoggerFactory.getLogger(EventProcessor.class);

    /**
     * Gives access to the UI configuration of the stage provided by the {@link EventDProcessor} class.
     */
    public abstract String getQuery();

    public abstract String getDateTimeFormat();

    public abstract String getAttrQuery();

    public abstract String getAttrColumnQuery();

    public abstract String getEvtTsAttribute();

    public abstract String getJdbcUrl();

    public abstract String getUsername();

    public abstract String getPassword();

    public abstract String getDriverClassName();

    private EventService eventService = null;
    private HikariDataSource hds = null;

    /** {@inheritDoc} */
    @Override
    protected List<ConfigIssue> init() {

        // Validate configuration values and open any required resources.
        List<ConfigIssue> configs = super.init();

        //Initialize the EventService
        logger.info("DateTimeFormat:" + getDateTimeFormat());
        eventService = new EventService(getQuery(), getAttrQuery(), getAttrColumnQuery(), getDateTimeFormat(), getEvtTsAttribute(), getJdbcUrl(), getUsername(), getPassword(), getDriverClassName());

        // If issues is not empty, the UI will inform the user of each configuration issue in the list.
        return configs;
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        // Clean up any open resources.
        super.destroy();
    }


    @Override
    protected void process(Record record, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
        logger.debug("Input record: {}", record);

        record = eventService.transform(record);

        logger.debug("Output record: " + record);

        singleLaneBatchMaker.addRecord(record);
    }
}
