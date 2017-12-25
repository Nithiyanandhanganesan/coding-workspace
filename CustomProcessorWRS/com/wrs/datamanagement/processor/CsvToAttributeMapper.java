package com.wrs.datamanagement.processor;

import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.OnRecordErrorException;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import com.wrs.datamanagement.exception.WRSDataException;
import com.wrs.datamanagement.lib.errors.Errors;
import com.wrs.datamanagement.util.DBManager;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.dbutils.DbUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by bmangalam on 4/21/17.
 */
public abstract class CsvToAttributeMapper extends SingleLaneRecordProcessor {
    private static final Logger logger = LoggerFactory.getLogger(CsvToAttributeMapper.class);

    private static final String DATASOURCE_ID = "datasource_id";
    private static final String PARTY_OWNER = "party_owner";

    private Map<String, String> attrMap = new ConcurrentHashMap<>();

    private HikariDataSource hds;


    /**
     * Gives access to the UI configuration of the stage provided by the {@link CsvToAttributeDMapper} class.
     */
    public abstract String getQuery();

    public abstract String getDatsourceId();

    public abstract String getJdbcUrl();
    public abstract String getUsername();
    public abstract String getPassword();
    public abstract String getDriverClassName();
    public abstract String getPartyOwner();

    @Override
    /** {@inheritDoc} */
    protected List<ConfigIssue> init() {

        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();

        try {
            hds = DBManager.getDatasource(getJdbcUrl(), getUsername(), getPassword(), getDriverClassName());
            this.executeQuery(getQuery());

            logger.debug("ATTR MAP: \n" + attrMap.toString());
        }
        catch(WRSDataException e)
        {
            getContext().reportError(e);
        }
        finally
        {
            if(hds != null) {
                IOUtils.closeQuietly(hds);
            }
            hds = null;
        }
        return issues;
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        // Clean up any open resources.
        super.destroy();
        if(hds != null) {
            IOUtils.closeQuietly(hds);
        }
    }

    @Override
    protected void process(Record record, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
        logger.debug("Input record: {}", record);

        LinkedHashMap<String, Field> fieldMap = record.get().getValueAsListMap();
        LinkedHashMap<String, Field> responseMap = new LinkedHashMap<>(fieldMap.size());
        for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
            String key = entry.getKey();
            Field field = entry.getValue();

            String columnValue = attrMap.get(key);
            logger.debug("key: " + key + " columnValue: " + columnValue + " Field Value: " + field.getValueAsString());
            if (columnValue != null) {
                responseMap.put(columnValue, Field.create(field.getValueAsString()));
            }
        }

        if(!responseMap.containsKey(PARTY_OWNER))
        {
            responseMap.put(PARTY_OWNER, Field.create(getPartyOwner()));
        }

        responseMap.put(DATASOURCE_ID, Field.create(getDatsourceId()));

        record.set("/", Field.createListMap(responseMap));

        logger.debug("Output record: " + record);

        singleLaneBatchMaker.addRecord(record);
    }

    public void executeQuery(String query)
    {
        PreparedStatement statement = null;
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = hds.getConnection();

            statement = connection.prepareStatement(query);

            resultSet =  statement.executeQuery();
            while (resultSet.next())
            {
                    attrMap.put(resultSet.getString(1), resultSet.getString(2));
            }

            resultSet.close();
            statement.close();
            connection.close();
            statement= null;
            connection = null;

        } catch (SQLException e) {
            logger.error("Exception occured while executing: " + query, e);
            //throw new WRSDataException("Exception occured while executing: " + query, e);
        } finally {
            DbUtils.closeQuietly(connection, statement, resultSet);
        }
    }

}