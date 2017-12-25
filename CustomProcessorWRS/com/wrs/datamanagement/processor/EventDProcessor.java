package com.wrs.datamanagement.processor;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.wrs.datamanagement.groups.EvtGroups;

/**
 * Created by bmangalam on 4/20/17.
 */


@StageDef(
        version = 1,
        label = "EventProcessor",
        description = "",
        icon = "default.png",
        onlineHelpRefUrl = ""
)

@ConfigGroups(EvtGroups.class)
@GenerateResourceBundle
public class EventDProcessor extends EventProcessor {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "select * from wrsdata.event_definitions where status = true and datasource_id=1004;",
            label = "Event Definition Query",
            displayPosition = 10,
            group = "EVENT"
    )
    public String query;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "select ds_src, atr_status, datasource_id, ds_entity, raw_atr, taxon_atr, is_null, behavior_map, is_masked, is_encrypted, info_sec_class, is_global  FROM wrsdata.ds_taxonomy where (datasource_id = ? or datasource_id = 1004) and  atr_status = true and (ds_entity=? or ds_entity='default');",
            label = "Event Attributes Query",
            displayPosition = 10,
            group = "EVENT"
    )

    public String attrQuery;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "select ds_src, atr_status, datasource_id, ds_entity, raw_atr, taxon_atr, is_null, behavior_map, is_masked, is_encrypted, info_sec_class, is_global  FROM wrsdata.ds_taxonomy where  datasource_id = 1004 and  atr_status = true and ds_entity='default';",
            label = "Event Attributes Query",
            displayPosition = 10,
            group = "EVENT"
    )

    public String attrColumnQuery;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "yyyy-MM-dd HH:mm:ss",
            label = "Source Date Time Format",
            displayPosition = 10,
            group = "EVENT"
    )

    public String dateTimeFormat;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "datetime",
            label = "Event Timestamp Attribute",
            displayPosition = 10,
            group = "EVENT"
    )

    public String evtTsAttribute;


    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "jdbc:postgresql://35.188.6.64:5432/wrsdata?currentSchema=wrsdata",
            label = "JDBC URL",
            displayPosition = 10,
            group = "CONNECTION"
    )
    public String jdbcUrl;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Username",
            displayPosition = 10,
            group = "CONNECTION"
    )
    public String username;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Password",
            displayPosition = 10,
            group = "CONNECTION"
    )
    public String password;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Driver Class Name",
            displayPosition = 10,
            group = "CONNECTION"
    )
    public String driverClassName;

    @Override
    public String getJdbcUrl() {
        return jdbcUrl;
    }

    @Override
    public String getUsername() {
        return username;
    }

    @Override
    public String getPassword() {
        return password;
    }

    @Override
    public String getDriverClassName() {
        return driverClassName;
    }

    /** {@inheritDoc} */
    @Override
    public String getQuery() {

        return query;
    }

    /** {@inheritDoc} */
    @Override
    public String getAttrQuery() {

        return attrQuery;
    }

    /** {@inheritDoc} */
    @Override
    public String getAttrColumnQuery() {

        return attrColumnQuery;
    }

    @Override
    public String getDateTimeFormat() {

        return dateTimeFormat;
    }

    @Override
    public String getEvtTsAttribute() {
        return evtTsAttribute;
    }
}