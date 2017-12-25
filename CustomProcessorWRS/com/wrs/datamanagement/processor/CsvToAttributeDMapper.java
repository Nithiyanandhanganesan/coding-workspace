package com.wrs.datamanagement.processor;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.wrs.datamanagement.groups.AttrGroups;

/**
 * Created by bmangalam on 4/24/17.
 */

@StageDef(
        version = 1,
        label = "CsvToAttributeMapper",
        description = "",
        icon = "default.png",
        onlineHelpRefUrl = ""
)

@ConfigGroups(AttrGroups.class)
@GenerateResourceBundle
public class CsvToAttributeDMapper extends CsvToAttributeMapper {
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

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "select taxon_cd, raw_atr from wrsdata.ds_taxonomy where datasource_id=1004 and ds_entity='impressions';",
            label = "CsvToAttribute Query",
            displayPosition = 10,
            group = "CsvToAttribute"
    )

    public String query;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "1004",
            label = "Datasource Id",
            displayPosition = 10,
            group = "CsvToAttribute"
    )
    public String datsourceId;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "10030",
            label = "partyOwner",
            displayPosition = 10,
            group = "CsvToAttribute"
    )
    public String partyOwner;


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
    public String getDatsourceId() {
        return datsourceId;
    }

    @Override
    public String getPartyOwner(){return partyOwner; }
}