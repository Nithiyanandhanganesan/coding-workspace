package com.wrs.datamanagement.processor;

/**
 * Created by zaziz on 4/27/17.
 */

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.StringEL;

import com.wrs.datamanagement.groups.ProfileGroups;


@StageDef(
        version = 1,
        label = "Profile Processor",
        description = "",
        icon = "default.png",
        onlineHelpRefUrl = ""
)
@ConfigGroups(ProfileGroups.class)
@GenerateResourceBundle
public class ProfileDProcessor extends ProfileProcessor {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.TEXT,
            description = "Project ID",
            label = "Project ID",
            elDefs = {StringEL.class, RecordEL.class},
            evaluation = ConfigDef.Evaluation.IMPLICIT,
            displayPosition = 10,
            group = "PROFILE"
    )
    public String projectID;


    @ConfigDef(
            required = true,
            type = ConfigDef.Type.TEXT,
            description = "Instance ID",
            label = "Instance ID",
            elDefs = {StringEL.class, RecordEL.class},
            evaluation = ConfigDef.Evaluation.IMPLICIT,
            displayPosition = 10,
            group = "PROFILE"
    )
    public String instanceID;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "",
            label = "Customer ID Field",
            description = "Field that contains the customer ID",
            group = "#0",
            displayPosition = 30
    )
    @FieldSelectorModel(singleValued = true)
    public String customerID;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "AppnexusUserID",
            label = "Customer ID Type",
            displayPosition = 110,
            group = "#0"
    )
    @ValueChooserModel(CustomerIDTypeChooserValues.class)
    public String idType;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "",
            label = "Party Owner Field",
            description = "Field that contains the party owner",
            group = "#0",
            displayPosition = 30
    )
    @FieldSelectorModel(singleValued = true)
    public String partyOwner;

    /** {@inheritDoc} */
    @Override
    public String getProjectID() {
        return projectID;
    }

    /** {@inheritDoc} */
    @Override
    public String getInstanceID() {
        return instanceID;
    }

    public String getCustomerId() {
        return customerID;
    }

    @Override
    public String getIDType() {
        return idType;
    }

    @Override
    public String getPartyOwner() {
        return partyOwner;
    }

}