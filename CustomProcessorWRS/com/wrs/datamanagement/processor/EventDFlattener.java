package com.wrs.datamanagement.processor;

import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigGroups;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.wrs.datamanagement.groups.FlattenGroups;

/**
 * Created by bmangalam on 5/23/17.
 */
@StageDef(
        version = 1,
        label = "EventFlattener",
        description = "",
        icon = "default.png",
        onlineHelpRefUrl = ""
)
@ConfigGroups(FlattenGroups.class)
@GenerateResourceBundle
public class EventDFlattener extends EventFlattener {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            defaultValue = "/'/event'",
            label = "Flattening Attribute",
            displayPosition = 10,
            group = "EventFlattener"
    )

    public String attribute;

    /** {@inheritDoc} */
    @Override
    public String getAttribute() {
        return attribute;
    }
}
