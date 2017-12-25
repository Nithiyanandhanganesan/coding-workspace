package com.wrs.datamanagement.destination.gcs;

import com.streamsets.pipeline.lib.el.RecordEL;
import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.streamsets.pipeline.api.ConfigDef;
import com.streamsets.pipeline.api.ConfigDefBean;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.ValueChooserModel;
import com.streamsets.pipeline.config.DataFormat;
import com.streamsets.pipeline.config.TimeZoneChooserValues;
import com.streamsets.pipeline.stage.destination.lib.DataGeneratorFormatConfig;

import java.util.List;

/**
 * Created by zaziz on 6/9/17.
 */
public class GCSTargetConfig {
    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Bucket",
            description = "Expression that will identify bucket for each record.",
            displayPosition = 20,
            evaluation = ConfigDef.Evaluation.EXPLICIT,
            elDefs = { RecordEL.class, TimeEL.class, TimeNowEL.class },
            group = "GCS"
    )
    public String bucketTemplate;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            label = "Common Prefix",
            description = "",
            displayPosition = 30,
            group = "GCS"
    )
    public String commonPrefix;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
            evaluation = ConfigDef.Evaluation.EXPLICIT,
            defaultValue = "",
            label = "Partition Prefix",
            description = "Partition to write to. If the partition doesn't exist on GCS, it will be created.",
            displayPosition = 180,
            group = "GCS"
    )
    public String partitionTemplate;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            defaultValue = "UTC",
            label = "Data Time Zone",
            description = "Time zone to use to resolve the date time of a time-based partition prefix",
            displayPosition = 190,
            group = "GCS"
    )
    @ValueChooserModel(TimeZoneChooserValues.class)
    public String timeZoneID;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            elDefs = {RecordEL.class, TimeEL.class, TimeNowEL.class},
            evaluation = ConfigDef.Evaluation.EXPLICIT,
            defaultValue = "${time:now()}",
            label = "Time Basis",
            description = "Time basis to use for a record. Enter an expression that evaluates to a datetime. To use the " +
                    "processing time, enter ${time:now()}. To use field values, use '${record:value(\"<filepath>\")}'.",
            displayPosition = 200,
            group = "GCS"
    )
    public String timeDriverTemplate;

    @ConfigDef(
            required = false,
            type = ConfigDef.Type.STRING,
            defaultValue = "sdc",
            description = "Prefix for object names that will be uploaded on GCS",
            label = "Object Name Prefix",
            displayPosition = 210,
            group = "GCS"
    )
    public String fileNamePrefix;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.MODEL,
            label = "Data Format",
            displayPosition = 1,
            group = "DATA_FORMAT"
    )
    @ValueChooserModel(DataFormatChooserValues.class)
    public DataFormat dataFormat;

    @ConfigDefBean()
    public DataGeneratorFormatConfig dataGeneratorFormatConfig;

    public List<Stage.ConfigIssue> init(Stage.Context context, List<Stage.ConfigIssue> issues) {
        dataGeneratorFormatConfig.init(
                context,
                dataFormat,
                "GCS",
                 "GCSTargetConfigdataGeneratorFormatConfig",
                issues
        );

        return issues;
    }
}
