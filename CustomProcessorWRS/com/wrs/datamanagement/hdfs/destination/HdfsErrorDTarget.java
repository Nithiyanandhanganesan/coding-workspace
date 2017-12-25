package com.wrs.datamanagement.hdfs.destination;


import com.streamsets.pipeline.api.ErrorStage;
import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.StageDef;
import com.streamsets.pipeline.stage.destination.hdfs.HdfsDTarget;

/**
 * Created by zaziz on 7/31/17.
 */
@StageDef(
        version = 3,
        label = "Write to HDFS",
        description = "Writes records to HDFS",
        onlineHelpRefUrl = ""
)
@ErrorStage
@GenerateResourceBundle
public class HdfsErrorDTarget extends HdfsDTarget {
}
