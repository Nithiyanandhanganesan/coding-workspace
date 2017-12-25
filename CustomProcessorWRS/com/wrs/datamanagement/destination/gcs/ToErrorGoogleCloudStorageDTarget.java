package com.wrs.datamanagement.destination.gcs;

import com.streamsets.pipeline.api.*;

/**
 * Created by zaziz on 6/19/17.
 */
@StageDef(
        version = 3,
        label = "Write to Google Cloud Storage",
        description = "Writes records to Google Cloud Storage",
        onlineHelpRefUrl = ""
)
@ErrorStage
@GenerateResourceBundle
public class ToErrorGoogleCloudStorageDTarget extends GoogleCloudStorageDTarget {
}
