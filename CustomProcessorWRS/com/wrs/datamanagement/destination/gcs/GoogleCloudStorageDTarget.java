package com.wrs.datamanagement.destination.gcs;

import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.configurablestage.DTarget;
import com.wrs.datamanagement.groups.GoogleCloudStorageGroups;

/**
 * Created by zaziz on 6/6/17.
 */
@StageDef(
        version = 1,
        label = "Google Cloud Storage",
        description = "",
        icon = "cloud-storage-logo.png",
        recordsByRef = true,
        onlineHelpRefUrl = ""
)
@ConfigGroups(value = GoogleCloudStorageGroups.class)
@GenerateResourceBundle
public class GoogleCloudStorageDTarget extends DTarget {

    @ConfigDefBean()
    public GCSTargetConfig gcsTargetConfig;

    @Override
    protected Target createTarget() {
        return new GoogleCloudStorageTarget(gcsTargetConfig);
    }
}
