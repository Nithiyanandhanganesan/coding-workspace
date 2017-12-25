package com.wrs.datamanagement.groups;

import com.streamsets.pipeline.api.GenerateResourceBundle;
import com.streamsets.pipeline.api.Label;

/**
 * Created by zaziz on 6/6/17.
 */
@GenerateResourceBundle
public enum GoogleCloudStorageGroups implements Label {
    GCS("Google Cloud Storage"),
    DATA_FORMAT("Data Format");

    private final String label;

    private GoogleCloudStorageGroups(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }
}
