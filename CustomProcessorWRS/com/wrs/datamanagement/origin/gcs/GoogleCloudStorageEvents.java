package com.wrs.datamanagement.origin.gcs;

import com.streamsets.pipeline.lib.event.EventCreator;

/**
 * Created by zaziz on 8/22/17.
 */
public class GoogleCloudStorageEvents {
    public static final EventCreator NO_MORE_DATA = new EventCreator.Builder("no-more-data", 1)
            .withOptionalField("record-count")
            .withOptionalField("error-count")
            .withOptionalField("file-count")
            .build();
}