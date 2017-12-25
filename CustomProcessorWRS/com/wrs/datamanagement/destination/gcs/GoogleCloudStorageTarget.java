package com.wrs.datamanagement.destination.gcs;

import com.streamsets.pipeline.lib.el.TimeEL;
import com.streamsets.pipeline.lib.el.TimeNowEL;
import com.google.cloud.storage.*;
import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.BaseTarget;
import com.streamsets.pipeline.api.el.ELEval;
import com.streamsets.pipeline.api.el.ELVars;
import com.streamsets.pipeline.lib.generator.DataGenerator;
import com.wrs.datamanagement.lib.errors.Errors;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by zaziz on 5/31/17.
 */
public class GoogleCloudStorageTarget extends BaseTarget {

    private static final String EL_PREFIX = "${";
    private static final String PARTITION_TEMPLATE = "partitionTemplate";
    private static final String TIME_DRIVER_TEMPLATE = "timeDriverTemplate";

    private final GCSTargetConfig gcsTargetConfig;

    private Storage storage;

    private ELVars  elVars;
    private ELEval partitionEval;
    private Calendar calendar;

    public GoogleCloudStorageTarget(GCSTargetConfig gcsTargetConfig) {
        this.gcsTargetConfig = gcsTargetConfig;
    }

    /** {@inheritDoc} */
    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = gcsTargetConfig.init(getContext(), super.init());

        storage = StorageOptions.getDefaultInstance().getService();

        elVars = getContext().createELVars();
        partitionEval = getContext().createELEval(PARTITION_TEMPLATE);

        calendar = Calendar.getInstance(TimeZone.getTimeZone(gcsTargetConfig.timeZoneID));

        return issues;
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        // Clean up any open resources.
        super.destroy();
    }

    /** {@inheritDoc} */
    @Override
    public void write(Batch batch) throws StageException {
        ByteArrayOutputStream bOut = new ByteArrayOutputStream();

        DataGenerator gen = null;

        try {
            gen = gcsTargetConfig.dataGeneratorFormatConfig.getDataGeneratorFactory().getGenerator(bOut);
        } catch (IOException e) {
            throw new StageException(Errors.GENERIC, e);
        }

        Iterator<Record> batchIterator = batch.getRecords();
        while (batchIterator.hasNext()) {
            Record record = batchIterator.next();

            try {
                gen.write(record);
            } catch (IOException e) {
                throw new StageException(Errors.GENERIC, e);
            }
        }

        try {
            gen.close();
        } catch (IOException e) {
            throw new StageException(Errors.GENERIC, e);
        }

        if (bOut.size() > 0) {
            TimeEL.setCalendarInContext(elVars, calendar);
            TimeNowEL.setTimeNowInContext(elVars, new Date());

            String partition = partitionEval.eval(elVars, gcsTargetConfig.partitionTemplate, String.class);
            String path = gcsTargetConfig.commonPrefix + partition + "/" + gcsTargetConfig.fileNamePrefix + UUID.randomUUID();

            BlobId blobId = BlobId.of(gcsTargetConfig.bucketTemplate, path);
            BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType(getContentType()).build();
            Blob blob = storage.create(blobInfo, bOut.toByteArray());
        }

    }

    private String getContentType() {
        switch (gcsTargetConfig.dataFormat.getLabel()) {
            case "JSON":
                return "text/json";
            case "CSV":
                return "text/csv";
        }

        return null;
    }
}
