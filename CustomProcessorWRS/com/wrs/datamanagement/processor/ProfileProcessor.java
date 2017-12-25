package com.wrs.datamanagement.processor;

import com.streamsets.pipeline.api.Batch;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.StageException;
import com.streamsets.pipeline.api.base.SingleLaneProcessor;
import com.streamsets.pipeline.api.base.SingleLaneRecordProcessor;

import com.wrs.datamanagement.service.ProfileService;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public abstract class ProfileProcessor extends SingleLaneProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(ProfileProcessor.class);
    /**
     * Gives access to the UI configuration of the stage provided by the {@link ProfileDProcessor} class.
     */
    public abstract String getProjectID();
    public abstract String getInstanceID();
    public abstract String getIDType();
    public abstract String getPartyOwner();
    public abstract String getCustomerId();

    private ProfileService profileService = null;

    /** {@inheritDoc} */
    @Override
    protected List<ConfigIssue> init() {
        // Validate configuration values and open any required resources.
        List<ConfigIssue> issues = super.init();
        profileService = new ProfileService(getContext(), getProjectID(), getInstanceID(), getCustomerId(), getIDType(), getPartyOwner());
        return issues;
    }

    @Override
    public void process(Batch batch, SingleLaneBatchMaker singleLaneBatchMaker) throws StageException {
        profileService.transform(batch, singleLaneBatchMaker);
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        // Clean up any open resources.
        super.destroy();
    }

}