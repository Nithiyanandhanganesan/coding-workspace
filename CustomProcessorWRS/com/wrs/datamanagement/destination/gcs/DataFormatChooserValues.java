package com.wrs.datamanagement.destination.gcs;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.config.DataFormat;

/**
 * Created by zaziz on 6/8/17.
 */
public class DataFormatChooserValues extends BaseEnumChooserValues<DataFormat> {

    public DataFormatChooserValues() {
        super(
                DataFormat.DELIMITED,
                DataFormat.JSON,
                DataFormat.SDC_JSON
        );
    }

}
