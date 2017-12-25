package com.wrs.datamanagement.processor;

import com.streamsets.pipeline.api.ChooserValues;
import com.streamsets.pipeline.api.Label;
import com.streamsets.pipeline.api.base.BaseEnumChooserValues;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zaziz on 8/18/17.
 */
public enum CustomerIDType implements Label {
    AppNexus("AppNexus"),
    Css("Css"),
    Customer("Customer")
    ;

    private final String label;

    CustomerIDType(String label) {
        this.label = label;
    }

    @Override
    public String getLabel() {
        return label;
    }
}
