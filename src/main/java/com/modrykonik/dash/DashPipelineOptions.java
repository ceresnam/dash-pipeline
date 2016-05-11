package com.modrykonik.dash;

import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.Validation.Required;

@SuppressWarnings("unused")
public interface DashPipelineOptions extends PipelineOptions {
    @Description("Server ID (201 or 202)")
    @Required
    int getServerid();
    void setServerid(int s);

    @Description("Date from (inclusive)")
    @Required
    String getDfrom();
    void setDfrom(String s);

    @Description("Date to (inclusive)")
    @Required
    String getDto();
    void setDto(String s);

}