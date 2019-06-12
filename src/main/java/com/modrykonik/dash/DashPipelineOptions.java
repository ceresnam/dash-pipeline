package com.modrykonik.dash;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

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