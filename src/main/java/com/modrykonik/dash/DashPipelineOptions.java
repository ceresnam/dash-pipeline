package com.modrykonik.dash;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;

@SuppressWarnings("unused")
public interface DashPipelineOptions extends PipelineOptions {
    @Required
    ValueProvider<Integer> getServerid();
    void setServerid(ValueProvider<Integer> s);

    @Required
    ValueProvider<String> getDfrom();
    void setDfrom(ValueProvider<String> s);

    @Required
    ValueProvider<String> getDto();
    void setDto(ValueProvider<String> s);
}
