package com.modrykonik.dash.transforms;

import com.modrykonik.dash.model.DateUtils;
import com.modrykonik.dash.model.UserStatsComputedRow;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.LocalDate;

import static com.modrykonik.dash.model.DateUtils.*;

/**
 * Rollup of a boolean feature for N days.
 *
 * E.g. if an user has is_active=true on 10.1.2016, then is_active7d=true for
 * next 7 days (10.1. - 16.1.2016 inclusive)
 *
 */
public class RollingBooleanFeatureFn
    extends PTransform<PCollection<UserStatsComputedRow>, PCollection<UserStatsComputedRow>>
{

    private final String colNameIn;
    private final String colNameOut;
    private final int numDays;
    private final ValueProvider<LocalDate> dfrom;
    private final ValueProvider<Long> dfromMillis;
    private final ValueProvider<LocalDate> dto;
    private final ValueProvider<Long> dtoMillis;

    public RollingBooleanFeatureFn(String colNameIn, String colNameOut,
                                   int numDays,
                                   ValueProvider<LocalDate> dfrom, ValueProvider<LocalDate> dto)
    {
        this.colNameIn = colNameIn;
        this.colNameOut = colNameOut;
        this.numDays = numDays;
        this.dfrom = dfrom;
        this.dfromMillis = NestedValueProvider.of(dfrom, DateUtils::toTimestampAtDayStart);
        this.dto = dto;
        this.dtoMillis = NestedValueProvider.of(dto, DateUtils::toTimestampAtDayStart);
    }

    private class CreateUserStatsComputedRowFn extends DoFn<KV<Long,Long>, UserStatsComputedRow>
    {
        @ProcessElement
        public void processElement(ProcessContext c)
            throws Exception
        {
            KV<Long,Long> day_userid = c.element();

            UserStatsComputedRow ucrow = new UserStatsComputedRow();
            ucrow.day = day_userid.getKey();
            ucrow.auth_user_id = day_userid.getValue();
            ucrow.setTrue(colNameOut);

            c.output(ucrow);
        }
    }

    private class RollFn extends DoFn<UserStatsComputedRow, KV<Long,Long>>
    {
        @ProcessElement
        public void processElement(ProcessContext c) {
            UserStatsComputedRow ucrow = c.element();

            //if isTrue() for ucrow.day, then it is true for
            //next (numDays-1) days in rolling feature
            for (int i=0; i<numDays; i++) {
                long day = new Instant(ucrow.day).plus(Duration.standardDays(i)).getMillis();

                //do not generate days before dfrom and after dto
                if (day<dfromMillis.get() || day>dtoMillis.get())
                    continue;

                c.output(KV.of(day, ucrow.auth_user_id));
            }
        }
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    @Override
    public PCollection<UserStatsComputedRow> expand(PCollection<UserStatsComputedRow> ucrows)
        throws IllegalArgumentException
    {
        PCollection<KV<Long,Long>> dayUids = ucrows
            //skip ucrows more than numDays before dfrom and after dto,
            //or where value is false. we do not have to process them
            //PCollection<UserStatsComputedRow>  ->  PCollection<UserStatsComputedRow>
            .apply("FilterDaysIn", Filter.by((UserStatsComputedRow ucrow) ->
                isBetween(toLocalDate(ucrow.day), dfrom.get().minusDays(numDays-1), dto.get()) &&
                ucrow.isTrue(colNameIn)
            ))
            //PCollection<UserStatsComputedRow>  ->  PCollection<KV<Long,Long>>
            //if feature is true, generate userid for (numDays-1) subsequent days
            .apply("Roll", ParDo.of(new RollFn()));

        //force to materialize PCollection<Long> by using GroupByKey inside UniqFn
        //see https://cloud.google.com/dataflow/service/dataflow-service-desc#fusion-prevention
        //this fixes java.lang.OutOfMemoryError
        //http://stackoverflow.com/questions/36793797/

        PCollection<UserStatsComputedRow> ucrowsOut = dayUids
            //keep each userid only once per each day (i.e. last day of window)
            //PCollection<KV<Long,Long>>  ->  PCollection<KV<Long,Long>>
            .apply("UniqUserIds", new UniqFn<>())
            //PCollection<KV<Long,Long>>  ->  PCollection<UserStatsComputedRow>
            .apply("CreateUserStatsRow", ParDo.of(new CreateUserStatsComputedRowFn()));

        return ucrowsOut;
    }

}
