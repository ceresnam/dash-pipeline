package com.modrykonik.dash.transforms;

import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.modrykonik.dash.model.UserStatsComputedRow;
import org.joda.time.Duration;
import org.joda.time.LocalDate;

import java.lang.reflect.Field;

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
	private final LocalDate dfrom;
	private final LocalDate dto;

	public RollingBooleanFeatureFn(String colNameIn, String colNameOut,
								   int numDays,
								   LocalDate dfrom, LocalDate dto)
	{
		this.colNameIn = colNameIn;
		this.colNameOut = colNameOut;
		this.numDays = numDays;
		this.dfrom = dfrom;
		this.dto = dto;
	}

	private class WindowedCreateUserStatsComputedRowFn extends DoFn<Long, UserStatsComputedRow>
		implements RequiresWindowAccess
	{

		@Override
		public void processElement(ProcessContext c)
			throws Exception
		{
			IntervalWindow w = (IntervalWindow) c.window();

			UserStatsComputedRow ucrow = new UserStatsComputedRow();
			ucrow.day = w.end().minus(Duration.standardDays(1)).toDateTime().toLocalDate(); //end() is exclusive
			ucrow.auth_user_id = c.element();

			Field fieldOut = UserStatsComputedRow.class.getDeclaredField(colNameOut);
			fieldOut.setBoolean(ucrow, true);

			c.output(ucrow);
		}
	}

    @SuppressWarnings("UnnecessaryLocalVariable")
	@Override
    public PCollection<UserStatsComputedRow> apply(PCollection<UserStatsComputedRow> ucrows)
    	throws IllegalArgumentException
    {
    	ucrows = ucrows
	    	//skip ucrows more than numDays before dfrom and after dto, do not have to process them
    		.apply("FilterDaysIn", Filter.byPredicate((UserStatsComputedRow ucrow) ->
        		ucrow.day.isAfter(dfrom.minusDays(numDays)) &&
    			(ucrow.day.isBefore(dto) || ucrow.day.isEqual(dto))
    		))
    		//filter where ucrow.is_active == true
    		.apply("FilterIsTrue", Filter.byPredicate((UserStatsComputedRow ucrow) -> {
				try {
					Field fieldIn = UserStatsComputedRow.class.getDeclaredField(colNameIn);
					return (boolean) fieldIn.get(ucrow);
				} catch (IllegalAccessException|NoSuchFieldException e) {
					throw new IllegalArgumentException(e);
				}
    		}));

    	// PCollection<UserStatsComputedRow>  ->  PCollection<UserStatsComputedRow> window
    	PCollection<UserStatsComputedRow> uwindow = ucrows
    		.apply(Window.named("DailyFixedWindows")
    			.<UserStatsComputedRow>into(
    				SlidingWindows.of(Duration.standardDays(numDays)).
    				every(Duration.standardDays(1))
    			)
    			//.triggering(AfterWatermark.pastEndOfWindow())
    			.accumulatingFiredPanes());

    	PCollection<Long> uids = uwindow
    		//PCollection<UserStatsComputedRow>  ->  PCollection<Long>
    		.apply("TakeUserIds", MapElements
    			.via((UserStatsComputedRow ucrow) -> ucrow.auth_user_id)
    			.withOutputType(new TypeDescriptor<Long>() {}))
    		//PCollection<Long>  ->  PCollection<Long>
    		.apply("UniqUserIds", new UniqFn<>());

    	PCollection<UserStatsComputedRow> ucrowsOut = uids
    		//PCollection<Long> -> PCollection<UserStatsComputedRow>
    		.apply("CreateUserStatsRow", ParDo.of(new WindowedCreateUserStatsComputedRowFn()))
	    	//drop ucrows before dfrom and after dto, have not seen complete input data
	    	.apply("FilterDaysOut", Filter.byPredicate((UserStatsComputedRow ucrow) ->
	    		(ucrow.day.isAfter(dfrom) || ucrow.day.isEqual(dfrom)) &&
	    		(ucrow.day.isBefore(dto) || ucrow.day.isEqual(dto))
	    	))
    		.apply("WindowEnd", Window.into(new GlobalWindows()));

    	return ucrowsOut;
    }
}