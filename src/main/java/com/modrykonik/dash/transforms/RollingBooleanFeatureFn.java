package com.modrykonik.dash.transforms;

import java.lang.reflect.Field;

import org.joda.time.Duration;
import org.joda.time.LocalDate;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.Filter;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.GlobalWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.SlidingWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.modrykonik.dash.model.UserStatsRow;

/**
 * Rollup of a boolean feature for N days.
 *
 * E.g. if an user has is_active=true on 10.1.2016, then is_active7d=true for
 * next 7 days (10.1. - 16.1.2016 inclusive)
 *
 */
public class RollingBooleanFeatureFn
	extends PTransform<PCollection<UserStatsRow>, PCollection<UserStatsRow>>
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

	class WindowedCreateUserStatsRowFn extends DoFn<Long, UserStatsRow>
		implements RequiresWindowAccess
	{

		@Override
		public void processElement(ProcessContext c)
			throws Exception
		{
			IntervalWindow w = (IntervalWindow) c.window();

			UserStatsRow urow = new UserStatsRow();
			urow.day = w.end().minus(Duration.standardDays(1)).toDateTime().toLocalDate(); //end() is exclusive
			urow.auth_user_id = c.element();

			Field fieldOut = UserStatsRow.class.getDeclaredField(colNameOut);
			fieldOut.setBoolean(urow, true);

			c.output(urow);
		}
	}

    @Override
    public PCollection<UserStatsRow> apply(PCollection<UserStatsRow> urows)
    	throws IllegalArgumentException
    {
    	urows = urows
	    	//skip urows more than numDays before dfrom and after dto, do not have to process them
    		.apply("FilterDaysIn", Filter.byPredicate((UserStatsRow urow) ->
        		urow.day.isAfter(dfrom.minusDays(numDays)) &&
    			(urow.day.isBefore(dto) || urow.day.isEqual(dto))
    		))
    		//filter where urow.is_active == true
    		.apply("FilterIsTrue", Filter.byPredicate((UserStatsRow urow) -> {
				try {
					Field fieldIn = UserStatsRow.class.getDeclaredField(colNameIn);
					return (boolean) fieldIn.get(urow);
				} catch (IllegalAccessException|NoSuchFieldException e) {
					throw new IllegalArgumentException(e);
				}
    		}));

    	// PCollection<UserStatsRow>  ->  PCollection<UserStatsRow> window
    	PCollection<UserStatsRow> uwindow = urows
    		.apply(Window.named("DailyFixedWindows")
    			.<UserStatsRow>into(
    				SlidingWindows.of(Duration.standardDays(numDays)).
    				every(Duration.standardDays(1))
    			)
    			//.triggering(AfterWatermark.pastEndOfWindow())
    			.accumulatingFiredPanes());

    	PCollection<Long> uids = uwindow
    		//PCollection<UserStatsRow>  ->  PCollection<Long>
    		.apply("TakeUserIds", MapElements
    			.via((UserStatsRow urow) -> urow.auth_user_id)
    			.withOutputType(new TypeDescriptor<Long>() {}))
    		//PCollection<Long>  ->  PCollection<Long>
    		.apply("UniqUserIds", new UniqFn<Long>());

    	PCollection<UserStatsRow> urowsOut = uids
    		//PCollection<Long> -> PCollection<UserStatsRow>
    		.apply("CreateUserStatsRow", ParDo.of(new WindowedCreateUserStatsRowFn()))
	    	//drop urows before dfrom and after dto, have not seen complete input data
	    	.apply("FilterDaysOut", Filter.byPredicate((UserStatsRow urow) ->
	    		(urow.day.isAfter(dfrom) || urow.day.isEqual(dfrom)) &&
	    		(urow.day.isBefore(dto) || urow.day.isEqual(dto))
	    	))
    		.apply("WindowEnd", Window.into(new GlobalWindows()));

    	return urowsOut;
    }
}