package com.modrykonik.dash.model;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.modrykonik.dash.DashPipeline;
import com.modrykonik.dash.io.LocalDateEncoding;
import org.apache.avro.reflect.AvroEncode;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.Nullable;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.LocalDate;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

@DefaultCoder(AvroCoder.class)
public class UserStatsComputedRow {

	@AvroEncode(using=LocalDateEncoding.class)
	public LocalDate day;
	public long auth_user_id;

	/*
	 *  Columns computed with dataflow
	 */
	@Nullable public boolean is_photoblog_active;
	@Nullable public boolean is_group_active;
	@Nullable public boolean is_pbandgroup_active;
	@Nullable public boolean is_forum_active;
	@Nullable public boolean is_bazar_active;
	@Nullable public boolean is_wiki_active;
	@Nullable public boolean is_ip_active;
	@Nullable public boolean is_hearts_active;
	@Nullable public boolean is_bazar_active28d;
	@Nullable public boolean is_forum_active28d;
	@Nullable public boolean is_group_active28d;
	@Nullable public boolean is_photoblog_active28d;

	@Nullable public boolean is_active;
	@Nullable public boolean is_active7d;
	@Nullable public boolean is_active28d;
	@Nullable public boolean is_active90d;

	@Nullable public boolean is_alive;
	@Nullable public boolean is_alive7d;
	@Nullable public boolean is_alive28d;
	@Nullable public boolean is_alive90d;

	@Nullable public boolean tmp_has_registered; //helper, carrying over data from UserStatsRow
	@Nullable public boolean has_registered7d;
	@Nullable public boolean has_registered28d;
	@Nullable public boolean has_registered90d;

	@Nullable public boolean is_bazar_alive;
	@Nullable public boolean is_forum_alive;
	@Nullable public boolean is_group_alive;
	@Nullable public boolean is_photoblog_alive;
	@Nullable public boolean is_bazar_alive28d;
	@Nullable public boolean is_forum_alive28d;
	@Nullable public boolean is_group_alive28d;
	@Nullable public boolean is_photoblog_alive28d;

	public UserStatsComputedRow() {}

	@AvroIgnore
	private static ArrayList<String> columns = null;

	/**
	 * Returns list of columns that are computed (i.e. not loaded from big query)
	 */
	private static ArrayList<String> getColumns() {
		if (columns!=null)
			return columns;

		columns = new ArrayList<>(50);
		Field[] fields = UserStatsComputedRow.class.getDeclaredFields();
		for (Field f: fields) {
			String name = f.getName();
			if (name.startsWith("is_") || name.startsWith("has_")) {
				columns.add(name);
			}
		}

		return columns;
	}

	/**
	 * Return true, if row's date is between dates dfrom (inclusive) and dto (inclusive)
	 * @param dfrom date interval start
	 * @param dto date interval end
     * @return true if in interval
     */
	public boolean dayBetween(LocalDate dfrom, LocalDate dto) {
		LocalDate d = dayAsDate();
		return
			(d.isAfter(dfrom) || d.isEqual(dfrom)) &&
			(d.isBefore(dto) || d.isEqual(dto));
	}

	public static UserStatsComputedRow fromBQTableRow(TableRow row) {
		UserStatsComputedRow ucrow = new UserStatsComputedRow();
		ucrow.day = Instant.parse((String) row.get("day"), DashPipeline.bqDatetimeFmt).toDateTime().toLocalDate();
		ucrow.auth_user_id = UserStatsRow.parseLong(row, "auth_user_id");
		assert ucrow.auth_user_id!=0;

        TableRow data = (TableRow) row.get("data");
		try {
			for (String name : getColumns()) {
				Field f = UserStatsComputedRow.class.getDeclaredField(name);
				if (f.getType().isAssignableFrom(long.class)) {
					long val = UserStatsRow.parseLong(data, name);
					f.setLong(ucrow, val);
				} else if (f.getType().isAssignableFrom(boolean.class)) {
					boolean val = UserStatsRow.parseBoolean(data, name);
					f.setBoolean(ucrow, val);
				}
			}

			return ucrow;
		} catch(NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Build table schema for BQ output table.
	 */
	@SuppressWarnings("UnnecessaryLocalVariable")
    public static TableSchema toBQTableSchema() {
	    List<TableFieldSchema> dataFields = new ArrayList<>(50);
	    for (String colName : getColumns()) {
	    	dataFields.add(new TableFieldSchema().setName(colName).setType("BOOLEAN").setMode("NULLABLE"));
	    }

	    List<TableFieldSchema> fields = new ArrayList<>(50);
	    fields.add(new TableFieldSchema().setName("day").setType("TIMESTAMP").setMode("REQUIRED"));
	    fields.add(new TableFieldSchema().setName("auth_user_id").setType("INTEGER").setMode("REQUIRED"));
	    fields.add(new TableFieldSchema().setName("data").setType("RECORD").setMode("REQUIRED").setFields(dataFields));
	    TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}

	/**
	 * convert to BQ table row
	 */
	public TableRow toBQTableRow() {
		TableRow data = new TableRow();
        boolean allFalse = true;
		for (String fieldName: getColumns()) {
			try {
				Field f = UserStatsComputedRow.class.getDeclaredField(fieldName);
				boolean val = f.getBoolean(this);
				if (val) {
					//set only if true, dash aggregation queries assume null instead of FALSE value
					data.set(fieldName, true);
                    allFalse = false;
				}
			} catch (IllegalAccessException|NoSuchFieldException e) {
				throw new RuntimeException(e);
			}
		}
        assert !allFalse;

		TableRow bqrow = new TableRow();
		bqrow.set("day", day.toDateTimeAtStartOfDay(DateTimeZone.UTC).toString());
		bqrow.set("auth_user_id", Long.toString(auth_user_id));
		bqrow.set("data", data);

		return bqrow;
	}


	/**
	 * merge several rows for the same day and auth_user_id in to one row
	 * using logical OR.
	 */
	public static UserStatsComputedRow orMerge(Iterable<UserStatsComputedRow> urows) {
		UserStatsComputedRow merged = null;

		for (UserStatsComputedRow ucrow : urows) {
			if (merged==null) {
				merged = new UserStatsComputedRow();
				merged.day = ucrow.day;
				merged.auth_user_id = ucrow.auth_user_id;
			} else {
				assert merged.day == ucrow.day;
				assert merged.auth_user_id == ucrow.auth_user_id;
			}

			for (String fieldName: getColumns()) {
				try {
					Field f = UserStatsComputedRow.class.getDeclaredField(fieldName);
					f.setBoolean(merged, f.getBoolean(merged) || f.getBoolean(ucrow));
				} catch (IllegalAccessException|NoSuchFieldException e) {
					throw new RuntimeException(e);
				}
			}
		}

		return merged;
	}

}