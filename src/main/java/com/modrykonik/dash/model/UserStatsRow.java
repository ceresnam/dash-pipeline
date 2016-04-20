package com.modrykonik.dash.model;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.reflect.AvroEncode;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.Nullable;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.modrykonik.dash.io.LocalDateEncoding;

@DefaultCoder(AvroCoder.class)
public class UserStatsRow implements Cloneable {

	@AvroEncode(using=LocalDateEncoding.class)
	public LocalDate day;
	public long auth_user_id;

	/*
	 *  Columns cached from big query table row
	 */
	@Nullable public boolean has_registered;
	@Nullable public boolean has_pregnancystate_pregnant;
	@Nullable public boolean has_pregnancystate_trying;
	@Nullable public boolean has_profile_avatar;
	@Nullable public boolean has_profile_birthdate;
	@Nullable public boolean has_profile_child;
	@Nullable public boolean has_profile_city;
	@Nullable public boolean has_profile_county;

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

	@Nullable public boolean has_pregnancystate_pregnant_active28d;
	@Nullable public boolean has_pregnancystate_trying_active28d;
	@Nullable public boolean has_profile_avatar_active28d;
	@Nullable public boolean has_profile_birthdate_active28d;
	@Nullable public boolean has_profile_child_active28d;
	@Nullable public boolean has_profile_city_active28d;
	@Nullable public boolean has_profile_county_active28d;

	@Nullable public boolean has_pregnancystate_pregnant_alive28d;
	@Nullable public boolean has_pregnancystate_trying_alive28d;
	@Nullable public boolean has_profile_avatar_alive28d;
	@Nullable public boolean has_profile_birthdate_alive28d;
	@Nullable public boolean has_profile_child_alive28d;
	@Nullable public boolean has_profile_city_alive28d;
	@Nullable public boolean has_profile_county_alive28d;

	public UserStatsRow() {}

	@AvroIgnore
	private static ArrayList<String> computedColumns = null;

	/**
	 * Returns list of columns that are computed (i.e. not loaded from big query)
	 */
	public static ArrayList<String> getComputedColumns() {
		if (computedColumns!=null)
			return computedColumns;

		computedColumns = new ArrayList<>(50);
		Field[] fields = UserStatsRow.class.getDeclaredFields();
		for (Field f: fields) {
			String name = f.getName();
			if (name.startsWith("is_") ||
				name.equals("has_registered7d") ||
				name.equals("has_registered28d") ||
				name.equals("has_registered90d"))
			{
				computedColumns.add(name);
			}
		}

		return computedColumns;
	}

	/**
	 * Build table schema for BQ output table.
	 */
	public static TableSchema toBQTableSchema() {
	    List<TableFieldSchema> dataFields = new ArrayList<>(50);
	    for (String colName : UserStatsRow.getComputedColumns()) {
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
		for (String fieldName: getComputedColumns()) {
			try {
				Field f = UserStatsRow.class.getDeclaredField(fieldName);
				Boolean val = f.getBoolean(this);
				if (val!=null && val.booleanValue()) {
					//dash aggregation queries assume null instead of FALSE value
					data.set(fieldName, val);
				}
			} catch (IllegalAccessException|NoSuchFieldException e) {
				throw new RuntimeException(e);
			}
		}

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
	public static UserStatsRow orMerge(Iterable<UserStatsRow> urows) {
		UserStatsRow merged = null;

		for (UserStatsRow urow : urows) {
			if (merged==null) {
				merged = new UserStatsRow();
				merged.day = urow.day;
				merged.auth_user_id = urow.auth_user_id;
			} else {
				assert merged.day == urow.day;
				assert merged.auth_user_id == urow.auth_user_id;
			}

			for (String fieldName: getComputedColumns()) {
				try {
					Field f = UserStatsRow.class.getDeclaredField(fieldName);
					f.setBoolean(merged, f.getBoolean(merged) || f.getBoolean(urow));
				} catch (IllegalAccessException|NoSuchFieldException e) {
					throw new RuntimeException(e);
				}
			}
		}

		return merged;
	}

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

}