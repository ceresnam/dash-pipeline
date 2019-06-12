package com.modrykonik.dash.model;

import com.google.api.services.bigquery.model.TableRow;
import com.modrykonik.dash.DashPipeline;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.Nullable;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.lang.reflect.Field;
import java.util.ArrayList;

@DefaultCoder(AvroCoder.class)
public class UserStatsRow {

	public long day;
	public long auth_user_id;

	/*
	 *  Columns cached from big query table row
	 */
	@Nullable public boolean has_registered;

	@Nullable public long num_photoblog_posts;
	@Nullable public long num_photoblog_comments;
	@Nullable public long num_photoblog_likes_given;
	@Nullable public long num_photoblog_likes_given_post;

	@Nullable public long num_group_joined;
	@Nullable public long num_group_posts;
	@Nullable public long num_group_post_comments;
	@Nullable public long num_group_likes_given_post;
	@Nullable public long num_groups;

	@Nullable public long num_forum_threads;
	@Nullable public long num_forum_messages;
	@Nullable public long num_forum_likes_given_thread;
	@Nullable public long num_forum_likes_given_message;

	@Nullable public long num_bazar_products;
	@Nullable public long num_bazar_products_reposted;
	@Nullable public long num_bazar_reviews;
	@Nullable public long num_bazar_transaction_message_to_seller;
	@Nullable public long num_bazar_transaction_message_to_buyer;
	@Nullable public long num_bazar_interest_made;
	@Nullable public long num_bazar_wishlist_added;
	@Nullable public long num_bazar_likes_given;

	@Nullable public long num_wiki_experiences;
	@Nullable public long num_wiki_likes_given_experience;

	@Nullable public long num_ip_sent;
	@Nullable public long num_ip_starred;

	@Nullable public long num_hearts_given;

	@Nullable public long num_logins;
	@Nullable public long num_minutes_on_site;
	@Nullable public long num_minutes_on_site_forum;
	@Nullable public long num_minutes_on_site_bazar;
	@Nullable public long num_minutes_on_site_group;
	@Nullable public long num_minutes_on_site_photoblog;

	public UserStatsRow() {}

	@AvroIgnore
	private static ArrayList<String> columns = null;

	/**
	 * Returns list of columns that are loaded from big query
	 */
	private static ArrayList<String> getColumns() {
		if (columns!=null)
			return columns;

		columns = new ArrayList<>(50);
		Field[] fields = UserStatsRow.class.getDeclaredFields();
		for (Field f: fields) {
			String name = f.getName();
			if (name.startsWith("num_") || name.startsWith("has_")) {
				columns.add(name);
			}
		}

		return columns;
	}

    /**
     * Extract DATE value from BQ table cell
     */
    static LocalDate parseDate(TableRow row, String colName) {
        String val = (String) row.get(colName);
        if (val==null)
            return null;
        else
            return Instant.parse(val, DashPipeline.bqDatetimeFmt).toDateTime(DateTimeZone.UTC).toLocalDate();
    }

    /**
     * Extract DATE value from BQ table cell. Return 0 if null in BQ table
     */
    static long parseDateMillis(TableRow row, String colName) {
        LocalDate d = parseDate(row, colName);
        return d==null ? 0 : d.toDateTimeAtStartOfDay(DateTimeZone.UTC).getMillis();
    }

    /**
	 * Extract BOOLEAN value from BQ table cell. Return 0 if null in BQ table
	 */
	static boolean parseBoolean(TableRow row, String colName) {
		Boolean val = (Boolean) row.get(colName);
		return val!=null && val;
	}

	/**
	 * Extract INTEGER value from BQ table cell. Return 0 if null in BQ table
	 */
	static long parseLong(TableRow row, String colName) {
		String val = (String) row.get(colName);
		return val!=null ? Long.parseLong(val) : 0;
	}

	public static UserStatsRow fromBQTableRow(TableRow row) {
		UserStatsRow urow = new UserStatsRow();
		urow.day = parseDateMillis(row, "day");
		urow.auth_user_id = parseLong(row, "auth_user_id");
		assert urow.day!=0 && urow.auth_user_id!=0;

        TableRow data = (TableRow) row.get("data");
		try {
			for (String name : getColumns()) {
				Field f = UserStatsRow.class.getDeclaredField(name);
				if (f.getType().isAssignableFrom(long.class)) {
					long val = parseLong(data, name);
					f.setLong(urow, val);
				} else if (f.getType().isAssignableFrom(boolean.class)) {
					boolean val = parseLong(data, name)!=0;
					f.setBoolean(urow, val);
				}
			}

			return urow;
		} catch(NoSuchFieldException | IllegalAccessException e) {
			throw new RuntimeException(e);
		}
	}

}