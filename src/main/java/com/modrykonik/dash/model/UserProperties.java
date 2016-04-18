package com.modrykonik.dash.model;

import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTimeZone;
import org.joda.time.Instant;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.DefaultCoder;
import com.modrykonik.dash.DashPipeline;

@DefaultCoder(AvroCoder.class)
public class UserProperties {

	public long auth_user_id;
	public Instant registered_on;

	public static UserProperties from(TableRow row) {
		UserProperties up = new UserProperties();

		up.auth_user_id = Long.parseLong((String) row.get("auth_user_id"));
		up.registered_on = Instant.parse((String) row.get("registered_on"), DashPipeline.bqDatetimeFmt);

		return up;
	}

	/**
	 * convert to BQ table row
	 */
	public TableRow toBQTableRow() {
		TableRow row = new TableRow();
		row.set("auth_user_id", Long.toString(auth_user_id));
		row.set("day", registered_on.toDateTime(DateTimeZone.UTC).toString());
		return row;
	}

	/**
	 * Build table schema for BQ output table.
	 */
	public static TableSchema toBQTableSchema() {
	    List<TableFieldSchema> fields = new ArrayList<>(50);
	    fields.add(new TableFieldSchema().setName("auth_user_id").setType("INTEGER").setMode("REQUIRED"));
	    fields.add(new TableFieldSchema().setName("registered_on").setType("TIMESTAMP").setMode("REQUIRED"));

	    TableSchema schema = new TableSchema().setFields(fields);
		return schema;
	}

}