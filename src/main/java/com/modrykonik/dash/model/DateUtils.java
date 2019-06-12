package com.modrykonik.dash.model;

import org.apache.beam.sdk.transforms.SerializableFunction;
import org.joda.time.DateTimeZone;
import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

public class DateUtils {

    /**
     * Datetime format returned in exported bigquery data
     */
    private static final DateTimeFormatter bqDatetimeFmt =
        new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .appendOptional(new DateTimeFormatterBuilder().appendLiteral('.').appendFractionOfSecond(1, 6).toParser())
            .appendLiteral(" UTC")
            .toFormatter().withZoneUTC();

    /**
     * Return true, if date d is between dates dfrom (inclusive) and dto (inclusive)
     *
     * @param d date to test
     * @param dfrom date interval start
     * @param dto   date interval end (inclusive)
     * @return true if in interval
     */
    public static boolean isBetween(LocalDate d, LocalDate dfrom, LocalDate dto) {
        return
            (d.isAfter(dfrom) || d.isEqual(dfrom)) &&
            (d.isBefore(dto) || d.isEqual(dto));
    }

    /**
     * Converts long (timestamp) to local date in utc timezone
     *
     * @param ts timestamp as long
     * @return date
     */
    public static LocalDate toLocalDate(long ts) {
        return toLocalDate(new Instant(ts));
    }

    /**
     * Converts timestamp to local date in utc timezone
     *
     * @param ts timestamp as Instant
     * @return date
     */
    public static LocalDate toLocalDate(Instant ts) {
        return ts.toDateTime(DateTimeZone.UTC).toLocalDate();
    }

    /**
     * Converts date to timestamp
     *
     * @param d date
     * @return timestamp as long
     */
    public static long toTimestampAtDayStart(LocalDate d) {
        return d.toDateTimeAtStartOfDay(DateTimeZone.UTC).getMillis();
    }

    /**
     * Parses timestamp string to long from format returned by bigquery export
     *
     * @param ts timestamp as string
     * @return timestamp as long
     */
    public static LocalDate fromBQDateStr(String ts) {
        try {
            return Instant.parse(ts, bqDatetimeFmt).toDateTime(DateTimeZone.UTC).toLocalDate();
        } catch (IllegalArgumentException e) {
            return Instant.parse(ts, ISODateTimeFormat.dateTime()).toDateTime(DateTimeZone.UTC).toLocalDate();
        }
    }

    /**
     * Converts timestamp long to string that is accepted in bigquery import
     *
     * @param ts timestamp as long
     * @return timestamp as string
     */
    public static String toBQDateStr(long ts) {
        return new Instant(ts).toDateTime(DateTimeZone.UTC).toString();
    }

    /**
     * Parse string with date in iso-format (YYYY-mm-dd)
     *
     * @param s date as string
     * @return date
     */
    public static LocalDate fromStr(String s) {
        return LocalDate.parse(s);
    }

}
