package com.modrykonik.dash.io;

import com.google.api.services.bigquery.model.TableRow;
import org.joda.time.LocalDate;

import java.util.Map;
import java.util.stream.StreamSupport;

import static com.modrykonik.dash.model.DateUtils.fromBQDateStr;
import static com.modrykonik.dash.model.DateUtils.toTimestampAtDayStart;

public class BQUtils {

    /**
     * Extract DATE value from BQ table cell
     */
    public static LocalDate parseDate(TableRow row, String colName) {
        String val = (String) row.get(colName);
        if (val==null)
            return null;
        else
            return fromBQDateStr(val);
    }

    /**
     * Extract DATE value from BQ table cell. Return 0 if null in BQ table
     */
    public static long parseDateMillis(TableRow row, String colName) {
        LocalDate d = parseDate(row, colName);
        return d==null ? 0 : toTimestampAtDayStart(d);
    }

    /**
     * Extract BOOLEAN value from BQ table cell. Return 0 if null in BQ table
     */
    public static boolean parseBoolean(TableRow row, String colName) {
        Boolean val = (Boolean) row.get(colName);
        return val!=null && val;
    }

    /**
     * Extract INTEGER value from BQ table cell. Return 0 if null in BQ table
     */
    public static long parseLong(TableRow row, String colName) {
        String val = (String) row.get(colName);
        return val!=null ? Long.parseLong(val) : 0;
    }

    private static void mergeIntoFirst(TableRow row1, Map<String, Object> row2) {
        row2.entrySet().forEach((Map.Entry<String, Object> e) -> {
            String colName = e.getKey();
            Object val = e.getValue();
            if (val==null) return;

            if (val instanceof Map) {
                TableRow subResult = (TableRow) row1.get(colName);
                if (subResult==null) {
                    subResult = new TableRow();
                    row1.set(colName, subResult);
                }
                mergeIntoFirst(subResult, (Map<String, Object>) val);
            } else {
                row1.set(colName, val);
            }
        });
    }

    /**
     * Merge list of TableRows into single TableRow. If several rows have field
     * with the same name, value of tha last TableRow is used. Nested RECORDs
     * are merged recursively.
     *
     * @param trows list of TableRows
     * @return merged TableRow
     */
    public static TableRow mergeTableRows(Iterable<TableRow> trows) {
        TableRow result = new TableRow();
        StreamSupport.stream(trows.spliterator(), false).forEach(
            r -> mergeIntoFirst(result, r)
        );

        return result;
    }
}
