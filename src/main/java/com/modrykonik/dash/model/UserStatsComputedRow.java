package com.modrykonik.dash.model;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.reflect.AvroIgnore;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.lang.reflect.Field;
import java.util.ArrayList;

import static com.modrykonik.dash.model.DateUtils.toBQDateStr;

@DefaultCoder(AvroCoder.class)
public class UserStatsComputedRow {

    public long day;
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

    @Nullable public boolean is_desktop;
    @Nullable public boolean is_desktop28d;
    @Nullable public boolean is_mobile;
    @Nullable public boolean is_mobile28d;

    @Nullable public long num_words;

    public UserStatsComputedRow() {}

    @AvroIgnore
    private static ArrayList<String> columns = null;

    /**
     * Returns list of columns that are computed (i.e. not loaded from big query)
     */
    private static ArrayList<String> getColumns() {
        if (columns != null)
            return columns;

        columns = new ArrayList<>(50);
        Field[] fields = UserStatsComputedRow.class.getDeclaredFields();
        for (Field f : fields) {
            String name = f.getName();
            if (name.startsWith("is_") || name.startsWith("has_") || name.startsWith("num_")) {
                columns.add(name);
            }
        }

        return columns;
    }

    /**
     * Returns value of the given feature
     *
     * @param name - name of column
     * @return value
     */
    public boolean isTrue(String name) {
        try {
            Field fieldIn = UserStatsComputedRow.class.getDeclaredField(name);
            return (boolean) fieldIn.get(this);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Sets true value for the given feature
     *
     * @param name - name of column
     */
    public void setTrue(String name) {
        try {
            Field fieldOut = UserStatsComputedRow.class.getDeclaredField(name);
            fieldOut.setBoolean(this, true);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * convert to BQ table row
     */
    public TableRow toBQTableRow() {
        TableRow bqrow = new TableRow();
        bqrow.set("day", toBQDateStr(day));
        bqrow.set("auth_user_id", Long.toString(auth_user_id));

        boolean allNull = true;
        for (String fieldName : getColumns()) {
            try {
                Field f = UserStatsComputedRow.class.getDeclaredField(fieldName);
                if (f.getType().isAssignableFrom(long.class)) {
                    long val = f.getLong(this);
                    if (val>0) {
                        //set only if not 0, dash aggregation queries assume null instead of 0 value
                        bqrow.set(fieldName, val);
                        allNull = false;
                    }
                } else if (f.getType().isAssignableFrom(boolean.class)) {
                    boolean val = f.getBoolean(this);
                    if (val) {
                        //set only if true, dash aggregation queries assume null instead of FALSE value
                        bqrow.set(fieldName, true);
                        allNull = false;
                    }
                }
            } catch (IllegalAccessException | NoSuchFieldException e) {
                throw new RuntimeException(e);
            }
        }
        assert !allNull;

        return bqrow;
    }


    /**
     * merge several rows for the same day and auth_user_id in to one row
     * using logical OR.
     */
    public static UserStatsComputedRow orMerge(Iterable<UserStatsComputedRow> urows) {
        UserStatsComputedRow merged = null;

        for (UserStatsComputedRow ucrow : urows) {
            if (merged == null) {
                merged = new UserStatsComputedRow();
                merged.day = ucrow.day;
                merged.auth_user_id = ucrow.auth_user_id;
            } else {
                assert merged.day == ucrow.day;
                assert merged.auth_user_id == ucrow.auth_user_id;
            }

            for (String fieldName : getColumns()) {
                try {
                    Field f = UserStatsComputedRow.class.getDeclaredField(fieldName);
                    if (f.getType().isAssignableFrom(long.class)) {
                        f.setLong(merged, f.getLong(merged) + f.getLong(ucrow));
                    } else if (f.getType().isAssignableFrom(boolean.class)) {
                        f.setBoolean(merged, f.getBoolean(merged) || f.getBoolean(ucrow));
                    }
                } catch (IllegalAccessException | NoSuchFieldException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return merged;
    }

}
