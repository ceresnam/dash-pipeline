package com.modrykonik.dash.io;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.NestedValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions;
import org.joda.time.LocalDate;

import java.io.Serializable;

/**
 * Read and write from partitioned UserStats table in bigquery
 */
public class BQUserStatsIO {

    private static class QueryValueProvider implements ValueProvider<String>, Serializable {
        private final ValueProvider<Integer> serverId;
        private final ValueProvider<LocalDate> dfrom;
        private final ValueProvider<LocalDate> dto;
        private final String nameSuffix;

        private transient volatile String cachedQuery;

        QueryValueProvider(ValueProvider<Integer> serverId,
                           ValueProvider<LocalDate> dfrom,
                           ValueProvider<LocalDate> dto,
                           String nameSuffix) {
            this.serverId = (ValueProvider) Preconditions.checkNotNull(serverId);
            this.dfrom = (ValueProvider) Preconditions.checkNotNull(dfrom);
            this.dto = (ValueProvider) Preconditions.checkNotNull(dto);
            this.nameSuffix = nameSuffix;
        }

        public static QueryValueProvider of(ValueProvider<Integer> serverId,
                                            ValueProvider<LocalDate> dfrom,
                                            ValueProvider<LocalDate> dto,
                                            String nameSuffix) {
            return new QueryValueProvider(serverId, dfrom, dto, nameSuffix);
        }

        public String get() {
            if(cachedQuery == null) {
                cachedQuery = String.format(
                    "SELECT *" +
                        "    FROM user_stats_%d.ds_daily_user_stats%s" +
                        "    WHERE day>='%s' and day<='%s'",
                    serverId.get(),
                    nameSuffix.equals("") ? "" : "_" + nameSuffix,
                    dfrom.get(),
                    dto.get()
                );

            }

            return this.cachedQuery;
        }

        public boolean isAccessible() {
            return serverId.isAccessible() && dfrom.isAccessible() && dto.isAccessible();
        }

        public String toString() {
            return this.isAccessible()
                ? String.valueOf(this.get())
                : MoreObjects.toStringHelper(this)
                .add("serverId", serverId)
                .add("dfrom", dfrom)
                .add("dto", dto)
                .toString();
        }
    }

    /**
     * Read rows from partitioned big query table for given date range.
     */
    public static class Read
        extends PTransform<PInput, PCollection<TableRow>>
    {
        private final ValueProvider<Integer> serverId;
        private final ValueProvider<LocalDate> dfrom;
        private final ValueProvider<LocalDate> dto;
        private final String nameSuffix;

        private Read(ValueProvider<Integer> serverId,
                     ValueProvider<LocalDate> dfrom,
                     ValueProvider<LocalDate> dto,
                     String nameSuffix)
        {
            this.serverId = serverId;
            this.dfrom = dfrom;
            this.dto = dto;
            this.nameSuffix = nameSuffix;
        }

        public static Read from(ValueProvider<Integer> serverId,
                                ValueProvider<LocalDate> dfrom,
                                ValueProvider<LocalDate> dto)
        {
            return from(serverId, dfrom, dto, "");
        }

        public static Read from(ValueProvider<Integer> serverId,
                                ValueProvider<LocalDate> dfrom,
                                ValueProvider<LocalDate> dto,
                                String nameSuffix)
        {
            return new Read(serverId, dfrom, dto, nameSuffix);
        }

        @Override
        public PCollection<TableRow> expand(PInput input) {
            Pipeline pipe = input.getPipeline();

            QueryValueProvider query = QueryValueProvider.of(serverId, dfrom, dto, nameSuffix);

            PCollection<TableRow> trows = pipe
                .apply("BQRead", BigQueryIO
                    .readTableRows()
                    .fromQuery(query)
                    .usingStandardSql()
                    .withTemplateCompatibility()
                    .withoutValidation()
                )
                .apply("Remove timestamps", Window.into(new GlobalWindows()));


            return trows;
        }
    }


    /**
     * Write rows of given date range to sharded big query table.
     */
    public static class Write
        extends PTransform<PCollection<TableRow>, PDone>
    {
        //values defining table name
        private final ValueProvider<Integer> serverId;
        private final String nameSuffix;

        Write(ValueProvider<Integer> serverId,
              String nameSuffix)
        {
            this.serverId = serverId;
            this.nameSuffix = nameSuffix;
        }

        public static Write to(ValueProvider<Integer> serverId,
                               String nameSuffix)
        {
            return new Write(serverId, nameSuffix);
        }

        @Override
        public PDone expand(PCollection<TableRow> input) {
            ValueProvider<String> tableName =
                NestedValueProvider.of(serverId, (Integer serverId) -> String.format(
                    "user_stats_%d.ds_daily_user_stats%s",
                    serverId,
                    nameSuffix.equals("") ? "" : "_" + nameSuffix
                ));

            input
                .apply("BQWrite", BigQueryIO
                    .writeTableRows()
                    .to(tableName)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

            return PDone.in(input.getPipeline());
        }
    }

}
