package com.modrykonik.dash.io;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.reflect.CustomEncoding;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

/**
 * This encoder/decoder writes a org.joda.time.LocalDate object as a long to
 * avro and reads a LocalDate object from long. The long stores the number of
 * milliseconds since January 1, 1970, 00:00:00 GMT represented by the LocalDate
 * object.
 */
public class LocalDateEncoding extends CustomEncoding<LocalDate> {
	{
		schema = Schema.create(Schema.Type.LONG);
		schema.addProp("CustomEncoding", "LocalDateEncoding");
	}

	@Override
	protected final void write(Object datum, Encoder out)
			throws IOException
	{
		long val = ((LocalDate) datum).toDateTimeAtStartOfDay(DateTimeZone.UTC).getMillis();
		out.writeLong(val);
	}

	@Override
	protected final LocalDate read(Object reuse, Decoder in)
		throws IOException
	{
		long val = in.readLong();
		return new LocalDate(val, DateTimeZone.UTC);
	}

}
