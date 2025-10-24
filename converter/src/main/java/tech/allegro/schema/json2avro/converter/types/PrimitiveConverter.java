package tech.allegro.schema.json2avro.converter.types;

import org.apache.avro.Schema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.function.Function;


public class PrimitiveConverter<T> extends AvroTypeConverterWithStrictJavaTypeCheck<T> {
    public final static AvroTypeConverter BOOLEAN = new PrimitiveConverter<>(Schema.Type.BOOLEAN, Boolean.class, bool -> bool);
    public final static AvroTypeConverter STRING = new PrimitiveConverter<>(Schema.Type.STRING, String.class, string -> string);
    public final static AvroTypeConverter INT = new PrimitiveConverter<>(Schema.Type.INT, Number.class, Number::intValue);
    public final static AvroTypeConverter LONG = new PrimitiveConverter<>(Schema.Type.LONG, Number.class, Number::longValue);
    public final static AvroTypeConverter DOUBLE = new PrimitiveConverter<>(Schema.Type.DOUBLE, Number.class, Number::doubleValue);
    public final static AvroTypeConverter FLOAT = new PrimitiveConverter<>(Schema.Type.FLOAT, Number.class, Number::floatValue);
    public final static AvroTypeConverter BYTES = new PrimitiveConverter<>(Schema.Type.BYTES, String.class, value -> ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8)));

    private final Schema.Type avroType;
    private final Function<T, Object> mapper;

    protected PrimitiveConverter(Schema.Type avroType, Class<T> javaType, Function<T, Object> mapper) {
        super(javaType);
        this.avroType = avroType;
        this.mapper = mapper;
    }

    @Override
    public Object convertValue(Schema.Field field, Schema schema, T value, Deque<String> path, boolean silently) {
        // When in silent mode (used by unions), check if the value's actual type matches the target Avro type
        // This prevents incorrect type selection in unions with multiple numeric types
        if (silently && value instanceof Number) {
            if (!isCompatibleNumericType((Number) value, avroType)) {
                return new Incompatible(avroType.getName());
            }
        }
        return mapper.apply(value);
    }

    /**
     * Checks if a Number value is compatible with the target Avro numeric type.
     * This ensures proper type selection in union types with multiple numeric types.
     * <p>
     * Rules:
     * - INT accepts: Integer (exact match), Byte, Short
     * - LONG accepts: Long (exact match), Integer, Byte, Short (can be promoted)
     * - FLOAT accepts: Float (exact match)
     * - DOUBLE accepts: Double (exact match), Float (can be promoted)
     */
    private boolean isCompatibleNumericType(Number value, Schema.Type targetType) {
        switch (targetType) {
            case INT:

                // INT only accepts integer types, not floating point
                if (value instanceof Integer || value instanceof Byte || value instanceof Short) {
                    return true;
                }

                // Accept Long only if it fits in int range
                if (value instanceof Long) {
                    long longVal = value.longValue();
                    return longVal >= Integer.MIN_VALUE && longVal <= Integer.MAX_VALUE;
                }
                return false;

            case LONG:
                // LONG accepts any integer type (they can all be promoted to long)
                return value instanceof Long || value instanceof Integer ||
                        value instanceof Byte || value instanceof Short;

            case FLOAT:
                // FLOAT prefers exact match but accepts types that can be exactly represented
                if (value instanceof Float) {
                    return true;
                }

                // Accept integer types that can be exactly represented as float
                // Note: Large integers may lose precision when converted to float,
                // But we allow it as it's a valid conversion
                return value instanceof Integer || value instanceof Byte || value instanceof Short;

            case DOUBLE:
                // DOUBLE accepts any numeric type (all can be promoted to double)
                return value instanceof Double || value instanceof Float ||
                        value instanceof Long || value instanceof Integer ||
                        value instanceof Byte || value instanceof Short;

            default:
                return true;
        }
    }

    @Override
    public boolean canManage(Schema schema, Deque<String> path) {
        return schema.getType().equals(avroType);
    }

}
