package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceParserHelper;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;


/**
 * The builder of the aggregatorFactory. Also implements the parsing of the request.
 */
public class DateHierarchyAggregationBuilder extends ValuesSourceAggregationBuilder<ValuesSource.Numeric, DateHierarchyAggregationBuilder>
        implements MultiBucketAggregationBuilder {
    public static final String NAME = "date_hierarchy";


    public static final ParseField INTERVAL_FIELD = new ParseField("interval");
    public static final ParseField ORDER_FIELD = new ParseField("order");
    public static final ParseField SIZE_FIELD = new ParseField("size");
    public static final ParseField SHARD_SIZE_FIELD = new ParseField("shard_size");
    public static final ParseField MIN_DOC_COUNT_FIELD = new ParseField("min_doc_count");


    public static final Map<String, IntervalConfig> INTERVAL_CONFIG;
    static {
        Map<String, IntervalConfig> dateFieldUnits = new LinkedHashMap<>();
        dateFieldUnits.put("years", new IntervalConfig(Rounding.DateTimeUnit.YEAR_OF_CENTURY, "yyyy"));
        dateFieldUnits.put("months", new IntervalConfig(Rounding.DateTimeUnit.MONTH_OF_YEAR, "MM"));
        dateFieldUnits.put("days", new IntervalConfig(Rounding.DateTimeUnit.DAY_OF_MONTH, "dd"));
        dateFieldUnits.put("hours", new IntervalConfig(Rounding.DateTimeUnit.HOUR_OF_DAY, "hh"));
        dateFieldUnits.put("minutes", new IntervalConfig(Rounding.DateTimeUnit.MINUTES_OF_HOUR, "mm"));
        dateFieldUnits.put("seconds", new IntervalConfig(Rounding.DateTimeUnit.SECOND_OF_MINUTE, "ss"));
        INTERVAL_CONFIG = unmodifiableMap(dateFieldUnits);
    }

    public static class IntervalConfig {
        final Rounding.DateTimeUnit dateTimeUnit;
        final String format;

        public IntervalConfig(Rounding.DateTimeUnit dateTimeUnit, String format) {
            this.dateTimeUnit = dateTimeUnit;
            this.format = format;
        }
    }

    public List<RoundingInfo> buildRoundings() {
        List<RoundingInfo> roundings = new ArrayList<>();

        ZoneId timeZone = timeZone() == null ? ZoneOffset.UTC: timeZone();

        for (String interval: INTERVAL_CONFIG.keySet()) {
            roundings.add(new RoundingInfo(interval, createRounding(INTERVAL_CONFIG.get(interval).dateTimeUnit),
                    new DocValueFormat.DateTime(DateFormatter.forPattern(INTERVAL_CONFIG.get(interval).format), timeZone,
                            DateFieldMapper.Resolution.MILLISECONDS)));
            if (interval.equals(interval())) {
                break;
            }
        }

        return roundings;
    }

    public static class RoundingInfo implements Writeable {
        final DocValueFormat format;
        final Rounding rounding;
        final String interval;

        public RoundingInfo(String interval, Rounding rounding, DocValueFormat docValueFormat) {
            this.interval = interval;
            this.rounding =  rounding;
            this.format = docValueFormat;
        }

        public RoundingInfo(StreamInput in) throws IOException {
            rounding = Rounding.read(in);
            interval = in.readString();
            format = in.readNamedWriteable(DocValueFormat.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            rounding.writeTo(out);
            out.writeString(interval);
            out.writeNamedWriteable(format);
        }
    }

    public static final DateHierarchyAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new
            DateHierarchyAggregator.BucketCountThresholds(10, -1);
    private static final ObjectParser<DateHierarchyAggregationBuilder, Void> PARSER;
    static {

        PARSER = new ObjectParser<>(DateHierarchyAggregationBuilder.NAME);
        ValuesSourceParserHelper.declareNumericFields(PARSER, true, true, true);

        PARSER.declareString(DateHierarchyAggregationBuilder::interval, INTERVAL_FIELD);

        PARSER.declareField(DateHierarchyAggregationBuilder::timeZone, p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return ZoneId.of(p.text());
            } else {
                return ZoneOffset.ofHours(p.intValue());
            }
        }, new ParseField("time_zone"), ObjectParser.ValueType.LONG);

        PARSER.declareInt(DateHierarchyAggregationBuilder::size, SIZE_FIELD);
        PARSER.declareLong(DateHierarchyAggregationBuilder::minDocCount, MIN_DOC_COUNT_FIELD);
        PARSER.declareInt(DateHierarchyAggregationBuilder::shardSize, SHARD_SIZE_FIELD);
        PARSER.declareObjectArray(DateHierarchyAggregationBuilder::order, (p, c) -> InternalOrder.Parser.parseOrderParam(p),
                ORDER_FIELD);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new DateHierarchyAggregationBuilder(aggregationName, null), null);
    }

    private long minDocCount = 0;
    private ZoneId timeZone = null;
    private String interval = "years";
    private BucketOrder order = BucketOrder.compound(BucketOrder.count(false)); // automatically adds tie-breaker key asc order
    private DateHierarchyAggregator.BucketCountThresholds bucketCountThresholds = new DateHierarchyAggregator.BucketCountThresholds(
            DEFAULT_BUCKET_COUNT_THRESHOLDS);


    private DateHierarchyAggregationBuilder(String name, ValueType valueType) {
        super(name, ValuesSourceType.ANY, valueType);
    }

    @Override
    protected boolean serializeTargetValueType() {
        return true;
    }

    /**
     * Read from a stream
     *
     */
    public DateHierarchyAggregationBuilder(StreamInput in) throws IOException {
        super(in, ValuesSourceType.ANY);
        bucketCountThresholds = new DateHierarchyAggregator.BucketCountThresholds(in);
        minDocCount = in.readVLong();
        interval = in.readString();
        order = InternalOrder.Streams.readOrder(in);
        if (in.readBoolean()) {
            timeZone = DateTimeZone.forID(in.readString());
        }
    }

    private DateHierarchyAggregationBuilder(DateHierarchyAggregationBuilder clone, Builder factoriesBuilder,
                                            Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        order = clone.order;
        minDocCount = clone.minDocCount;
        this.bucketCountThresholds = new DateHierarchyAggregator.BucketCountThresholds(clone.bucketCountThresholds);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metaData) {
        return new DateHierarchyAggregationBuilder(this, factoriesBuilder, metaData);
    }

    /**
     * Write to a stream
     */
    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        bucketCountThresholds.writeTo(out);
        out.writeVLong(minDocCount);
        out.writeString(interval);
        order.writeTo(out);
        boolean hasTimeZone = timeZone != null;
        out.writeBoolean(hasTimeZone);
        if (hasTimeZone) {
            out.writeString(timeZone.getID());
        }
    }

    /**
     * Returns the date interval that is set on this source
     **/
    public String interval() {
        return interval;
    }

    public DateHierarchyAggregationBuilder interval(String interval) {

        if (INTERVAL_CONFIG.get(interval) == null) {
            throw new IllegalArgumentException("[interval] is invalid");
        }

        this.interval = interval;
        return this;
    }

    /**
     * Sets the time zone to use for this aggregation
     */
    public DateHierarchyAggregationBuilder timeZone(ZoneId timeZone) {
        if (timeZone == null) {
            throw new IllegalArgumentException("[timeZone] must not be null: [" + name + "]");
        }
        this.timeZone = timeZone;
        return this;
    }

    /**
     * Gets the time zone to use for this aggregation
     */
    public ZoneId timeZone() {
        return timeZone;
    }

    private Rounding createRounding(Rounding.DateTimeUnit dateTimeUnit) {
        Rounding.Builder tzRoundingBuilder;
        tzRoundingBuilder = Rounding.builder(dateTimeUnit);

        if (timeZone() != null) {
            tzRoundingBuilder.timeZone(timeZone());
        }
        Rounding rounding = tzRoundingBuilder.build();
        return rounding;
    }

    /** Set the order in which the buckets will be returned. It returns the builder so that calls
     *  can be chained. A tie-breaker may be added to avoid non-deterministic ordering. */
    private DateHierarchyAggregationBuilder order(BucketOrder order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        if(order instanceof InternalOrder.CompoundOrder || InternalOrder.isKeyOrder(order)) {
            this.order = order; // if order already contains a tie-breaker we are good to go
        } else { // otherwise add a tie-breaker by using a compound order
            this.order = BucketOrder.compound(order);
        }
        return this;
    }

    private DateHierarchyAggregationBuilder order(List<BucketOrder> orders) {
        if (orders == null) {
            throw new IllegalArgumentException("[orders] must not be null: [" + name + "]");
        }
        // if the list only contains one order use that to avoid inconsistent xcontent
        order(orders.size() > 1 ? BucketOrder.compound(orders) : orders.get(0));
        return this;
    }


    /**
     * Sets the size - indicating how many term buckets should be returned
     * (defaults to 10)
     */
    public DateHierarchyAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /** Set the minimum count of matching documents that buckets need to have
     *  and return this builder so that calls can be chained. */
    public DateHierarchyAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                    "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]");
        }
        this.minDocCount = minDocCount;
        return this;
    }

    /**
     * Returns the number of term buckets currently configured
     */
    public int size() {
        return bucketCountThresholds.getRequiredSize();
    }


    /**
     * Sets the shard_size - indicating the number of term buckets each shard
     * will return to the coordinating node (the node that coordinates the
     * search execution). The higher the shard size is, the more accurate the
     * results are.
     */
    public DateHierarchyAggregationBuilder shardSize(int shardSize) {
        if (shardSize <= 0) {
            throw new IllegalArgumentException(
                    "[shardSize] must be greater than 0. Found [" + shardSize + "] in [" + name + "]");
        }
        bucketCountThresholds.setShardSize(shardSize);
        return this;
    }

    /**
     * Returns the number of term buckets per shard that are currently configured
     */
    public int shardSize() {
        return bucketCountThresholds.getShardSize();
    }

    @Override
    protected ValuesSourceAggregatorFactory<ValuesSource.Numeric, ?> innerBuild(
            SearchContext context,
            ValuesSourceConfig<ValuesSource.Numeric> config,
            AggregatorFactory<?> parent,
            Builder subFactoriesBuilder) throws IOException {

        final List<RoundingInfo> roundingsInfo = buildRoundings();

        return new DateHierarchyAggregatorFactory(
                name, config, order, roundingsInfo, minDocCount, bucketCountThresholds,
                context, parent, subFactoriesBuilder, metaData);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (order != null) {
            builder.field(ORDER_FIELD.getPreferredName());
            order.toXContent(builder, params);
        }

        builder.field(MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);

        return builder.endObject();
    }

    /**
     * Used for caching requests, amongst other things.
     */
    @Override
    protected int innerHashCode() {
        return Objects.hash(interval, order, minDocCount, bucketCountThresholds, timeZone);
    }

    @Override
    protected boolean innerEquals(Object obj) {
        DateHierarchyAggregationBuilder other = (DateHierarchyAggregationBuilder) obj;
        return Objects.equals(interval, other.interval)
                && Objects.equals(order, other.order)
                && Objects.equals(minDocCount, other.minDocCount)
                && Objects.equals(bucketCountThresholds, other.bucketCountThresholds)
                && Objects.equals(timeZone, other.timeZone);
    }

    @Override
    public String getType() {
        return NAME;
    }
}

