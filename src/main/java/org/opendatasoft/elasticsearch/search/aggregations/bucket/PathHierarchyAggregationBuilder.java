package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorFactories.Builder;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * The builder of the aggregatorFactory. Also implements the parsing of the request.
 */
public class PathHierarchyAggregationBuilder extends ValuesSourceAggregationBuilder<PathHierarchyAggregationBuilder> {
    public static final String NAME = "path_hierarchy";
    public static final ValuesSourceRegistry.RegistryKey<PathHierarchyAggregationSupplier> REGISTRY_KEY =
            new ValuesSourceRegistry.RegistryKey<>(NAME, PathHierarchyAggregationSupplier.class);

    public static final ParseField SEPARATOR_FIELD = new ParseField("separator");
    public static final ParseField MIN_DEPTH_FIELD = new ParseField("min_depth");
    public static final ParseField MAX_DEPTH_FIELD = new ParseField("max_depth");
    public static final ParseField KEEP_BLANK_PATH = new ParseField("keep_blank_path");
    public static final ParseField DEPTH_FIELD = new ParseField("depth");
    public static final ParseField ORDER_FIELD = new ParseField("order");
    public static final ParseField SIZE_FIELD = new ParseField("size");
    public static final ParseField SHARD_SIZE_FIELD = new ParseField("shard_size");
    public static final ParseField MIN_DOC_COUNT_FIELD = new ParseField("min_doc_count");

    public static final PathHierarchyAggregator.BucketCountThresholds DEFAULT_BUCKET_COUNT_THRESHOLDS = new
            PathHierarchyAggregator.BucketCountThresholds(10, -1);
    public static final ObjectParser<PathHierarchyAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(PathHierarchyAggregationBuilder.NAME);
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);

        PARSER.declareString(PathHierarchyAggregationBuilder::separator, SEPARATOR_FIELD);
        PARSER.declareInt(PathHierarchyAggregationBuilder::minDepth, MIN_DEPTH_FIELD);
        PARSER.declareInt(PathHierarchyAggregationBuilder::maxDepth, MAX_DEPTH_FIELD);
        PARSER.declareBoolean(PathHierarchyAggregationBuilder::keepBlankPath, KEEP_BLANK_PATH);
        PARSER.declareInt(PathHierarchyAggregationBuilder::depth, DEPTH_FIELD);
        PARSER.declareInt(PathHierarchyAggregationBuilder::size, SIZE_FIELD);
        PARSER.declareLong(PathHierarchyAggregationBuilder::minDocCount, MIN_DOC_COUNT_FIELD);
        PARSER.declareInt(PathHierarchyAggregationBuilder::shardSize, SHARD_SIZE_FIELD);
        PARSER.declareObjectArray(PathHierarchyAggregationBuilder::order, (p, c) -> InternalOrder.Parser.parseOrderParam(p),
                ORDER_FIELD);
    }

    public static AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new PathHierarchyAggregationBuilder(aggregationName), null);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        PathHierarchyAggregatorFactory.registerAggregators(builder);
    }

    private static final String DEFAULT_SEPARATOR = "/";
    private static final int DEFAULT_MIN_DEPTH = 0;
    private static final int DEFAULT_MAX_DEPTH = 3;
    private static final boolean DEFAULT_KEEP_BLANK_PATH = false;
    private String separator = DEFAULT_SEPARATOR;
    private int minDepth = DEFAULT_MIN_DEPTH;
    private int maxDepth = DEFAULT_MAX_DEPTH;
    private boolean keepBlankPath = DEFAULT_KEEP_BLANK_PATH;
    private long minDocCount = 0;
    private int depth = -1;
    private BucketOrder order = BucketOrder.compound(BucketOrder.count(false)); // automatically adds tie-breaker key asc order
    private PathHierarchyAggregator.BucketCountThresholds bucketCountThresholds = new PathHierarchyAggregator.BucketCountThresholds(
            DEFAULT_BUCKET_COUNT_THRESHOLDS);


    private PathHierarchyAggregationBuilder(String name) {
        super(name);
    }

    @Override
    protected boolean serializeTargetValueType(Version version) {
        return true;
    }

    /**
     * Read from a stream
     */
    public PathHierarchyAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        bucketCountThresholds = new PathHierarchyAggregator.BucketCountThresholds(in);
        separator = in.readString();
        minDocCount = in.readVLong();
        minDepth = in.readOptionalVInt();
        maxDepth = in.readOptionalVInt();
        keepBlankPath = in.readOptionalBoolean();
        depth = in.readOptionalVInt();
        order = InternalOrder.Streams.readOrder(in);
    }

    private PathHierarchyAggregationBuilder(PathHierarchyAggregationBuilder clone, Builder factoriesBuilder,
                                           Map<String, Object> metaData) {
        super(clone, factoriesBuilder, metaData);
        separator = clone.separator;
        minDepth = clone.minDepth;
        maxDepth = clone.maxDepth;
        keepBlankPath = clone.keepBlankPath;
        depth = clone.depth;
        order = clone.order;
        minDocCount = clone.minDocCount;
        this.bucketCountThresholds = new PathHierarchyAggregator.BucketCountThresholds(clone.bucketCountThresholds);
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metaData) {
        return new PathHierarchyAggregationBuilder(this, factoriesBuilder, metaData);
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    /**
     * Write to a stream
     */
    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        bucketCountThresholds.writeTo(out);
        out.writeString(separator);
        out.writeVLong(minDocCount);
        out.writeOptionalVInt(minDepth);
        out.writeOptionalVInt(maxDepth);
        out.writeOptionalBoolean(keepBlankPath);
        out.writeOptionalVInt(depth);
        order.writeTo(out);
    }

    private PathHierarchyAggregationBuilder separator(String separator) {
        this.separator = separator;
        return this;
    }

    private PathHierarchyAggregationBuilder minDepth(int minDepth) {
        this.minDepth = minDepth;
        return this;
    }

    private PathHierarchyAggregationBuilder maxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return this;
    }

    private PathHierarchyAggregationBuilder keepBlankPath(boolean keepBlankPath) {
        this.keepBlankPath = keepBlankPath;
        return this;
    }

    private PathHierarchyAggregationBuilder depth(int depth) {
        this.depth = depth;
        return this;
    }

    /** Set the order in which the buckets will be returned. It returns the builder so that calls
     *  can be chained. A tie-breaker may be added to avoid non-deterministic ordering. */
    private PathHierarchyAggregationBuilder order(BucketOrder order) {
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

    private PathHierarchyAggregationBuilder order(List<BucketOrder> orders) {
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
    public PathHierarchyAggregationBuilder size(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("[size] must be greater than 0. Found [" + size + "] in [" + name + "]");
        }
        bucketCountThresholds.setRequiredSize(size);
        return this;
    }

    /** Set the minimum count of matching documents that buckets need to have
     *  and return this builder so that calls can be chained. */
    public PathHierarchyAggregationBuilder minDocCount(long minDocCount) {
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

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.MANY;
    }

    /**
     * Sets the shard_size - indicating the number of term buckets each shard
     * will return to the coordinating node (the node that coordinates the
     * search execution). The higher the shard size is, the more accurate the
     * results are.
     */
    public PathHierarchyAggregationBuilder shardSize(int shardSize) {
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
    protected ValuesSourceAggregatorFactory innerBuild(QueryShardContext context,
                                                       ValuesSourceConfig config,
                                                       AggregatorFactory parent,
                                                       AggregatorFactories.Builder subFactoriesBuilder) throws IOException {


        if (minDepth > maxDepth)
            throw new IllegalArgumentException("[minDepth] (" + minDepth + ") must not be greater than [maxDepth] (" +
                    maxDepth + ")");

        if (depth >= 0) {
            if (minDepth > depth)
                throw new IllegalArgumentException("[minDepth] (" + minDepth + ") must not be greater than [depth] (" +
                        depth + ")");
            minDepth = depth;
            maxDepth = depth;
        }

        return new PathHierarchyAggregatorFactory(
                name,
                config,
                separator,
                minDepth,
                maxDepth,
                keepBlankPath,
                order,
                minDocCount,
                bucketCountThresholds,
                context,
                parent,
                subFactoriesBuilder,
                metadata);
    }

    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        if (order != null) {
            builder.field(ORDER_FIELD.getPreferredName());
            order.toXContent(builder, params);
        }

        builder.field(MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);

        if (!separator.equals(DEFAULT_SEPARATOR)) {
            builder.field(SEPARATOR_FIELD.getPreferredName(), separator);
        }

        if (minDepth != DEFAULT_MIN_DEPTH) {
            builder.field(MIN_DEPTH_FIELD.getPreferredName(), minDepth);
        }

        if (maxDepth != DEFAULT_MAX_DEPTH) {
            builder.field(MAX_DEPTH_FIELD.getPreferredName(), maxDepth);
        }

        if (depth != 0) {
            builder.field(DEPTH_FIELD.getPreferredName(), depth);
        }

        return builder.endObject();
    }

    /**
     * Used for caching requests, amongst other things.
     */
    @Override
    public int hashCode() {
        return Objects.hash(separator, minDepth, maxDepth, depth, order, minDocCount, bucketCountThresholds);
    }

    @Override
    public boolean equals(Object obj) {
        PathHierarchyAggregationBuilder other = (PathHierarchyAggregationBuilder) obj;
        return Objects.equals(separator, other.separator)
                && Objects.equals(minDepth, other.minDepth)
                && Objects.equals(maxDepth, other.maxDepth)
                && Objects.equals(depth, other.depth)
                && Objects.equals(order, other.order)
                && Objects.equals(minDocCount, other.minDocCount)
                && Objects.equals(bucketCountThresholds, other.bucketCountThresholds);
    }

    @Override
    public String getType() {
        return NAME;
    }

    @Override
    protected ValuesSourceRegistry.RegistryKey<?> getRegistryKey() { return REGISTRY_KEY; }
}

