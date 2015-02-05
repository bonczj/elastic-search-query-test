
package bonczj;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Assert;
import org.junit.Test;

/**
 * Common code for the tests.
 */
public abstract class BaseElasticSearch
{
    public static final String INDEX_NAME  = "my-index";
    public static final String QUERY_TYPE  = "my-test";
    public static final String FIELD_NAME  = "name";
    public static final String FIELD_VALUE = "value";
    public static final long   ES_TIMEOUT  = 5000;

    private Client             client;
    private List<String>       testValues;

    @Test
    public void testSingleMatchNativeQuery()
    {
        // Look for the first ID
        String id = getTestValues().get(0);
        QueryBuilder queryBuilder = constructQuery(id);
        SearchResponse response = search(queryBuilder);

        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getHits());
        Assert.assertEquals(1, response.getHits().totalHits());
    }

    @Test
    public void testMultiMatchNativeQuery()
    {
        QueryBuilder queryBuilder = constructQuery(getTestValues().get(0), "no-in-result-set");
        SearchResponse response = search(queryBuilder);

        Assert.assertNotNull(response);
        Assert.assertNotNull(response.getHits());
        Assert.assertEquals(1, response.getHits().totalHits());
    }

    // @Test
    public void testMultiMatchRestQuery()
    {

    }

    public void setUp() throws Exception
    {
        createIndex();
        
        for (int i = 0; i < 20; i++)
        {
            UUID id = UUID.randomUUID();
            XContentBuilder content = createDocument(id);
            UpdateResponse response = getClient().prepareUpdate(INDEX_NAME, QUERY_TYPE, id.toString()).setRefresh(true).setDoc(content)
                    .setDocAsUpsert(true).get(new TimeValue(ES_TIMEOUT));

            if (null == response)
            {
                throw new Exception("Failed to insert content");
            }

            getTestValues().add(id.toString());
        }
    }
    
    public void tearDown() throws Exception
    {
        if (null != getClient())
        {
            ActionFuture<DeleteIndexResponse> responseFuture = getClient().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME));
            DeleteIndexResponse response = responseFuture.get();
            
            if (null == response || !response.isAcknowledged())
            {
                throw new Exception("Failed to delete index");
            }
        }
    }

    protected SearchResponse search(QueryBuilder query)
    {
        return getClient().prepareSearch(INDEX_NAME).setTypes(QUERY_TYPE).setQuery(query).get(new TimeValue(ES_TIMEOUT));
    }

    protected void createIndex() throws Exception
    {
        IndicesExistsRequest existsRequest = new IndicesExistsRequest(INDEX_NAME);
        IndicesExistsResponse existsResponse = getClient().admin().indices().exists(existsRequest).actionGet(ES_TIMEOUT);

        if (null != existsResponse && !existsResponse.isExists())
        {
            CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);
            ActionFuture<CreateIndexResponse> responseFuture = getClient().admin().indices().create(request);
            CreateIndexResponse response = responseFuture.actionGet(ES_TIMEOUT);

            if (!response.isAcknowledged())
            {
                throw new Exception("Failed to create index");
            }
        }
    }

    /**
     * Create an ES document with the id also as a field to search.
     * 
     * @param id
     * @return
     * @throws IOException
     */
    protected XContentBuilder createDocument(UUID id) throws IOException
    {
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint().startObject();
        Random random = new Random();

        builder.field(FIELD_NAME, id.toString()).field(FIELD_VALUE, random.nextInt());

        return builder.endObject();
    }

    /**
     * Construct a queries to find each id as an optional result.
     * 
     * @param ids
     * @return
     */
    protected QueryBuilder constructQuery(String... ids)
    {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        for (String id : ids)
        {
            boolQuery.should(QueryBuilders.matchQuery(FIELD_NAME, id));
        }

        // add an outer bool to simulate possible other attributes being searched that must be there
        BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

        finalQuery.must(boolQuery);

        return finalQuery;
    }

    protected Client getClient()
    {
        return client;
    }

    protected void setClient(Client client)
    {
        this.client = client;
    }

    protected List<String> getTestValues()
    {
        if (null == this.testValues)
        {
            this.testValues = new ArrayList<String>();
        }

        return testValues;
    }

}
