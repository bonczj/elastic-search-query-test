
package bonczj;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
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
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Set up a embedded ElasticSearch environment by creating a new Node. Run tests to validate that a single match test works, multiple
 * matches joined with boolean tests fail and the same query accessed through REST works.
 *
 */
public class TestElasticSearchQuery
{
    private static final String INDEX_NAME = "my-index";
    private static final String QUERY_TYPE = "my-test";
    private static final String FIELD_NAME = "name";
    private static final long   ES_TIMEOUT = 5000;

    private Client              client;
    private Node                node;
    private List<String>        testValues;

    @Before
    public void setUp() throws Exception
    {
        setNode(NodeBuilder.nodeBuilder().data(true).local(false).node());
        setClient(getNode().client());
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

    @After
    public void tearDown() throws Exception
    {
        if (null != getNode())
        {
            getNode().client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME));
            getNode().stop();
        }
    }

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
    
    protected SearchResponse search(QueryBuilder query)
    {
        return getClient().prepareSearch(INDEX_NAME).setTypes(QUERY_TYPE).setQuery(query).get(new TimeValue(ES_TIMEOUT));
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

        builder.field(FIELD_NAME, id.toString());

        return builder.endObject();
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

    protected Client getClient()
    {
        return client;
    }

    protected void setClient(Client client)
    {
        this.client = client;
    }

    protected Node getNode()
    {
        return node;
    }

    protected void setNode(Node node)
    {
        this.node = node;
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
