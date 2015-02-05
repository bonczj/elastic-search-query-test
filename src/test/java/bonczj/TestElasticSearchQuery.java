
package bonczj;

import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Before;

/**
 * Set up a embedded ElasticSearch environment by creating a new Node. Run tests to validate that a single match test works, multiple
 * matches joined with boolean tests fail and the same query accessed through REST works.
 *
 */
public class TestElasticSearchQuery extends BaseElasticSearch
{
    @Before
    public void setUp() throws Exception
    {
        Node node = NodeBuilder.nodeBuilder().data(true).local(false).node();
        setClient(node.client());

        super.setUp();
    }
}
