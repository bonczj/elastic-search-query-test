package bonczj;

import org.elasticsearch.client.Client;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Test if there is a difference by loading the node through Spring.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/test-context.xml")
public class TestSpringElasticSearchQuery extends BaseElasticSearch
{
    @Autowired
    private Client springClient;

    @Before
    public void setUp() throws Exception
    {
        setClient(this.springClient);
        
        super.setUp();
    }

}
