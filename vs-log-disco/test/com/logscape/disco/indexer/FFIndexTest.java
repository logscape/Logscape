package com.logscape.disco.indexer;

import com.liquidlabs.common.file.raf.RAF;
import com.liquidlabs.common.file.raf.RafFactory;
import com.logscape.disco.grokit.GrokItPool;
import com.logscape.disco.indexer.flatfile.FFIndexFeed;
import com.logscape.disco.kv.RulesKeyValueExtractor;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.matchers.StringContains.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 01/04/15
 * Time: 17:51
 * To change this template use File | Settings | File Templates.
 */
public class FFIndexTest {


    @Test
    public void shouldStoreIt() throws IOException {
        FFIndexFeed ffIndexFeed = new FFIndexFeed("build",new RulesKeyValueExtractor(), new GrokItPool());
        List<Pair> indexed = new ArrayList<Pair>();
        String items = "'city':'London','lat':'51.514206','lat':'-0.09309387'";
        indexed.add(new Pair("city", items));
        ffIndexFeed.store(1, 1, indexed);
        ffIndexFeed.store(1, 2, indexed);
        ffIndexFeed.close();

        Map<String, String> values = ffIndexFeed.getAsMapSingle(1, 1, 1);
        Assert.assertEquals(9, values.size());

    }


    @Test
    public void scrollProperly() throws IOException {
        FFIndexFeed ffIndexFeed = new FFIndexFeed(".", null, null);

        String filename = "./build/data/FF-Test.idx";
        new File(filename).getParentFile().mkdirs();
        FileOutputStream os = new FileOutputStream(filename);
        for (int i = 0; i < 20; i++) {
            os.write(("_line!" + i + "!stuff\n").getBytes());
        }
        os.close();

        RAF raf = RafFactory.getRafSingleLine(filename);
        int line = 10;
        String foundline = ffIndexFeed.scrollToLine(raf, 0, 10);
        Assert.assertThat(foundline, containsString("_line!" + 10 + "!") );


    }
}
