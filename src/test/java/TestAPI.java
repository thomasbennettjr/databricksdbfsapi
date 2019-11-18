import com.talend.databricks.dbfs.DBFS;
import com.talend.databricks.dbfs.DBFSException;
import org.junit.Test;

import java.io.*;


public class TestAPI {
    private  DBFS dbfs = null;
    @Test
    public void testAddBlock() {
        try {
            dbfs = DBFS.getInstance("eastus", System.getenv("DB_TOKEN"));
            int handle = dbfs.create("/test123/dbricks/MOCK_DATA.csv", true);
            this.processFile(handle);
            dbfs.close(handle);
            String line = null;
        } catch(DBFSException e)
        {
            e.printStackTrace();
        }
    }

    private void processFile(int handle) {
        // Read the text input stream one line at a time and display each line.
        try {
            BufferedReader reader = new BufferedReader(new FileReader(new File("/Users/tbennett/Downloads/MOCK_DATA.csv")));
            String line = null;
            int chunkSize = 1048576; // 1MB but coding below will not break multibyte characters
            StringBuilder buff = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                buff.append(line + "\n");
            }

            dbfs.addBlock(handle, buff.toString().getBytes("UTF-8"));
        } catch(DBFSException | IOException e)
        {
            e.printStackTrace();
        }
    }
}
