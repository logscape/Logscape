package com.liquidlabs.replicator;

import com.liquidlabs.replicator.data.Meta;
import com.liquidlabs.replicator.data.MetaInfo;
import com.liquidlabs.replicator.data.Upload;
import com.liquidlabs.replicator.service.ReplicationService;
import com.liquidlabs.space.lease.LeaseRenewer;
import com.liquidlabs.space.lease.Renewer;
import junit.framework.TestCase;
import org.jmock.Expectations;
import org.jmock.Mockery;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class UploadManagerTest extends TestCase {
	
	Mockery context = new Mockery();

	private File file;
	private ReplicationService replicator;
	private LeaseRenewer leaseRenewer;
	private UploadManager uploadManager;

	protected void setUp() throws Exception {
		
		file = File.createTempFile("abcd", "123");
		file.deleteOnExit();
		writeBytes(1024);
		replicator = context.mock(ReplicationService.class);
		leaseRenewer = context.mock(LeaseRenewer.class);
		uploadManager = new UploadManager(replicator,leaseRenewer, 3000);
		context.checking(new Expectations(){private boolean forceUpdate;

		{
			atLeast(0).of(leaseRenewer).add(with(any(Renewer.class)), with(any(Integer.class)), with(any(String.class)));
			atLeast(0).of(replicator).fileUploaded(with(any(Upload.class)), with(any(String.class)), with(forceUpdate));
		}});
		
	}
    public void testShouldNotPublishDOTANDBAK() throws Exception {
        assertTrue(uploadManager.isValid("hello.log"));
        assertFalse(uploadManager.isValid("hello.bak"));
        assertFalse(uploadManager.isValid(".hello.log"));

    }
	
	public void testShouldPublishAvailabilityOfFile() throws Exception {
		MetaInfo metaInfo = new MetaInfo(file, 128);
		context.checking(new Expectations(){{
			one(replicator).publishAvailability(with(any(Meta.class)), with(any(Integer.class))); will(returnValue("lease"));
			
		}});
		uploadManager.publish(metaInfo);
	}

	public void testShouldDoAddToLeaseRenewer() throws Exception {
		MetaInfo metaInfo = new MetaInfo(file, 128);
		
		context.checking(new Expectations(){{
			one(replicator).publishAvailability(with(any(Meta.class)), with(any(Integer.class))); will(returnValue("lease"));
			atLeast(0).of(leaseRenewer).add(with(any(Renewer.class)), with(any(Integer.class)), with(any(String.class)));
		}});
		
//		leaseRenewer.expects(once()).method("addLease").with(eq("lease"), eq(new Integer(60)), eq(new Integer(30)));
		uploadManager.publish(metaInfo);
	}
	

	private void writeBytes(int kbytes) throws FileNotFoundException,
			IOException {
		FileOutputStream stream = new FileOutputStream(file);
		byte[] buf = new byte[kbytes * 1024];
		Arrays.fill(buf, (byte) 1);
		stream.write(buf);
		stream.close();
	}

}
