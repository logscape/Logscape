package com.liquidlabs.space.impl.prevalent;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;
import org.prevayler.Clock;
import org.prevayler.Prevayler;
import org.prevayler.PrevaylerFactory;
import org.prevayler.Query;
import org.prevayler.SureTransactionWithQuery;
import org.prevayler.Transaction;
import org.prevayler.TransactionWithQuery;

import com.liquidlabs.common.concurrent.NamingThreadFactory;

public class PrevaylerProxy implements Prevayler {

    private Prevayler prevayler;
    private final PrevaylerFactory factory;
    private ExecutorService runner;

    private static final Logger LOGGER = Logger.getLogger(PrevaylerProxy.class);
    private int MAX_RETRIES = 10;


    public PrevaylerProxy(Prevayler prevayler, PrevaylerFactory factory, ExecutorService runner) {
        this.prevayler = prevayler;
        this.factory = factory;
        this.runner = runner;
    }

    interface DoesIt {
        Object doesIt() throws Throwable;
    }

    private synchronized Object doIt(DoesIt doesIt) {
        try {
            return doesIt.doesIt();
        } catch (Throwable t) {
            LOGGER.error("Exception Caught whilst executing prevalyer transaction. Shutting down and restarting", t);
            try {
                prevayler.close();
            } catch (IOException e) {
            }
            prevayler = null;
            int retryCount = 0;
            while (prevayler == null && retryCount++ < MAX_RETRIES) {
                try {
                    prevayler = factory.create();
                } catch (Throwable error) {
                    LOGGER.warn("Failed to re-start prevayler attempt no " + retryCount, error);
                    sleep();
                }
            }
            if (prevayler == null) {
                LOGGER.warn("Unable to restart prevayler system");
                throw new RuntimeException("Prevalyer is toast. System needs to be bounced");
            }
            return doIt(doesIt);
        }
    }

    private void sleep()  {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException io) {
        }
    }

    @Override
    public Object prevalentSystem() {
        return doIt(new DoesIt() {
            @Override
            public Object doesIt() {
                return prevayler.prevalentSystem();
            }
        });
    }

    @Override
    public Clock clock() {
        return (Clock) doIt(new DoesIt() {
            @Override
            public Object doesIt() {
                return prevayler.clock();
            }
        });
    }

    public void execute(final Transaction transaction) {
    	// decouple the txn because it will hit sch lock
    	runner.execute(new Runnable() {
			public void run() {
				doTxn(transaction);
			}
    	});
    }

	private void doTxn(final Transaction transaction) {
		doIt(new DoesIt() {
		       @Override
		       public Object doesIt() {
		           prevayler.execute(transaction);
		           return null;
		       }
		   });
	}

    @Override
    public Object execute(final Query query) throws Exception {
        return doIt(new DoesIt() {
            @Override
            public Object doesIt() throws Exception {
                return prevayler.execute(query);
            }
        });
    }

    @Override
    public Object execute(final TransactionWithQuery transactionWithQuery) throws Exception {
        return doIt(new DoesIt() {
            @Override
            public Object doesIt() throws Exception {
                return prevayler.execute(transactionWithQuery);
            }
        });
    }

    @Override
    public Object execute(final SureTransactionWithQuery sureTransactionWithQuery) {
        return doIt(new DoesIt() {
            @Override
            public Object doesIt() throws Exception {
                return prevayler.execute(sureTransactionWithQuery);
            }
        });
    }

    @Override
    public void takeSnapshot() throws IOException {
        doIt(new DoesIt() {
            @Override
            public Object doesIt() throws Exception {
                prevayler.takeSnapshot();
                return null;
            }
        });
    }

    @Override
    public void close() throws IOException {
        doIt(new DoesIt() {
            @Override
            public Object doesIt() throws Exception {
                prevayler.close();
                return null;
            }
        });
    }
}
