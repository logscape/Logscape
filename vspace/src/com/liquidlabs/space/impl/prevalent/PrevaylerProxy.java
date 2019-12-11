package com.liquidlabs.space.impl.prevalent;

import org.apache.log4j.Logger;
import org.prevayler.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ExecutorService;

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
        return doIt(() -> prevayler.prevalentSystem());
    }

    @Override
    public Clock clock() {
        return (Clock) doIt(() -> prevayler.clock());
    }

    public void execute(final Transaction transaction) {
    	// decouple the txn because it will hit sch lock
    	runner.execute(() -> doTxn(transaction));
    }

	private void doTxn(final Transaction transaction) {
		doIt(() -> {
            prevayler.execute(transaction);
            return null;
        });
	}

    @Override
    public Object execute(final Query query) throws Exception {
        return doIt(() -> prevayler.execute(query));
    }

    @Override
    public Object execute(final TransactionWithQuery transactionWithQuery) throws Exception {
        return doIt(() -> prevayler.execute(transactionWithQuery));
    }

    @Override
    public Object execute(final SureTransactionWithQuery sureTransactionWithQuery) {
        return doIt(() -> prevayler.execute(sureTransactionWithQuery));
    }

    @Override
    public File takeSnapshot() throws IOException {
        doIt(() -> prevayler.takeSnapshot());
        return null;
    }

    @Override
    public void close() throws IOException {
        doIt(() -> {
            prevayler.close();
            return null;
        });
    }
}
