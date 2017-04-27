package com.liquidlabs.log.space.agg;

import com.liquidlabs.common.PIDGetter;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.concurrent.ExecutorService;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.search.ReplayEvent;
import com.liquidlabs.log.space.LogReplayHandler;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.transport.serialization.Convertor;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.omg.SendingContext.RunTime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ChronicleQReplayAggregator implements ReplayAggregator {

    final int CHUNK_SIZE_BYTES = Integer.getInteger("chronicle.q.chunk.kb", 32) * 1024;

    static String logscapeQDir = System.getProperty("java.io.tmpdir") + File.separator + "LogscapeQ";

    static {
        // clean out the tmp/LogscapeQ on startup
        try {
            FileUtil.deleteDir(new File(logscapeQDir));
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    private final static Logger LOGGER = Logger.getLogger(ChronicleQReplayAggregator.class);

	private final LogRequest request;
    private String uuid;
    private String basePath;
    private Chronicle queue = null;
    private ConcurrentLinkedQueue<ReplayEvent> writeQueue = new ConcurrentLinkedQueue<>();
    private LogReplayHandler replayHandler;

	private long startTime = new DateTime().getMillis();

	private AtomicBoolean cancelled = new AtomicBoolean(false);
    private volatile int sent = 0;

    ScheduledExecutorService scheduler = ExecutorService.newScheduledThreadPool("services");
    private AtomicInteger size = new AtomicInteger();


    private volatile int written;
	public String listenerId;
    private ScheduledFuture<?> future;
    private int pausePeriod = Integer.getInteger("q.pause",100);
    private volatile AtomicInteger writing = new AtomicInteger();
    private int MAX_WRITTEN = Integer.getInteger("replay.max.queue", 1 * 100 * 1000);

    volatile static int created = 0;

    public ChronicleQReplayAggregator(LogRequest request, LogReplayHandler replayHandler, String handlerId) {
		this.request = request;
		this.replayHandler = replayHandler;
		this.listenerId = request.subscriber() + "_id";

        try {
            String pid =PIDGetter.getPID();
            uuid = created++ +"_" + "pid" + pid.substring(0, pid.indexOf("@")) + "_" +  UID.getUUID().replace(":","").replace("-","_");

            LOGGER.info("Created QDir:" + uuid + " Search:" + request.subscriber());
            basePath = logscapeQDir + File.separator + uuid;
            queue = ChronicleQueueBuilder.vanilla(basePath).cleanupOnClose(true).dataBlockSize(67108864L/4).indexBlockSize(16777216L/2).build();
//            queue = ChronicleQueueBuilder.indexed(basePath).build();
            queue.clear();
            reader = queue.createTailer();

        } catch (IOException e) {
            e.printStackTrace();
        }
        this.future = scheduler.scheduleWithFixedDelay(this, pausePeriod, Integer.getInteger("replay.queue.pump", pausePeriod), TimeUnit.MILLISECONDS);
    }

    volatile int bytes = 0;


    public void handle(ReplayEvent event) {
		if (cancelled.get() || written++ > MAX_WRITTEN) {
            if (!cancelled.get()) cancelled.set(true);
            return ;
        }
        writing.incrementAndGet();
        size.incrementAndGet();

        writeQueue.add(event);
        if (bytes <= CHUNK_SIZE_BYTES) {
            bytes += event.getRawData().length();
        } else {
            flushQueue();
        }
	}

    private void flushQueue() {
        bytes = 0;
        List<ReplayEvent> writting = new ArrayList<>();
        int currentChunkSize = 0;
        while (!writeQueue.isEmpty()) {
            ReplayEvent next = writeQueue.poll();
            if (next != null) {
                currentChunkSize += next.getRawData().length();
                writting.add(next);
                writing.decrementAndGet();
                if (currentChunkSize > CHUNK_SIZE_BYTES) {
                    persist(writting);
                    writting = new ArrayList<>();
                    currentChunkSize = 0;
                }
            }
        }

        if (writting.size() > 0) {
            persist(writting);
        }
    }

    int queuedPersisted = 0;
    int persistedBytes = 0;
    private void persist(List<ReplayEvent> writeQueue) {
        queuedPersisted++;
        try {
            ExcerptAppender appender = queue.createAppender();
            byte[] bytes = Convertor.kryoSerialize(writeQueue);
            appender.startExcerpt(bytes.length + 4);
            appender.writeInt(bytes.length);
            persistedBytes += bytes.length + 4;

            appender.write(bytes);
            appender.finish();
        } catch (IOException e) {
            LOGGER.warn(e);
        }
    }

    ExcerptTailer reader = null;

    public void run(){
		try {
                while (!cancelled.get() && reader.nextIndex()) {
                    int sizes = reader.readInt();
                    byte[] bytess = new byte[sizes];
                    reader.read(bytess);

                    List<ReplayEvent> items = (List<ReplayEvent>) Convertor.deserialize(bytess);
                    reader.finish();
//                System.out.println("RUNNER: " + items.size() + " Size:" + size + " Sent:" + sent + " written:" + written);
                    size.addAndGet(items.size() * -1);
                    sent += items.size();

                    replayHandler.handle(items);

                }
    	} catch (Throwable t){
            LOGGER.warn("LOGGER" + t.getMessage() + " cancelled:" + cancelled + " sent:" + sent + " incoming" + size);
            cancelled.set(true);
			if (t.getMessage().contains("RetryInvocationException: SendFailed.Throwable:noSender")) {
				replayHandler = null;
			}
		} finally {
			if (cancelled.get() || isExpired() || request.isCancelled()) {
				cancel();
			}
		}
	}

	public Integer age(long now) {
		return Long.valueOf((now - startTime)/1000l).intValue();
	}

	public void cancel() {
        synchronized (this) {
            if (this.queue == null) return;

            if (this.future != null) {
                future.cancel(false);
                try {
                    Thread.sleep(100);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            this.cancelled.set(true);
            LOGGER.info("LOGGER Cancelled:" + request.subscriber() + " Listener:" + listenerId + " written: " + written + " sent:" + sent + " persistedBytes:" + persistedBytes + " persistedQueues:" + queuedPersisted);
            // try and coordinate the shutdown
            int waiting = 0;
            while (writing.get() > 0 && waiting++ < Integer.getInteger("replay.agg.wait.count", 100)) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (queue != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

                try {
                    LOGGER.info("Closing Q:" + uuid);
                    queue.close();
                    queue = null;

                    FileUtil.deleteDir(new File(basePath));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                deleteQueueFiles();
            }
        }
	}
    boolean closed = false;
    public void close() {
        if (!closed) {
            closed = true;
            flush();
            cancel();
        }
    }
    private void deleteQueueFiles() {
        try {
            new File(basePath + ".data").delete();
            new File(basePath + ".index").delete();
            FileUtil.deleteDir(new File(basePath));
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    public boolean isExpired() {
		return replayHandler == null ||  this.request.isExpired();
	}

	public boolean isRunnable() {
		boolean itemsQueued = queue.size() > 0;
		return itemsQueued && !request.isCancelled() &&  !request.isExpired() ;
	}

	public int size() {
		return size.get();
	}

    @Override

    public void flush() {
        if (cancelled.get()) return;

        flushQueue();

        try {
            Thread.sleep(200);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public LogReplayHandler getReplayHandler() {
        return replayHandler;
    }
}
