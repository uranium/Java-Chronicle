package net.openhft.chronicle;

import net.openhft.chronicle.tcp.InProcessChronicleSink;
import net.openhft.chronicle.tcp.InProcessChronicleSource;
import net.openhft.chronicle.tools.ChronicleTools;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Vladimir Mitrovic (vladimir.x.mitrovic@gmail.com) (uranium)
 *         7:27 PM 11/14/13
 *
 */
public class SourceSinkUTF8Test {

    private static final String TMP = System.getProperty("java.io.tmpdir");
    private final int port = 10101;
    private final String sourceChroniclePath = TMP + "/source.chronicle.test";
    private final String sinkChroniclePath = TMP + "/sink.chronicle.test";
    private final int _200K = 204800;
    private final String host = "localhost";


    @Test
    public void writeReadUT8TestCase1() throws IOException {

        ChronicleTools.deleteOnExit(sourceChroniclePath);
        ChronicleTools.deleteOnExit(sinkChroniclePath);

        final Chronicle sourceChronicle = new IndexedChronicle(sourceChroniclePath);
        final InProcessChronicleSource source = new InProcessChronicleSource(sourceChronicle, port);
        final ExcerptAppender appender = source.createAppender();
        final Chronicle sinkChronicle = new IndexedChronicle(sinkChroniclePath);
        final InProcessChronicleSink sink = new InProcessChronicleSink(sinkChronicle, host, port);
        final ExcerptTailer tailer = sink.createTailer();

        final ExecutorService producer = Executors.newSingleThreadExecutor();
        final ExecutorService consumer = Executors.newSingleThreadExecutor();

        final AtomicBoolean testPassed = new AtomicBoolean(false);

        producer.execute(new Runnable() {

            @Override
            public void run() {

                appender.startExcerpt(_200K);
                appender.writeUTFΔ(perlX("a", 65632));
                appender.finish();
            }
        });

        producer.shutdown();

        consumer.execute(new Runnable() {
            @Override
            public void run() {

                while (true) {

                    while (!tailer.nextIndex()) {

                        try {

                            Thread.sleep(10);

                        } catch (InterruptedException e) {

                            Thread.interrupted();
                        }
                    }

                    if (tailer.wasPadding()) {

                        continue;
                    }

                    try {

                        tailer.readUTFΔ();
                        testPassed.set(true);

                    } catch (IllegalStateException e) {

                        e.printStackTrace();

                    } finally {

                        tailer.finish();
                    }

                    break;
                }
            }
        });

        consumer.shutdown();

        await(producer);
        await(consumer);

        source.close();
        sink.close();

        Assert.assertTrue(testPassed.get());
    }

    private void await(final ExecutorService service) {

        while (!service.isTerminated()) {

            try {

                service.awaitTermination(1, TimeUnit.SECONDS);

            } catch (InterruptedException ignore) {

            }
        }
    }

    private String perlX(final String seed, final int times) {

        final StringBuilder sb = new StringBuilder(seed.length() * times);

        for (int i = 0; i < times; ++i) {

            sb.append(seed);
        }

        return sb.toString();
    }
}
