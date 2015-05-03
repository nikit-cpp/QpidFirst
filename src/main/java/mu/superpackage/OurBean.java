package mu.superpackage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.ejb.*;
import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.io.IOException;
import java.util.Properties;

/**
 * @author Nikita Konev
 */

@Startup
@Singleton
@ConcurrencyManagement(ConcurrencyManagementType.CONTAINER)
@Lock(LockType.READ)

public class OurBean {
    private Thread backgroundProcess;

    private Logger logger = LoggerFactory.getLogger(OurBean.class);

    private volatile Context context = null;
    private volatile Connection connection = null;

    @PostConstruct
    private void init() {

        try {
            // initalizing
            System.setProperty("qpid.amqp.version", "0-9-1");
            Properties properties = new Properties();
            properties.load(OurBean.class.getResourceAsStream("/helloworld.properties"));
            Context context = new InitialContext(properties);

            ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("qpidConnectionFactory");
            Connection connection = connectionFactory.createConnection();
            connection.start();

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE); // transactions=false for delete message from queue
            Queue queue = (Queue) context.lookup("myqueue");

            MessageConsumer messageConsumer = session.createConsumer(queue);

            Runnable r = () -> {
                while (!(Thread.currentThread().isInterrupted())) {
                    try {
                        logger.info("Iteration the " + Thread.currentThread());
                        Message recievedMessage = messageConsumer.receive();

                        String stringMessage;
                        if (recievedMessage instanceof BytesMessage) {
                            BytesMessage bytesMessage = (BytesMessage) recievedMessage;
                            byte data[] = new byte[(int) bytesMessage.getBodyLength()];
                            bytesMessage.readBytes(data);
                            stringMessage = new String(data);
                        } else if (recievedMessage == null) {
                            logger.info("MessageConsumer has been closed, task in closing process...");
                            return;
                        } else {
                            logger.error("Unsupported message type " + recievedMessage);
                            return;
                        }
                        logger.info("stringMessage= " + stringMessage);
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            };

            backgroundProcess = new Thread(r);

            backgroundProcess.start();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PreDestroy
    private void destroy() {
        logger.info("Destroying in " + Thread.currentThread());
        if (null != backgroundProcess) {
            backgroundProcess.interrupt();
        }
        try {
            if (connection != null) {
                connection.close();
            }

            if (context != null)
                context.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
