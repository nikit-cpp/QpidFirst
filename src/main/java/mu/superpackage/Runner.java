package mu.superpackage;

import javax.jms.*;
import javax.naming.Context;
import javax.naming.InitialContext;
import java.util.Properties;

/**
 * Created by Nikita Konev on 13.05.2015.
 */
public class Runner {

    public Runner() {
    }

    public static void main(String[] args) throws Exception {
        Runner runner = new Runner();
        runner.runTest();
    }

    private void runTest() throws Exception {
        System.setProperty("qpid.amqp.version", "0-9-1");
        Properties properties = new Properties();
        properties.load(this.getClass().getResourceAsStream("/helloworld.properties"));
        Context context = new InitialContext(properties);

        ConnectionFactory connectionFactory = (ConnectionFactory) context.lookup("qpidConnectionFactory");
        Connection connection = connectionFactory.createConnection();
        connection.start();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = (Queue) context.lookup("myqueue");

        MessageConsumer messageConsumer = session.createConsumer(queue);
        MessageProducer messageProducer = session.createProducer(queue);

        int count = 5;
        for(int i=1; i<=count; ++i) {
            TextMessage message = session.createTextMessage();
            message.setText("тело");
            message.setStringProperty("topic", "топик");
            message.setStringProperty("to", "nikita.konev@ourcompany.com");
            message.setIntProperty("server_id", 1);
            messageProducer.send(message);
            //System.out.println("Sended " + i);
        }
        System.out.println("Sended " + count);

        // message = (TextMessage)messageConsumer.receive(1000);
        // System.out.println(message.getText());

        connection.close();
        context.close();
    }
}