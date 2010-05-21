package raptor.store.bdb;

import org.apache.log4j.Logger;

import com.sleepycat.db.Database;
import com.sleepycat.db.Environment;
import com.sleepycat.db.ErrorHandler;
import com.sleepycat.db.FeedbackHandler;
import com.sleepycat.db.MessageHandler;

public class DefaultBDBMessageHandler 
    implements ErrorHandler, FeedbackHandler, MessageHandler {
    
    final private static Logger log = Logger.getLogger(MessageHandler.class);

    public void error(Environment environment,
               String errpfx,
               String msg) {
        log.info("error: environment: " + environment + ", errpfx: " + errpfx + ": " +
            msg);
    }
    
    public void recoveryFeedback(Environment environment, int percent) {
        log.info("recoveryFeedback: " + environment + ": " + percent + "%");
    }
    
    public void upgradeFeedback(Database database, int percent) {
        try {
            log.info("upgradeFeedback: " + database.getDatabaseName() + ": " + percent + "%");
        } catch(Exception ex) {
            log.info("upgradeFeedback: " + database + ": " + percent + "%");
        }
    }
    
    public void verifyFeedback(Database database, int percent) {
        try {
            log.info("verifyFeedback: " + database.getDatabaseName() + ": " + percent + "%");
        } catch(Exception ex) {
            log.info("verifyFeedback: " + database + ": " + percent + "%");
        }
    }
    
    public void message(Environment environment, String message) {
        log.info("message: " + environment + ": " + message);
    }
}
