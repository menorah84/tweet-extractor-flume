import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import Constants;
import ExtractorUtils;
import InvalidTwitterKeywordException;

public class TwitterSearchSource extends AbstractSource 
	implements EventDrivenSource, Configurable {
	
	private static final Logger logger = LogManager.getLogger(TwitterSearchSource.class);
	
	private String[] keywords;
	
	private Twitter twitter;
	
	private boolean isCSVLimitedWriteMode = false; 
	
	public void configure(Context context) {
		
		TwitterConnection conn = new TwitterConnection();
		String writeMode = context.getString(Constants.WRITE_MODE);
		if (null != writeMode && writeMode.trim().toLowerCase().equals(Constants.CSV_LIMITED)) {
			isCSVLimitedWriteMode = true;
		} // else defaults to raw JSON
		twitter = conn.setCredentialsFromFlumeConfig(context, isCSVLimitedWriteMode)
					  .establishRESTConnection();
		keywords = ExtractorUtils.getKeywords(context.getString(Constants.KEYWORDS_KEY, Constants.EMPTY_STRING));
		
	}
	
	@Override
	public void start() {
		
		final ChannelProcessor channel		= getChannelProcessor();
		final Map<String, String> headers 	= new HashMap<String, String>();
		
		try {
			RateLimitStatus rateLimit = twitter.getRateLimitStatus().get(Constants.SEARCH_TWEETS_ENDPOINT);
			
			Query query = ExtractorUtils.searchQuery(keywords);
			query.setCount(Constants.TWEETS_PER_QUERY);
			query.setResultType(Query.ResultType.recent);
			
			QueryResult result;
			
			do {
				
				if (rateLimit.getRemaining() == 0) {
					
					int secondsToSleep = rateLimit.getSecondsUntilReset() + 10;
					
					logger.debug("Twitter Search API call maxed. Sleeping for " + secondsToSleep + " seconds to refresh...");
					
					try {
						Thread.sleep(secondsToSleep * 1000l);
					} catch (InterruptedException e) {
						logger.debug("InterruptedException. Sleep interrupted.");
					}
				}
				
	            result = twitter.search(query);
	            List<Status> tweets = result.getTweets();
	            
	            if (isCSVLimitedWriteMode) {
	            	for (Status tweet : tweets) {
		            	logger.debug("@" + tweet.getUser().getScreenName() + ": " + tweet.getText());
		            	
		            	headers.put("timestamp", String.valueOf(tweet.getCreatedAt().getTime()));
		            	
		            	// use limited CSV write if specified
		            	Event event = EventBuilder.withBody(
		            			ExtractorUtils.getTweetCSVRecord(tweet).getBytes(), headers);
		            	channel.processEvent(event);
		            }
	            } else {
	            	for (Status tweet : tweets) {
		            	logger.debug("@" + tweet.getUser().getScreenName() + ": " + tweet.getText());
		            	
		            	headers.put("timestamp", String.valueOf(tweet.getCreatedAt().getTime()));
		            	
		            	// use raw JSON of limited CSV write not specified 
		            	Event event = EventBuilder.withBody(
		            			TwitterObjectFactory.getRawJSON(tweet).getBytes(), headers);
		            	channel.processEvent(event);
		            }
	            }
	            
	            rateLimit = result.getRateLimitStatus();
	        } while ((query = result.nextQuery()) != null);
			
			logger.debug("Twitter Search API not returning results anymore.");
			
		} catch (InvalidTwitterKeywordException ie) {
			ie.printStackTrace();
			logger.debug(ie.getMessage());
		} catch (TwitterException te) {
            te.printStackTrace();
            logger.debug("Failed to search tweets: " + te.getMessage());
        }
		
		super.start();
	}	
	
	@Override 
	public void stop() {
		logger.debug("Shutting down Twitter... ");
		super.stop();
	}
	
}
