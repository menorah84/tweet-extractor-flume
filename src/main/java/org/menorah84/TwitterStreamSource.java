package org.menorah84;

import java.util.HashMap;
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

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;

public class TwitterStreamSource extends AbstractSource 
	implements EventDrivenSource, Configurable {
	
	private static final Logger logger = LogManager.getLogger(TwitterStreamSource.class);
	
	private String[] keywords;
	
	private TwitterStream twitterStream;
	
	private boolean isCSVLimitedWriteMode = false;
	
	public void configure(Context context) {
		
		TwitterConnection conn = new TwitterConnection();
		String writeMode = context.getString(Constants.WRITE_MODE);
		if (null != writeMode && writeMode.trim().toLowerCase().equals(Constants.CSV_LIMITED)) {
			isCSVLimitedWriteMode = true;
		} // else defaults to raw JSON
		twitterStream = conn.setCredentialsFromFlumeConfig(context, isCSVLimitedWriteMode)
					  	    .establishStreamConnection();
		keywords = ExtractorUtils.getKeywords(context.getString(Constants.KEYWORDS_KEY, "")); 
		
	}
	
	@Override
	public void start() {
		
		final ChannelProcessor channel 		= getChannelProcessor();
		final Map<String, String> headers	= new HashMap<String, String>();
		
		StatusListener listener = new StatusListener() {
		// StreamListener listener = new StatusListener() {
			
			public void onStatus(Status status) {
				logger.debug("@" + status.getUser().getScreenName() + ": " + status.getText());
				
				headers.put("timestamp", String.valueOf(status.getCreatedAt().getTime()));
				Event event = isCSVLimitedWriteMode ? 
						EventBuilder.withBody(ExtractorUtils.getTweetCSVRecord(status).getBytes(), headers) : 
						EventBuilder.withBody(TwitterObjectFactory.getRawJSON(status).getBytes(), headers);
						
				channel.processEvent(event);
			}

			public void onException(Exception arg0) {}
			public void onDeletionNotice(StatusDeletionNotice arg0) {}
			public void onScrubGeo(long arg0, long arg1) {}
			public void onStallWarning(StallWarning arg0) {}
			public void onTrackLimitationNotice(int arg0) {}
		};
		
		twitterStream.addListener(listener);
		
		if (keywords.length == 0) {
			logger.debug("Starting up Twitter sampling...");
			twitterStream.sample();
		} else {
			logger.debug("Starting up Twitter filtering... ");
			
			FilterQuery query = new FilterQuery().track(keywords);
			twitterStream.filter(query);
		}
		super.start();
	}
	
	@Override
	public void stop() {
		logger.debug("Shutting down Twitter stream... ");
		twitterStream.shutdown();
		super.stop();
	}

}
