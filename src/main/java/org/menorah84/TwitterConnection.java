package org.menorah84;

import org.apache.flume.Context;

import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterConnection {
	
	private ConfigurationBuilder cb;
	private Twitter			twitter;
	private TwitterStream 	twitterStream;
	
	private String 	consumerKey;
	private String 	consumerSecret;
	private String 	accessToken;
	private String 	accessTokenSecret;
	private String 	proxyHost;
	private int 	proxyPort;
	
	private boolean isCSVLimitedWriteMode;
	
	public Twitter getTwitter() {
		return twitter;
	}

	public TwitterStream getTwitterStream() {
		return twitterStream;
	}
	
	public String getConsumerKey() {
		return consumerKey;
	}
	
	public String getAccessToken() {
		return accessToken;
	}
	
	public TwitterConnection setCredentialsFromFlumeConfig(Context context, boolean isCSVLimitedWriteMode) {
		consumerKey 		= context.getString(Constants.CONSUMER_KEY_KEY);
		consumerSecret 		= context.getString(Constants.CONSUMER_SECRET_KEY);
		accessToken			= context.getString(Constants.ACCESS_TOKEN_KEY);
		accessTokenSecret 	= context.getString(Constants.ACCESS_TOKEN_SECRET_KEY);
		proxyHost			= context.getString(Constants.PROXY_HOST);
		proxyPort			= context.getInteger(Constants.PROXY_PORT);
		this.isCSVLimitedWriteMode = isCSVLimitedWriteMode;
		return this;
	}
	
	private ConfigurationBuilder setCredentials() {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setOAuthConsumerKey(this.consumerKey);
		cb.setOAuthConsumerSecret(this.consumerSecret);
		cb.setOAuthAccessToken(this.accessToken);
		cb.setOAuthAccessTokenSecret(this.accessTokenSecret);
		cb.setHttpProxyHost(proxyHost);
		cb.setHttpProxyPort(proxyPort);
		cb.setHttpProxyUser("");
		cb.setHttpProxyPassword("");
		return cb;
	}
	
	public Twitter establishRESTConnection() {
		cb = this.setCredentials();
		if (!this.isCSVLimitedWriteMode) {
			cb.setJSONStoreEnabled(true);
			cb.setIncludeEntitiesEnabled(true);
		}

		twitter = new TwitterFactory(cb.build()).getInstance();
		
		return twitter;
	}
	
	public TwitterStream establishStreamConnection() {
		cb = this.setCredentials();
		if (!this.isCSVLimitedWriteMode) {
			cb.setJSONStoreEnabled(true);
			cb.setIncludeEntitiesEnabled(true);
		}

		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		
		return twitterStream;
	}
	
}
