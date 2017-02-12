package org.menorah84;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.apache.log4j.LogManager;

import twitter4j.GeoLocation;
import twitter4j.Place;
import twitter4j.Query;
import twitter4j.Status;
import twitter4j.User;

public class ExtractorUtils {
	
	private static final Logger logger = LogManager.getLogger(ExtractorUtils.class);
	
	private static DateFormat dateFormat = new SimpleDateFormat(Constants.ISO8601_DATE_FORMAT, Locale.US);
	private static Random random = new Random();
	
	static {
		dateFormat.setTimeZone(TimeZone.getTimeZone(Constants.UTC_TIME_ZONE));
	}
	
	/**
	 * Splits a string of keywords and returns an array of String.
	 * It ignores spaces and non-printable characters like \n and \t. 
	 * Returns null if no valid word in input.
	 *  
	 * @param keywordStr a single string of keywords, separated by commas
	 * @return a list of keywords
	 */
	public static String[] getKeywords(String keywordStr) {
		
		String[] keywords;
		if (null == keywordStr || keywordStr.trim().length() == 0) {
			return null;
		} else {
			keywords = keywordStr.split(",");
			List<String> wordList = new ArrayList<String>();
			for (int i = 0; i < keywords.length; i++) {
				String word = keywords[i].trim();
				if (!word.equals("")) {
					wordList.add(word);
				}
				
			}
			
			return wordList.toArray(new String[wordList.size()]);
		}
	}

	/** 
	 * Transforms an array of String (keywords) into a single String
	 * as a search key compatible to Twitter Search API specification.
	 * 
	 * @param keywords an array of String
	 * @return a single String as a search key
	 */
	public static Query searchQuery(String[] keywords) 
		throws InvalidTwitterKeywordException {
		StringBuilder sb = new StringBuilder();
		String prefix = Constants.EMPTY_STRING;
		boolean appended = false;
		if (null != keywords) {
			for (int i = 0; i < keywords.length; i++) {
				if (null != keywords[i] && keywords[i].trim().length() != 0) {
					sb.append(prefix);
					prefix = Constants.OR;
					sb.append(keywords[i].trim());
					appended = true;
				}
			}
		}
		
		// if nothing got appended, it means we don't have a valid keyword
		if (appended) {
			return new Query(sb.toString());
		} else {
			throw new InvalidTwitterKeywordException(Constants.INVALID_TWITTER_KEYWORD_EXCEPTION_MESSAGE);
		}
	}
	
	/**
	 * "Cleans" a string of text (applied on tweets and user description) by removing
	 * non-printable characters like tab, new line and carriage return.
	 * 
	 * Deprecated, use clean instead
	 * 
	 * @param str a String of text to be cleaned
	 * @return
	 */
	public static String removeTabNewline(final String str) {
	
        StringBuilder sb = new StringBuilder();
        char current;
        
        if (null != str) {
            for (int i = 0; i < str.length(); i++) {
            	current = str.charAt(i);
                if (current != Constants.NEW_LINE && 
                	current != Constants.CARRIAGE_RETURN && 
                	current != Constants.TAB) {
                	sb.append(current);
                // Add the logic on next line in and uncomment if you want this method to replace ',' with ';'
                //	 && current != Constants.COMMA
                //	} else if (current == Constants.COMMA) {
                //		sb.append(Constants.COMMA_REPLACEMENT);
                } else {
                	sb.append(Constants.EMPTY_STRING);
                } // else don't append anything
            }
            
            String s = sb.toString();
           
            return s;

        } else {
        	return null;
        }
    }
	
	/**
	 * "Cleans" a string of text (applied on tweets and user description) by removing
	 * non-printable characters like tab, new line and carriage return.
	 * 
	 * Replaces double-quotes with single-quotes
	 * 
	 * @param str a String of text to be cleaned
	 * @return
	 */
	public static String clean(final String str) {
	
        StringBuilder sb = new StringBuilder();
        char current;
        
        if (null != str) {
            for (int i = 0; i < str.length(); i++) {
            	current = str.charAt(i);
                if (current != Constants.NEW_LINE && 
                	current != Constants.CARRIAGE_RETURN && 
                	current != Constants.TAB &&
                	current != Constants.QUOTE) {
                	sb.append(current);
                // Add the logic on next line in and uncomment if you want this method to replace ',' with ';'
                //	 && current != Constants.COMMA
                //	} else if (current == Constants.COMMA) {
                //		sb.append(Constants.COMMA_REPLACEMENT);
                } else if (current == Constants.QUOTE) {
                	sb.append(Constants.SINGLE_QUOTE);
                } else {
                
                	sb.append(Constants.EMPTY_STRING);
                } // else don't append anything
            }
            
            String s = sb.toString();
           
            return s;

        } else {
        	return null;
        }
    }
	
	public static String getTweetCSVRecord(Status tweet) {
		
		StringBuilder sb = new StringBuilder();
		
		// id
		sb.append(String.valueOf(tweet.getId()));
		sb.append(Constants.FIELD_TERMINATOR);
		
		/* This is the alternative using joda time
		 *
		 * Be sure to include declaration at the start of the class
		 * private static DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
		 * 
		 * sb.append(fmt.print(new DateTime(tweet.getCreatedAt()).withZone(DateTimeZone.UTC))); 
		 * 
		 */

		// created_at
		// sets to ISO 8601 format, UTC timezone
		sb.append(dateFormat.format(tweet.getCreatedAt()));
		sb.append(Constants.FIELD_TERMINATOR);

		// user_name, screen_name, user_desc, user_location
		User user = tweet.getUser();
		if (null != user) {
			sb.append(clean(user.getName()));
			sb.append(Constants.FIELD_TERMINATOR);
			
			sb.append(Constants.AT_SYMBOL + user.getScreenName());
			sb.append(Constants.FIELD_TERMINATOR);
			
			sb.append(clean(user.getDescription()));
			sb.append(Constants.FIELD_TERMINATOR);
			
			sb.append(clean(user.getLocation()));
			sb.append(Constants.FIELD_TERMINATOR);
		} else {
			sb.append(Constants.NULL_STRING);
			sb.append(Constants.FIELD_TERMINATOR);
			sb.append(Constants.NULL_STRING);
			sb.append(Constants.FIELD_TERMINATOR);
			sb.append(Constants.NULL_STRING);
			sb.append(Constants.FIELD_TERMINATOR);
			sb.append(Constants.NULL_STRING);
			sb.append(Constants.FIELD_TERMINATOR);
		}
		
		// text (tweet)
		sb.append(clean(tweet.getText().trim()));
		sb.append(Constants.FIELD_TERMINATOR);
		
		// favorite_count
		sb.append(tweet.getFavoriteCount());
		sb.append(Constants.FIELD_TERMINATOR);
		
		// retweet_count
		sb.append(tweet.getRetweetCount());
		sb.append(Constants.FIELD_TERMINATOR);
		
		// latitude, longitude
		GeoLocation geo = tweet.getGeoLocation();
		if (null != geo) {
			sb.append(geo.getLatitude());
			sb.append(Constants.FIELD_TERMINATOR);
			sb.append(geo.getLongitude());
			sb.append(Constants.FIELD_TERMINATOR);
		} else {
			sb.append(Constants.NULL_STRING);
			sb.append(Constants.FIELD_TERMINATOR);
			sb.append(Constants.NULL_STRING);
			sb.append(Constants.FIELD_TERMINATOR);
		}
		
		Place place = tweet.getPlace();
		
		// place.name, place.full_name, place.country, place.country_code
		if (null != place) {
			sb.append(place.getName());
			sb.append(Constants.FIELD_TERMINATOR);
			
			sb.append(place.getFullName());
			sb.append(Constants.FIELD_TERMINATOR);
			
			sb.append(place.getCountry());
			sb.append(Constants.FIELD_TERMINATOR);

			sb.append(place.getCountryCode());	
			sb.append(Constants.FIELD_TERMINATOR);
		} else {
			sb.append(Constants.NULL_STRING);
			sb.append(Constants.FIELD_TERMINATOR);
			sb.append(Constants.NULL_STRING);
			sb.append(Constants.FIELD_TERMINATOR);
			sb.append(Constants.NULL_STRING);
			sb.append(Constants.FIELD_TERMINATOR);
			sb.append(Constants.NULL_STRING);
			sb.append(Constants.FIELD_TERMINATOR);
		}
		
		// alternative_latitude, alternative_longitude
		if (null == geo && null != place) {
			GeoLocation alternativeGeo = getAlternativeCoordinates(place);
			sb.append(alternativeGeo.getLatitude());
			sb.append(Constants.FIELD_TERMINATOR);
			sb.append(alternativeGeo.getLongitude());
		} else {
			sb.append(Constants.NULL_STRING);
			sb.append(Constants.FIELD_TERMINATOR);
			sb.append(Constants.NULL_STRING);
		}
		
		return sb.toString();
	}
	
	private static GeoLocation getAlternativeCoordinates(Place place) {
		
		GeoLocation[][] bounds = place.getBoundingBoxCoordinates();
		
		/*
		 * Get a point contained within boundary of place by either of the following:
		 * 1. the mean of the bounding box
		 * 2. the mean of the optimums (lat & long separately) of the bounding box
		 * 3. one of the points in the bounding box
		 *
		 * we'll use (1) and (2) randomly
		 */
		double minLatitude = 0.00;
		double maxLatitude = 0.00;
		double minLongitude = 0.00;
		double maxLongitude = 0.00;		
		double sumLatitude = 0.00;
		double sumLongitude = 0.00;
		int numPoints = 0;
		
		for (int i = 0; i < bounds.length; i++) {
			for (int j = 0; j < bounds[i].length; j++) {
				double latitude = bounds[i][j].getLatitude();
				double longitude = bounds[i][j].getLongitude();
				
				sumLatitude += latitude;
				sumLongitude += longitude;
				numPoints++;
				
				if (i == 0 && j == 0) {
					minLatitude = latitude;
					maxLatitude = latitude;
					minLongitude = longitude;
					maxLongitude = longitude;
				} else {
					minLatitude = (latitude < minLatitude) ? latitude : minLatitude;
					maxLatitude = (latitude > maxLatitude) ? latitude : maxLatitude;
					minLongitude = (longitude < minLongitude) ? longitude : minLongitude;
					maxLongitude = (longitude > maxLongitude) ? longitude : maxLongitude;
				}
			}
		}
		
	
		GeoLocation geo = random.nextBoolean() ? 
						  // mean of the min & max
						  new GeoLocation((minLatitude + maxLatitude) / 2d, (minLongitude + maxLongitude) / 2d) :
						  // mean of bounding box points	  
					      new GeoLocation(sumLatitude / (double) numPoints, sumLongitude / (double) numPoints);
		
		return geo;
	}
	
}
