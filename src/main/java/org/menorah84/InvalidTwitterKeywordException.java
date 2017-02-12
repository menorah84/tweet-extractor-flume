package org.menorah84;

public class InvalidTwitterKeywordException extends Exception {
	
	private static final long serialVersionUID = 1l;

	public InvalidTwitterKeywordException() {
		super();
	}
	
	public InvalidTwitterKeywordException(String message) {
		super(message);
	}
	
	public InvalidTwitterKeywordException(String message, Throwable cause) {	
		super(message, cause);
	}
	
	public InvalidTwitterKeywordException(Throwable cause) {
		super(cause);
	}
}
