
import static org.junit.Assert.*;

import org.junit.Test;

import twitter4j.Query;

import org.menorah84.InvalidTwitterKeywordException;

public class ExtractorUtilsTest {
	
	@Test
	public void testGetKeywordsNull() {
		assertArrayEquals(null, ExtractorUtils.getKeywords(null));
	}
	
	@Test
	public void testGetKeywordsEmptySpace() {
		assertArrayEquals(null, ExtractorUtils.getKeywords(""));
		assertArrayEquals(null, ExtractorUtils.getKeywords("    "));
	}

	@Test
	public void testGetKeywords1() {
		String keywordStr = "oneword";
		
		String[] keywords = {"oneword"};
		
		assertArrayEquals(keywords, ExtractorUtils.getKeywords(keywordStr));
	}
	
	@Test
	public void testGetKeywordsN() {
		String keywordStr = "one, two, three, , #four, /five,,,,,";
		
		String[] keywords = {"one", "two", "three", "#four", "/five"};
		
		assertArrayEquals(keywords, ExtractorUtils.getKeywords(keywordStr));
	}
	
	@Test
	public void testGetKeywordsEscape() {
		String keywordStr = " \n newLine, \t";
		
		String[] keywords = {"newLine"};
		
		assertArrayEquals(keywords, ExtractorUtils.getKeywords(keywordStr));
	}
	
	@Test
	public void testSearchQuery1() 
			throws InvalidTwitterKeywordException {
		String[] keywords = {"     oneword    "};
		
		Query actual = ExtractorUtils.searchQuery(keywords);
		
		Query expected = new Query("oneword");
		
		assertTrue(expected.equals(actual));
			
	}
	
	@Test
	public void testSearchQueryN() 
			throws InvalidTwitterKeywordException {
		String[] keywords = {"one", " two ", "three", "", "#four", null, "/five"};
		
		Query actual = ExtractorUtils.searchQuery(keywords);
		
		Query expected = new Query("one OR two OR three OR #four OR /five");
		
		assertTrue(expected.equals(actual));
			
	}
	
	@Test
	public void testSearchQueryEscape() 
			throws InvalidTwitterKeywordException {
		String[] keywords = {" \n newLine", "\t"};
		
		Query actual = ExtractorUtils.searchQuery(keywords);
		
		Query expected = new Query("newLine");
		
		assertTrue(expected.equals(actual));
			
	}
	
	@Test
	public void testSearchQueryNull() {
		Throwable e = null;
		String[] keywords = null;
		
		try {
			ExtractorUtils.searchQuery(keywords);
		} catch (Throwable ex) {
			e = ex;
		}
		
		assertTrue(e instanceof InvalidTwitterKeywordException);
	}
	
	@Test
	public void testSearchQueryEmptySpace() {
		Throwable e = null;
		String[] keywords = {" \t ", "  \n ", null, "  "};
		
		try {
			ExtractorUtils.searchQuery(keywords);
		} catch (Throwable ex) {
			e = ex;
		}
		
		assertTrue(e instanceof InvalidTwitterKeywordException);
	}
	
	/*
	 * Test for words with no tab, new line, or carriage return
	 */
	@Test
	public void testRemoveTabNewline1() {
		
		String word = "The quick brown fox jumps over the lazy dog.";
		
		assertEquals("The quick brown fox jumps over the lazy dog.", 
				ExtractorUtils.removeTabNewline(word));
	}
	
	/*
	 * Test for removing tab (\t)
	 */
	@Test
	public void testRemoveTabNewlineT() {
		
		String word = "\t tweet \t";
		
		assertEquals(" tweet ", ExtractorUtils.removeTabNewline(word));
	}
	
	/*
	 * Test for removing new line (\n)
	 */
	@Test
	public void testRemoveTabNewlineNL() {
		
		String word = "\n user description \n";
		
		assertEquals(" user description ", ExtractorUtils.removeTabNewline(word));
	}
	
	/*
	 * Test for removing carriage return (\r)
	 */
	@Test
	public void testRemoveTabNewlineCR() {
		
		String word = "\r place \r";
		
		assertEquals(" place ", ExtractorUtils.removeTabNewline(word));
	}
	
	/*
	 * Test if input str is null
	 */
	@Test
	public void testRemoveTabNewlineNull() {
		
		assertEquals(null, ExtractorUtils.removeTabNewline(null));
	}
	
	
	
	/*
	 * Test for words with empty space
	 */
	@Test
	public void testRemoveTabNewlineEmptySpace() {
		
		String word = "";
		
		assertEquals("", 
				ExtractorUtils.removeTabNewline(word));
	}
	
	/*
	 * Test for replacing ',' with ';' 
	@Test
	public void testRemoveTabNewline() {
		
		String word = "apple, orange, pear";
		
		assertEquals("apple; orange; pear", ExtractorUtils.removeTabNewline(word));
	}
	*/
	
	/*
	 * Test if words with quotes are escaped
	 */

	@Test
	public void testClean() {
		String word = " He said, \n\"Go therefore!\"  ";
		
		assertEquals(" He said, 'Go therefore!'  ", ExtractorUtils.clean(word));
		
	}
	

}
