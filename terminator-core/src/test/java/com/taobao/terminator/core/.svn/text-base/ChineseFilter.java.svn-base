package com.taobao.terminator.core;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermAttribute;

public final class ChineseFilter extends TokenFilter {


    // Only English now, Chinese to be added later.
    public static final String[] STOP_WORDS = {
    "and", "are", "as", "at", "be", "but", "by",
    "for", "if", "in", "into", "is", "it",
    "no", "not", "of", "on", "or", "such",
    "that", "the", "their", "then", "there", "these",
    "they", "this", "to", "was", "will", "with"
    };


    private Map stopTable;

    private TermAttribute termAtt;
    
    public ChineseFilter(TokenStream in) {
        super(in);

        stopTable = new HashMap(STOP_WORDS.length);
        for (int i = 0; i < STOP_WORDS.length; i++)
            stopTable.put(STOP_WORDS[i], STOP_WORDS[i]);
        termAtt = (TermAttribute) addAttribute(TermAttribute.class);
    }

    public boolean incrementToken() throws IOException {

        while (input.incrementToken()) {
            String text = termAtt.term();

          // why not key off token type here assuming ChineseTokenizer comes first?
            if (stopTable.get(text) == null) {
                switch (Character.getType(text.charAt(0))) {

                case Character.LOWERCASE_LETTER:
                case Character.UPPERCASE_LETTER:

                    // English word/token should larger than 1 character.
                    if (text.length()>=1) {
                        return true;
                    }
                    break;
                case Character.OTHER_LETTER:

                    // One Chinese character as one Chinese word.
                    // Chinese word extraction to be added later here.

                    return true;
                }

            }

        }
        return false;
    }

}