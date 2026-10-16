#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
package ${package}.utils;

import java.util.Iterator;

public interface CloseableIterator<E> extends Iterator<E>, AutoCloseable {

}
