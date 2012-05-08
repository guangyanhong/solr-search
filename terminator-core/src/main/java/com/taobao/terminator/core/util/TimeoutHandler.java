package com.taobao.terminator.core.util;

/**
 * 这是一个标记接口，被<code>TimeoutThread</code>使用。当发生超时时，<code>TimeoutThread</code>
 * 调用<code>TimeoutHandler</code>实现类中的指定方法来处理超时。
 * @author tianxiao
 *
 */
public interface TimeoutHandler {}
