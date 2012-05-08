package com.taobao.terminator.common.timers;

import java.util.HashMap;
import java.util.Map;

@Deprecated
public class LazyLoadTimerExpressions implements TimerExpression{
	Map<String,Holder> expressions ;
	
	public LazyLoadTimerExpressions(){
		this.initDefaults();
	}
	
	private void initDefaults() {
		if(this.expressions == null) {
			this.expressions = new HashMap<String,Holder>();
		}
		
		expressions.put("perday", new Holder() {
			@Override
			public TimerExpression newInstance() {
				return new PerdayExpression();
			}
		});

		expressions.put("interval", new Holder() {

			@Override
			public TimerExpression newInstance() {
				return new IntervalExpression();
			}
		});
	}
	
	@Override
	public TimerInfo parse(String expression) throws TimerExpressionException {
//		TimerExp t = Utils.parseToExp(expression);
		/*String name = t.name;
		String expre = t.expression;*/
		
		
		return null;
	}
	
	abstract class Holder {
		TimerExpression te = null;
		public TimerExpression getInstance() {
			if(te == null) {
				te = newInstance();
			}
			return te;
		}
		public abstract TimerExpression newInstance();
	}
}
