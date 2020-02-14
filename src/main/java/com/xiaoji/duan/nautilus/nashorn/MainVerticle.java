package com.xiaoji.duan.nautilus.nashorn;

import java.util.Set;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.commons.lang.StringUtils;

import io.netty.util.internal.StringUtil;
import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class MainVerticle extends AbstractVerticle {

	private AmqpBridge local = null;
	
	private ScriptEngine logicalengine = null;
	private ScriptEngine shouldlogicalengine = null;

	@Override
	public void start(Promise<Void> startPromise) throws Exception {
		vertx.exceptionHandler(exception -> {
			error("Vertx exception caught.");
			System.exit(-1);
		});

		ScriptEngineManager factory = new ScriptEngineManager();

		logicalengine = factory.getEngineByName("JavaScript");
		shouldlogicalengine = factory.getEngineByName("JavaScript");

		String shouldlogical = config().getString("shouldclean", "");
		String logical = config().getString("clean", "");
		
		if (StringUtils.isEmpty(shouldlogical)) {
			shouldlogical = "function shouldclean(datasource) \n" + "{\n" + "  var result = {};\n"
					+ "  // filter source code here start\n" + "  var input = JSON.parse(datasource);\n"
					+ "  \n" + "  // filter source code here end\n" + "  return true;\n" + "}";
		} else {
			Buffer buf = vertx.fileSystem().readFileBlocking(config().getString("path.shouldclean", "/usr/verticles/shouldclean.js"));
			shouldlogical = new String(buf.getBytes());
		}
		
		if (StringUtils.isEmpty(logical)) {
			logical = "function clean(datasource) \n" + "{\n" + "  var result = {};\n"
					+ "  // filter source code here start\n" + "  var input = JSON.parse(datasource);\n"
					+ "  \n" + "  // filter source code here end\n" + "  return result;\n" + "}";
		} else {
			Buffer buf = vertx.fileSystem().readFileBlocking(config().getString("path.clean", "/usr/verticles/clean.js"));
			logical = new String(buf.getBytes());
		}
		
		try {
			shouldlogicalengine.eval(shouldlogical);
			logicalengine.eval(logical);
		} catch (Exception e) {
			error(e.getMessage());
			System.out.println(shouldlogical);
			System.out.println(logical);
		}
		
		local = AmqpBridge.create(vertx);
		connectLocalServer();
	}

	private void connectLocalServer() {
		local.start(config().getString("local.server.host", "sa-amq"), config().getInteger("local.server.port", 5672),
				res -> {
					if (res.failed()) {
						res.cause().printStackTrace();
						connectLocalServer();
					} else {
						info("Local stomp server connected.");
						subscribeTrigger(config().getString("amq.app.id", "exc"));
					}
				});
	}
	
	private void subscribeTrigger(String trigger) {
		MessageConsumer<JsonObject> consumer = local.createConsumer(trigger);
		System.out.println("Consumer " + trigger + " subscribed.");
		consumer.handler(vertxMsg -> this.process(trigger, vertxMsg));
	}
	
	private void process(String consumer, Message<JsonObject> received) {
		System.out.println("Consumer " + consumer + " received [" + getShortContent(received.body().encode()) + "]");
		JsonObject data = received.body().getJsonObject("body");

		String next = data.getJsonObject("context").getString("next");

		String ruleId = data.getJsonObject("context").getString("ruleid", "");
		JsonObject datasource = null;

		try {
			datasource = data.getJsonObject("context").getJsonObject("datasource", new JsonObject());
			
			// 查找是否有合并参数 merge_XXXX
			Set<String> keys = data.getJsonObject("context").fieldNames();
			
			for (String key : keys) {
				if (key.startsWith("merge_")) {
					String variable = key.replace("merge_", "");
					Object value = data.getJsonObject("context").getValue(key);
					
					datasource.put(variable, value);
				}
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (datasource == null) {
				datasource = new JsonObject();
			}
		}

		try {
			autoclean(consumer, ruleId, datasource, next, 1);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private void autoclean(String consumer, String targetrule, JsonObject datasource, String nextTask, Integer retry) {
		JsonObject nexto = new JsonObject();

		try {
			Future<JsonObject> logicalFuture = Future.future();

			vertx.executeBlocking(blockingHandler -> {

				if (shouldclean(shouldlogicalengine, datasource)) {
					String output = clean(logicalengine, datasource);
					JsonObject cleaned = StringUtil.isNullOrEmpty(output) ? new JsonObject()
							: (output.startsWith("{") ? new JsonObject(output)
									: new JsonObject().put(config().getString("amq.app.id", "nautilus-nashorn"), new JsonArray(output)));
					blockingHandler.complete(cleaned);
				} else {
					blockingHandler.complete(new JsonObject());
				}
			}, logicalFuture.completer());
			
			logicalFuture.setHandler(handler -> {
				if (handler.succeeded()) {
					JsonObject result = handler.result();
					
					nexto.mergeIn(result);

					JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("cleaned", nexto));

					MessageProducer<JsonObject> producer = local.createProducer(nextTask);
					producer.send(new JsonObject().put("body", nextctx));
					producer.end();

					System.out.println("Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.size() + "]");
				} else {
					handler.cause().printStackTrace();
					
					JsonObject nextctx = new JsonObject().put("context", new JsonObject().put("cleaned", nexto));

					MessageProducer<JsonObject> producer = local.createProducer(nextTask);
					producer.send(new JsonObject().put("body", nextctx));
					producer.end();

					System.out.println("Consumer " + consumer + " send to [" + nextTask + "] result [" + nextctx.size() + "]");
				}
			});

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// 判断输入数据是否需要当前规则清洗
	private boolean shouldclean(ScriptEngine logical, JsonObject datasource) {

		Object res = null;
		Boolean output = Boolean.FALSE;

		try {
			logical.put("datasource", datasource.encode());
			Invocable jsInvoke = (Invocable) logical;
			res = jsInvoke.invokeFunction("shouldclean", new Object[] { datasource });
			output = (Boolean) res;
			//System.out.println(res.getClass().getName());
		} catch (ScriptException | NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (!(output instanceof Boolean)) {
				output = Boolean.FALSE;
			}
		}

		return output;
	}

	// 使用当前规则清洗
	private String clean(ScriptEngine logical, JsonObject datasource) {
		Object res = null;
		String output = "";

		try {
			logical.put("datasource", datasource.encode());
			Invocable jsInvoke = (Invocable) logical;
			res = jsInvoke.invokeFunction("clean", new Object[] { datasource });
			output = (String) res;
			//System.out.println(res.getClass().getName());
		} catch (ScriptException | NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return output;
	}

	public static String getShortContent(String origin) {
		return origin.length() > 512 ? origin.substring(0, 512) : origin;
	}
	
	private void info(String log) {
		if (config().getBoolean("log.info", Boolean.FALSE)) {
			System.out.println(log);
		}
	}

	private void debug(String log) {
		if (config().getBoolean("log.debug", Boolean.FALSE)) {
			System.out.println(log);
		}
	}

	private void error(String log) {
		if (config().getBoolean("log.error", Boolean.TRUE)) {
			System.out.println(log);
		}
	}
	
}
