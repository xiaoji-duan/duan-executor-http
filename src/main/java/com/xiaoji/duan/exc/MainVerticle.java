package com.xiaoji.duan.exc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;

public class MainVerticle extends AbstractVerticle {

	private WebClient client = null;
	private AmqpBridge bridge = null;

	@Override
	public void start(Future<Void> startFuture) throws Exception {

		client = WebClient.create(vertx);

		bridge = AmqpBridge.create(vertx);

		bridge.endHandler(endHandler -> {
			connectStompServer();
		});
		connectStompServer();

	}

	private void connectStompServer() {
		bridge.start(config().getString("stomp.server.host", "sa-amq"),
				config().getInteger("stomp.server.port", 5672), res -> {
					if (res.failed()) {
						res.cause().printStackTrace();
						connectStompServer();
					} else {
						subscribeTrigger(config().getString("amq.app.id", "exc"));
					}
				});
		
	}
	
	private void subscribeTrigger(String trigger) {
		MessageConsumer<JsonObject> consumer = bridge.createConsumer(trigger);
		System.out.println("Consumer " + trigger + " subscribed.");
		consumer.handler(vertxMsg -> this.process(trigger, vertxMsg));
	}
	
	public static void main(String[] args) {
		String httpUrlAbs = "http://sa-aba:8080/aba/#{sd}/user/#{sd}/#{openid}";
		String regex = "\\#\\{([^}]+)\\}";
        Pattern pattern = Pattern.compile (regex);
        Matcher matcher = pattern.matcher(httpUrlAbs);
        JsonObject params = new JsonObject().put("sd", "sd");
        while (matcher.find())
        {
        	String mark = matcher.group();
            String key = mark.substring(2, mark.length() - 1);
            System.out.println(mark + " - " + key);
            
            String paramVal = params.getString(key, "");
            httpUrlAbs = httpUrlAbs.replace(mark, paramVal);
        }
        
        System.out.println(httpUrlAbs);
	}

	public static String getShortContent(String origin) {
		return origin.length() > 512 ? origin.substring(0, 512) : origin;
	}
	
	private void process(String consumer, Message<JsonObject> received) {
		System.out.println("Consumer " + consumer + " received [" + getShortContent(received.body().encode()) + "]");
		JsonObject data = received.body().getJsonObject("body", new JsonObject());

		String httpMethod = data.getJsonObject("context", new JsonObject()).getString("method", "get");
		String httpUrlAbs = data.getJsonObject("context", new JsonObject()).getString("urlabs", "");
		
		if (!(data.getJsonObject("context", new JsonObject()).getValue("params") instanceof JsonObject)) {
			System.out.println("Wrong parameters exit.");
			return;
		}
		
		JsonObject params = data.getJsonObject("context", new JsonObject()).getJsonObject("params", new JsonObject());
		
		if (params == null || httpUrlAbs == null || "".equals(httpUrlAbs)) {
			System.out.println("Wrong parameters exit.");
			return;
		}

		if (httpUrlAbs.contains("#")) {
            System.out.println("Replace before " + httpUrlAbs);

            String regex = "\\#\\{([^}]+)\\}";
            Pattern pattern = Pattern.compile (regex);
            Matcher matcher = pattern.matcher(httpUrlAbs);
            while (matcher.find())
            {
            	String mark = matcher.group();
                String key = mark.substring(2, mark.length() - 1);
                System.out.println(mark + " - " + key);

                String paramVal = "";
                if (params.getValue(key) instanceof String) {
                    paramVal = params.getString(key, "");
                } else {
                    paramVal = params.getValue(key, new String("")).toString();
                }
                
                httpUrlAbs = httpUrlAbs.replace(mark, paramVal);
            }

            System.out.println("Replace after " + httpUrlAbs);
		}
		JsonObject httpData = data.getJsonObject("context").getJsonObject("data", new JsonObject());

		String next = data.getJsonObject("context").getString("next");
		
		Future<JsonObject> future = Future.future();
		
		if ("get".equals(httpMethod.toLowerCase())) {
			get(future, httpUrlAbs, httpData);
		}
		
		if ("post".equals(httpMethod.toLowerCase())) {
			post(future, httpUrlAbs, httpData);
		}
		
		future.setHandler(handler -> {
			if (handler.succeeded()) {
				JsonObject nextctx = new JsonObject()
						.put("context", new JsonObject()
								.put("executed", handler.result()));
				
				MessageProducer<JsonObject> producer = bridge.createProducer(next);
				producer.send(new JsonObject().put("body", nextctx));
				System.out.println("Consumer " + consumer + " send to [" + next + "] result [" + nextctx.encode() + "]");
			} else {
				JsonObject nextctx = new JsonObject()
						.put("context", new JsonObject()
								.put("executed", new JsonObject()));
				
				MessageProducer<JsonObject> producer = bridge.createProducer(next);
				producer.send(new JsonObject().put("body", nextctx));
				System.out.println("Consumer " + consumer + " send to [" + next + "] result [" + nextctx.encode() + "]");
			}
		});
	}
	
	private void get(Future<JsonObject> future, String url, JsonObject data) {
		client.getAbs(url).sendJsonObject(data, handler -> {
			if (handler.succeeded()) {
				HttpResponse<Buffer> result = handler.result();
				
				if (result != null) {
					String resp = result.bodyAsString();
					
					if (resp.startsWith("{") && resp.endsWith("}")) {
						future.complete(new JsonObject().put("Content-Type", result.getHeader("Content-Type")).put("type", "JsonObject").put("response", new JsonObject(resp)));
					} else if (resp.startsWith("[") && resp.endsWith("]")) {
						future.complete(new JsonObject().put("Content-Type", result.getHeader("Content-Type")).put("type", "JsonArray").put("response", new JsonArray(resp)));
					} else {
						future.complete(new JsonObject().put("Content-Type", result.getHeader("Content-Type")).put("type", "Plain").put("response", resp));
					}
				} else {
					future.complete(new JsonObject());
				}
			} else {
				future.fail(handler.cause());
			}
		});
	}

	private void post(Future<JsonObject> future, String url, JsonObject data) {
		client.postAbs(url).sendJsonObject(data, handler -> {
			if (handler.succeeded()) {
				HttpResponse<Buffer> result = handler.result();
				
				if (result != null) {
					String resp = result.bodyAsString();
					
					if (resp.startsWith("{") && resp.endsWith("}")) {
						future.complete(new JsonObject().put("Content-Type", result.getHeader("Content-Type")).put("type", "JsonObject").put("response", new JsonObject(resp)));
					} else if (resp.startsWith("[") && resp.endsWith("]")) {
						future.complete(new JsonObject().put("Content-Type", result.getHeader("Content-Type")).put("type", "JsonArray").put("response", new JsonArray(resp)));
					} else {
						future.complete(new JsonObject().put("Content-Type", result.getHeader("Content-Type")).put("type", "Plain").put("response", resp));
					}
				} else {
					future.complete(new JsonObject());
				}
			} else {
				future.fail(handler.cause());
			}
		});
	}
}
