package com.xiaoji.duan.exc;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.vertx.amqpbridge.AmqpBridge;
import io.vertx.amqpbridge.AmqpBridgeOptions;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.multipart.MultipartForm;

public class MainVerticle extends AbstractVerticle {

	private WebClient client = null;
	private AmqpBridge bridge = null;
	private AmqpBridge remote = null;

	@Override
	public void start(Future<Void> startFuture) throws Exception {

		String cachedir = config().getString("cache.dir", "/opt/duan/exc/caches");

		if (!vertx.fileSystem().existsBlocking(cachedir)) {
			vertx.fileSystem().mkdirsBlocking(cachedir);
		}
		
		client = WebClient.create(vertx);

		bridge = AmqpBridge.create(vertx);

		bridge.endHandler(endHandler -> {
			connectStompServer();
		});
		connectStompServer();

		AmqpBridgeOptions remoteOption = new AmqpBridgeOptions();
		remoteOption.setReconnectAttempts(60);			// 重新连接尝试60次
		remoteOption.setReconnectInterval(60 * 1000);	// 每次尝试间隔1分钟
		
		remote = AmqpBridge.create(vertx, remoteOption);
		
		connectRemoteServer();
	}

	private void connectRemoteServer() {
		remote.start(config().getString("remote.server.host", "sa-amq"),
			config().getInteger("remote.server.port", 5672), res -> {
				if (res.failed()) {
//						res.cause().printStackTrace();
					connectRemoteServer();
				} else {
			        System.out.println("Remote amqp server connected.");
				}
			});
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
        String regex = "\\#\\{([^\\/^}]+)\\}";
        Pattern pattern = Pattern.compile (regex);
        Matcher matcher = pattern.matcher(httpUrlAbs);
        JsonObject params = new JsonObject().put("sd", "sd").put("openid", "openid");
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

		JsonArray callback = data.getJsonObject("context", new JsonObject()).getJsonArray("callback", new JsonArray());
		String httpMethod = data.getJsonObject("context", new JsonObject()).getString("method", "get");
		JsonObject querys = data.getJsonObject("context", new JsonObject()).getJsonObject("querys", new JsonObject());
		String httpUrlAbs = data.getJsonObject("context", new JsonObject()).getString("urlabs", "");
		String charset = data.getJsonObject("context", new JsonObject()).getString("charset", "");
		String bridgeType = data.getJsonObject("context", new JsonObject()).getString("bridge", "");
		JsonObject header = data.getJsonObject("context", new JsonObject()).getJsonObject("header", new JsonObject());

		if (httpUrlAbs.contains("#")) {
			if (!(data.getJsonObject("context", new JsonObject()).getValue("params") instanceof JsonObject)) {
				System.out.println("Wrong parameters exit.");
				return;
			}
			
			JsonObject params = data.getJsonObject("context", new JsonObject()).getJsonObject("params", new JsonObject());
			
			if (params == null || httpUrlAbs == null || "".equals(httpUrlAbs)) {
				System.out.println("Wrong parameters exit.");
				return;
			}

            System.out.println("Replace before " + httpUrlAbs);

            String regex = "\\#\\{([^\\/^}]+)\\}";
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
		
		System.out.println(httpMethod + ", " + bridgeType);
		
		if ("get".equals(httpMethod.toLowerCase())) {
			if ("binary".equals(bridgeType)) {
				getbinary(future, httpUrlAbs, charset, header, querys, httpData);
			} else {
				get(future, httpUrlAbs, charset, header, querys, httpData);
			}
		}
		
		if ("put".equals(httpMethod.toLowerCase())) {
			put(future, httpUrlAbs, charset, header, querys, httpData);
		}
		
		if ("post".equals(httpMethod.toLowerCase())) {
			post(future, httpUrlAbs, charset, header, querys, httpData);
		}
		
		future.setHandler(handler -> {
			if (handler.succeeded()) {
				JsonObject nextctx = new JsonObject()
						.put("context", new JsonObject()
								.put("executed", handler.result()));
				
				MessageProducer<JsonObject> producer = bridge.createProducer(next);
				producer.send(new JsonObject().put("body", nextctx));
				producer.end();

				System.out.println("Consumer " + consumer + " send to [" + next + "] result [" + nextctx.encode() + "]");
				
				if (callback.size() > 0) {
					for (int i = 0; i < callback.size(); i++) {
						String onecallback = callback.getString(i);
						
						MessageProducer<JsonObject> callbackproducer = remote.createProducer(onecallback);
						callbackproducer.send(new JsonObject().put("body", nextctx));
						callbackproducer.end();
					}
				}
			} else {
				JsonObject nextctx = new JsonObject()
						.put("context", new JsonObject()
								.put("executed", new JsonObject().put("cause", handler.cause().getMessage())));
				
				MessageProducer<JsonObject> producer = bridge.createProducer(next);
				producer.send(new JsonObject().put("body", nextctx));
				producer.end();

				System.out.println("Consumer " + consumer + " send to [" + next + "] result [" + nextctx.encode() + "]");

				if (callback.size() > 0) {
					for (int i = 0; i < callback.size(); i++) {
						String onecallback = callback.getString(i);
						
						MessageProducer<JsonObject> callbackproducer = remote.createProducer(onecallback);
						callbackproducer.send(new JsonObject().put("body", nextctx));
						callbackproducer.end();
					}
				}
			}
		});
	}
	
	private void put(Future<JsonObject> future, String url, String charset, JsonObject header, JsonObject querys, JsonObject data) {
		System.out.println("Launch url(put) : " + url);
		HttpRequest<Buffer> request = client.putAbs(url);
		
		if (!header.isEmpty()) {
			for (String field : header.fieldNames()) {
				request.putHeader(field, header.getString(field, ""));
			}
		}
		
		if (!querys.isEmpty()) {
			for (String field : querys.fieldNames()) {
				request.addQueryParam(field, querys.getString(field, ""));
			}
		}
		
		request.sendJsonObject(data, handler -> {
			if (handler.succeeded()) {
				HttpResponse<Buffer> result = handler.result();
				
				if (result != null) {
					String resp = "";
					
					if ("".equals(charset)) {
						resp = result.bodyAsString();
					} else {
						try {
							resp = new String(result.bodyAsBuffer().getBytes(), charset);
						} catch (Exception e) {
							e.printStackTrace();
							resp = e.getMessage();
						} finally {
							if (resp == null) {
								resp = "";
							}
						}
					}
					
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
	
	private void get(Future<JsonObject> future, String url, String charset, JsonObject header, JsonObject querys, JsonObject data) {
		System.out.println("Launch url(get) : " + url);
		HttpRequest<Buffer> request = client.getAbs(url);
		
		if (!header.isEmpty()) {
			for (String field : header.fieldNames()) {
				request.putHeader(field, header.getString(field, ""));
			}
		}
		
		if (!querys.isEmpty()) {
			for (String field : querys.fieldNames()) {
				request.addQueryParam(field, querys.getString(field, ""));
			}
		}
		
		request.sendJsonObject(data, handler -> {
			if (handler.succeeded()) {
				HttpResponse<Buffer> result = handler.result();
				
				if (result != null) {
					String resp = "";
					
					if ("".equals(charset)) {
						resp = result.bodyAsString();
					} else {
						try {
							resp = new String(result.bodyAsBuffer().getBytes(), charset);
						} catch (Exception e) {
							e.printStackTrace();
							resp = e.getMessage();
						} finally {
							if (resp == null) {
								resp = "";
							}
						}
					}
					
					if (resp.startsWith("{") && resp.endsWith("}")) {
						future.complete(new JsonObject().put("Content-Type", result.getHeader("Content-Type")).put("type", "JsonObject").put("response", new JsonObject(resp)));
					} else if (resp.startsWith("[") && resp.endsWith("]")) {
						future.complete(new JsonObject().put("Content-Type", result.getHeader("Content-Type")).put("type", "JsonArray").put("response", new JsonArray(resp)));
					} else {
						future.complete(new JsonObject().put("Content-Type", result.getHeader("Content-Type")).put("type", "Plain").put("response", resp));
					}
				} else {
					System.out.println("Response body is empty.");
					future.complete(new JsonObject());
				}
			} else {
				future.fail(handler.cause());
			}
		});
	}

	/**
	 * 
	 * 获取数据后上传指定服务
	 * 
	 * @param future
	 * @param url
	 * @param charset
	 * @param header
	 * @param querys
	 * @param data
	 */
	private void getbinary(Future<JsonObject> future, String url, String charset, JsonObject header, JsonObject querys, JsonObject data) {
		System.out.println("Launch url(getbinary) : " + url);
		HttpRequest<Buffer> request = client.getAbs(url);
		
		if (header != null && !header.isEmpty()) {
			for (String field : header.fieldNames()) {
				request.putHeader(field, header.getString(field, ""));
			}
		}
		
		if (querys != null && !querys.isEmpty()) {
			for (String field : querys.fieldNames()) {
				request.addQueryParam(field, querys.getString(field, ""));
			}
		}
		
		StringBuffer nextpath = new StringBuffer();
		
		if (data != null && !data.isEmpty()) {
			nextpath.append(data.getString("path", ""));
		}
		
		System.out.println("nextpath : " + nextpath);
		request.sendJsonObject(new JsonObject(), handler -> {
			if (handler.succeeded()) {
				HttpResponse<Buffer> result = handler.result();
				
				if (result != null) {
					System.out.println(result.headers().toString());

					String contentType = result.getHeader("Content-Type");
					String filedisp = result.getHeader("Content-Disposition");
					
					String extension = "";
					String cachedir = config().getString("cache.dir", "/opt/duan/exc/caches");
					StringBuffer filename = new StringBuffer();
					
//					if (contentType != null && !"".equals(contentType) && contentType.contains("/")) {
//						extension = contentType.substring(contentType.lastIndexOf("/") + 1).toLowerCase();
//						filename.append(UUID.randomUUID().toString() + "." + extension);
//					} else {
//						filename.append(UUID.randomUUID().toString());
//					}
					filename.append(UUID.randomUUID().toString());

					System.out.println(contentType);
					System.out.println(filename);
					
					vertx.fileSystem().writeFile(cachedir + "/" + filename.toString(), result.body(), writeFile -> {
						if (writeFile.succeeded()) {
							// 接收到的数据上传到指定服务
							HttpRequest<Buffer> push = client.postAbs(nextpath.toString());

							MultipartForm form = MultipartForm.create()
									.attribute("saPrefix", "exc")
									.attribute("group", "pluto")
									.attribute("username", "group")
									.binaryFileUpload("file", filename.toString(), cachedir + "/" + filename.toString(), contentType);
							
							push.sendMultipartForm(form, upload -> {
								if (upload.succeeded()) {
									future.complete(new JsonObject());
								} else {
									future.fail(upload.cause());
								}
							});
						} else {
							future.fail(writeFile.cause());
						}
					});
				} else {
					System.out.println("Response body is empty.");
					future.complete(new JsonObject());
				}
			} else {
				System.out.println(handler.cause().getMessage());
				future.fail(handler.cause());
			}
		});
	}
	
	private void post(Future<JsonObject> future, String url, String charset, JsonObject header, JsonObject querys, JsonObject data) {
		System.out.println("Launch url(post) : " + url);
		HttpRequest<Buffer> request = client.postAbs(url);
		
		if (!header.isEmpty()) {
			for (String field : header.fieldNames()) {
				request.putHeader(field, header.getString(field, ""));
			}
		}
		
		if (!querys.isEmpty()) {
			for (String field : querys.fieldNames()) {
				request.addQueryParam(field, querys.getString(field, ""));
			}
		}
		
		request.sendJsonObject(data, handler -> {
			if (handler.succeeded()) {
				HttpResponse<Buffer> result = handler.result();
				
				if (result != null) {
					String resp = "";
					
					if ("".equals(charset)) {
						resp = result.bodyAsString();
					} else {
						try {
							resp = new String(result.bodyAsBuffer().getBytes(), charset);
						} catch (Exception e) {
							e.printStackTrace();
							resp = e.getMessage();
						} finally {
							if (resp == null) {
								resp = "";
							}
						}
					}
					
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
