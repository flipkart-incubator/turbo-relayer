Turbo Java Client
=================


#### About
Java client for Turbo relayer 

#### Dependency
```
        <dependency>
            <groupId>com.flipkart.turborelayer</groupId>
            <artifactId>turbo-client</artifactId>
            <version>2.4.0</version>
        </dependency>
   
```

#### Repository
```
    <repositories>
        <repository>
            <id>clojars</id>
            <name>Clojars repository</name>
            <url>https://clojars.org/repo</url>
        </repository>
        <repository>
            <id>central</id>
            <name>Maven Central</name>
            <url>https://repo1.maven.org/maven2/</url>
        </repository>
    </repositories>
```
#### Usage
- Initialize client by providing a [config file](https://github.fkinternal.com/Flipkart/turbo-client/blob/master/hibernate-restbus-client/sample_config.yml), details are given inline.
- Config file needs to be present on `/etc/fk-sc-mq/turbo.yml`, which you can override using SYSTEM property `SC_MQ_TURBO_CONFIG="/anyLocation"`.
- After initializing config properly call .persist method as shown below.
```
    String exchangeName = "exchangeName";
    String exchangeType = "queue";
    String methodType = "POST";
    String httpUri = "http://flipkart.com";
    String replyToExchangeName = null;
    String replyToHttpMethod = "GET";
    String replyToHttpUri = "http://flipkart.com/__reply__to";
    String payload = "{ \"test\": 123 }";
    String groupId = "T1";
    Map<String, String> options = new HashMap<String, String>();
    String appId = "hibernate-restbus-client";
    OutboundMessage outboundMessage = OutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
            methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
            payload, groupId, options, appId);

    // begin transaction has to be called for this session anytime before this call
    ..
    //session is of type org.hibernate.Session
    OutboundMessageRepositoryFactory.getOutboundMessageRepository(session).persist(outboundMessage);
    ..
    // commit transaction has to be called for this session anytime before this call
    
   
    Similarly you can construct turbo specific messages using
    TurboOutboundMessageUtils.constructOutboundMessage(exchangeName, exchangeType,
                    methodType, httpUri, replyToExchangeName, replyToHttpMethod, replyToHttpUri,
                    payload, groupId, options, appId)

```

#### Things to look out for

- Schema creation will be managed by turbo app entirely, so properly configure it on turbo app side.
- Turbo only support hibernate4.x version, for other hibernate versions you can fork out a branch and contribute the same.
- If you are overriding turbo config using `TurboConfig.setConfig(overridenTurboConfig)`, make you enable autocommit in that config also. For example if you are using hibernate set `hibernate.connection.autocommit` to `true` while providing config.


#### Contributing

1. Fork the branch.
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
