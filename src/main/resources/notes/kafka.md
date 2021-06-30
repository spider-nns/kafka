# kafka

``消息系统、存储系统、流式处理平台``

## 基本概念

```
经典Kafka体系架构包括若干Producer、若干 Broker、若干 Consumer，以及一个 Zookeeper 集群（可插拔）
Zookeeper 负责集群元数据管理、控制器的选举
Producer 将消息发送到 Broker
Broker 负责将收到的消息存储到磁盘中，代理服务
Consumer 负责从 Broker 订阅并消费消息
```

```
Kafka 消息都是以主题为单位进行归类，生产者负责将消息发送到特定的主题(发送到Kafka 集群中的每一条消息都需要指定一个主题)
主题是逻辑上的概念，还可以细分为多个分区，一个分区只属于单个主题，也叫做主题分区(Topic Pariition)

同一个主题下的不同分区包含的消息是不同的，分区在存储层面可以看作是一个可追加的日志文件，消息在追加到分区日志文件的时候都会分配一个特定的偏移量
offset 是消息在分区中的唯一标识，Kafka 通过它来保证消息在分区内的顺序性，不过offset并不跨越分区，所以保证的是分区有序
分区解决的主题的IO，创建主题可以通过指定参数设置分区个数，也可以在主题创建完成修改分区的数量，通过增加分区数量实现水平拓展
```

```
Kakfa 为分区引入了多副本 (Replica)机制，通过增加副本数量可以提升容灾能力
同一分区的不同副本中保存的是相通的消息（在同一时刻，副本之间并非完全一样），副本之间是一主多从的关系
leader 节点负责处理读写请求，follower 副本只负责与 leader 副本的消息同步
副本处于不同的 broker 中，当 leader 副本出现故障时，从 follower 副本中重新选举新的 leader 副本对外提供服务
Kafka 通过多副本机制实现了故障的自动转移，当 Kafka 集群中某个 broker 失效时仍然能够保证服务可用
```

```
Kafka 消费端也具备一定的容灾能力，Consumer 使用拉（Pull）模式从服务端拉取消息，并且保存消费的具体位置，当消费者宕机后恢复上线时可以根据之前保存的消费位置重新拉取需要的消息进行消费,不会造成消息丢失
```

```
AR  分区中的所有副本 Assigned Replicas
ISR 与leader副本保持一定程度同步的副本(包括leader副本在内)组成 In-Sync Replicas
OSR 与leader 副本同步滞后过多的副本组成 Out-of-Sync 
AR=ISR+OSR 正常情况下 OSR=nil
leader 副本负责维护和跟踪ISR集合中所有 follower 副本的滞后状态，当 follower 副本落后太多或者失效时，leader 副本会把他从 ISR 集合中剔除，如果 OSR 集合中有 follower 副本追上了 leader 副本，那么 leader 副本会把他从 OSR 集合转移到 ISR 集合。
默认情况下，只有在 ISR 集合中的副本才有资格被选举为新的 leader，OSR 集合中的副本则没有任何机会(可以修改配置改变)
```

```
ISR 与 HW、LEO
HW (high watermark),高水位，标识了一个特定的消息偏移量，消费者只能拉取到这个 offset 之前的消息，后面消息不可见
LEO(Log End Offset) 标识当前日志文件中下一条代写入消息的 Offset，LEO 大小相当于当前日志分区中最后一条消息的 Offset +1
分区ISR 集合中的每个副本都会维护自身的 LEO，而ISR集合中最小的LEO极为HW,对消费者而言只能消费HW之前的消息。

HW 通过 follower 副本同步完数据保证了数据的一致，即不是完全的同步复制也不是单纯的异步复制
```

## 安装(zk)

```

```

## kafka服务端参数配置

```
zookeeper.connect
broker 要链接的 zookeeper 集群的服务地址(包含端口号)，必填无默认值
chroot 路径 c1:2181,c2:2181/kafka
```

```
listeners
broker 监听客户端连接的地址列表，客户端连接 broker 的入口地址列表
格式： protocol1://hostname0:port0,protocol2://hostname1:port1
protocoal 协议类型，支持类型 PLAINTEXT,SSL,SASL_SSL 
未开启安全认证，使用简单的 PLAINTEXT ，hostname 主机名默认为空，不知定主机名默认绑定默认网卡（127.0.0.1 就可能无法对外提供服务）

advertised.listeners 主要用于 Iaas(Infrastructure as a Service)环境，公网只配置这个
listeners 绑定私网 broker 间通信
```

```
broker.id
指定集群中 broker 唯一标示，默认-1,如果没有设置自动生成一个，和 meta.properties、服务端参数 broker.id.generation.enable 、reserved.broker.max.id 有关
```

```
log.dir log.dirs
kafka 把所有的消息报存在磁盘上，log.dir 配置kafka 日志文件存放的根目录
log.dir 配置单个根目录，log.dirs 用来配置多个根目录(逗号分隔)
log.dirs 优先级高，没有配置 log.dirs 会以 log.dir 配置为准
```

```
message.max.bytes
broker 所能接受消息的最大值，默认值为 1000012(B)，～ 976.6kb
如果 Producer 发送的消息大于这个参数设置的值，Producer 会报出 RecordToolLargeException 异常
修改此参数还要考虑 max.request.size 客户端参数、max.message.bytes(topic 端参数)等参数的影响
为避免修改此参数而引起的级联影响，建议考虑分拆消息的可行性
```

## 消息发送消费

### KafkaProducer

```
KafkaProducer 是线程安全的，send 方法异步发送，消息存储到缓冲区会立即返回
发送的结果是一个RecordMetadata，指定记录被发送到的分区，它被分配的偏移量和记录的时间戳。
producer.send(record).get() 阻塞实现同步发送
get(long timeout,TimeUnit unit) Future 实现超时同步阻塞
异常：
1.NetWorkException
2.LeaderNotAvailiableException 分区leader 副本不可用，发生在副本下线，新leader 副本上线前，需要重试恢复
3.UnknowTopicOrPartitionExceptop
3.NotEnoughReplicasException
4.NotCoordinatorException
5.RecordTooLargeException 不会进行重试，直接抛出异常
properties 配置 ProducerConfig.RETRIES_CONFIG
消息发送模式
1.发后即忘 fire-and-forget
2.同步 sync
3.异步
```

```java
producer.send(record, (metadata, exception) -> {
                //metadata, exception 是互斥的
                if (exception != null) {
                    //do something log...
                    exception.printStackTrace();
                } else {
                    //
                    System.out.println(metadata.topic() + "-" + metadata.partition() + ":" + metadata.offset());
                }
            });
//对于同一个分区，如果消息recored1 先发送，Kafka 可以保证对应的 callback 先调用
producer.send(record1,callBack1)
producer.send(record2,callBack2)
//会阻塞等待之前所有的发送请求完成后在关闭 KafkaProduce
producer.close(10, TimeUnit.SECONDS);//超时关闭
```

### ProducerRecord

```
构建消息，创建 ProducerRecord对象，topic,value 属性是必填项
针对不同消息需要构建不同的 ProducerRecord 对象
 /**
         * public class ProducerRecord<K, V> {
         *     private final String topic; 主题
         *     private final Integer partition; 分区号
         *     private final Headers headers; 消息同步
         *     private final K key; 键 指定消息的键，可以用来计算分区号，可以对消息进行二次归类
         *     private final V value; 值
         *     private final Long timestamp; 消息的时间戳
         *}
         */
同一个 key 的消息会被划分到同一个分区         
```

## 序列化

```
生产者需要用序列化器把对象换成字节数组才能通过网络发送给 Kafka，消费端需要用反序列化器把从 Kafka 中收到的字节数组转换成相应的对象
序列化器 数据类型 String,ByteArray,ByteBuffer,Bytes,Double,Integer,Long
```

```java
    Serializer 接口
		/**
     * Configure this class.
     * @param configs configs in key/value pairs
     * @param isKey whether is for key or value
     */
		void configure(Map<String, ?> configs, boolean isKey);

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data typed data
     * @return serialized bytes
     */
    byte[] serialize(String topic, T data);

    /**
     * Close this serializer.
     * 实现需要保证幂等性
     * This method must be idempotent as it may be called multiple times.
     */
    @Override
    void close();
```

## 分区器 partitioner

```
消息在通过 send 方法发往 broker 过程中，有可能需要经过拦截器 interceptor、序列化器 serializer 、分区器 partitioner 的一系列作用后才能真正被发往 broker，拦截器不必须，序列化器是必须的。
消息经过序列化之后就需要确定他发往的分区，如果消息 ProducerRecord 中指定了 partition 字段，就不需要分区器作用，没有指定就需要依赖分区器，根据 key 计算 partition 值，分区器的作用就是为消息分配分区。
默认分区器 org.apache.kafka.clients.producer.internals.DefaultPartitioner 
```

```java
   implements Paritioner -> implements Configurable ,method  config 获取配置信息、初始化
		/**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes The serialized key to partition on( or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster);

    /**
     * This is called when partitioner is closed.
     */
    public void close();
```

```java
 /**
     * Compute the partition for the given record.
     *
     * @param topic The topic name
     * @param key The key to partition on (or null if no key)
     * @param keyBytes serialized key to partition on (or null if no key)
     * @param value The value to partition on or null
     * @param valueBytes serialized value to partition on or null
     * @param cluster The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (keyBytes == null) {
            int nextValue = nextValue(topic);
            List<PartitionInfo> availablePartitions = cluster.availablePartitionsForTopic(topic);
            // keyBytes 为 null，并且有可用分区，计算得到分区仅为可用分区中的任意一个
            if (availablePartitions.size() > 0) {
                int part = Utils.toPositive(nextValue) % availablePartitions.size();
                return availablePartitions.get(part).partition();
            } else {
                // no partitions are available, give a non-available partition
                return Utils.toPositive(nextValue) % numPartitions;
            }
        } else {
            // hash the keyBytes to choose a partition
            // 序列化后的 key 不为空，使用 murmurhash2 对 keyBytes 进行哈希计算分区号，拥有相同分区号被写入一个分区
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
        }
    }
```

```
不改变主题分区数量情况下，key 与分区之间的映射可以保持不变
一旦主题增加了分区，就难以保证 key 与分区之间的映射关系
实现 Partitioner 接口，可以使得 当 key 不存在是选择非可用的分区 ？？？？？
```

## 拦截器 interceptor

```
生产者、消费者拦截器，可以在发送前做一些准备，eg 按照某个规则过滤不符合要求的消息，修改消息的内容，也可以在发送回调逻辑前做一些定制化
```

### producerInterceptor

```java
implements org.apache.kafka.clients.producer.ProducerInterceptor
这三个方法抛出的异常都会被捕获并且记录到日志中，不会向上传递

  //将消息序列化和计算分区之前进行定制化操作，一般不要修改消息 ProductRecord topic、key、partition
  //修改 key 不仅影响分区，还以影响 broker 端日志压缩
	public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record);
	// 在消息被应答 acknowledgement 之前或消息被发送失败时调用生产者拦截器的 onAcknowledgement() 方法
	// 优先于用户设定的 callBack 执行，方法运行于 producer 的 io 线程中，所以逻辑越简单越好
  public void onAcknowledgement(RecordMetadata metadata, Exception exception);
  public void close();
```

```java
//拦截器,可以多个构成拦截链,如果某个执行失败，下一个拦截器会接着从上一个执行成功的拦截器继续执行
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, CompanyInterceptorPlus.class.getName() +
                "," + CompanyInterceptorPrefix.class.getName());

/**
 * 自定义拦截器
 */
public class CompanyInterceptorPrefix implements ProducerInterceptor<String, String> {

    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        String modifiedValue = "prefix-" + record.value();
        return new ProducerRecord<>(record.topic(),
                record.partition(), record.timestamp(), record.key(), modifiedValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (Objects.isNull(exception)) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendFailure + sendSuccess);
        System.out.println("[INFO] 发送成功率=" + String.format("%f", successRatio * 100) + "%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}

```

