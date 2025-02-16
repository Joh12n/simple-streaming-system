# Stream-Computing-System
简易流计算系统设计
## 配置信息：
jdk 8
spark 3.9版本

## 使用方法：
修改resources/stream-config.properties文件，将其中的地址修改成你自己的kafka地址以及自己的topic名称。
启动main后，往Kafka输入数据，过5分钟会在本地生成一个output.txt文件，里面是词频统计的结果。
