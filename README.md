# eventlet_mqtt_client(eventlet 实现的mqtt client)
# 注意：这一版Client没有实现ClientId自动生成，需要使用标准的支持自动分配ClientId的 Mqtt Broker
+ 示例
+ import Client
+ client = Client()
+ client.Connect('127.0.0.1', 5000)
+ client.Sub(0, '/test')
+ client.Pub(0, '/test', 'test content')
+ result = client.Get()
