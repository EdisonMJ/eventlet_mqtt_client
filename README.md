# eventlet_mqtt_client(eventlet 实现的mqtt client)
+ 示例
+ import Client
+ client = Client()
+ client.Connect('127.0.0.1', 5000)
+ client.Pub(0, '/test', 'test content')
+ client.Sub(0, '/test')
+ result = client.Get()
