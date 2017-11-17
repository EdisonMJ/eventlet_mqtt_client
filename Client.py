import eventlet
from Work.Util import CommWork

class Client:

    def __init__(self, evPool=None):
        # 报文标志
        self.pkFlag = None
        self.sock = None
        self.qTimeout = 1.5
        self.closed = False
        if evPool:
            self.evPool = evPool
        else:
            self.evPool = eventlet.greenpool.GreenPool()

    # 连接broker
    def Connect(self, ip, port):
        if not ip:
            return False
        self.pkFlag = [n for n in range(65535)]
        #self.pkFlag = [n for n in range(1200)]
        self.sock = eventlet.green.socket.socket()
        self.sock.connect((ip, port))
        self.connectQr = eventlet.queue.LightQueue()
        self.pubQr = eventlet.queue.LightQueue()
        self.subQr = eventlet.queue.LightQueue()
        self.subMsgQ = eventlet.queue.LightQueue()
        # 固定报头
        ctHead = b'\x10\x0C'
        # 可变报头
        cgHead = b'\x00\x04MQTT\x04\x02\x00\x0A'
        # 有效载荷
        payLoad = b'\x00\x00'
        self.evPool.spawn_n(self.Recv)
        if not CommWork.SockSend(self.sock, ctHead + cgHead + payLoad):
            self.Close()
            return None
        try:
            (recvs, lgCount) = self.connectQr.get(timeout=self.qTimeout)
        except:
            self.Close()
            return False
        if recvs:
            self.closed = False
            self.sock.settimeout(15)
            self.evPool.spawn_n(self.Ping)
            return True
        else:
            self.Close()
            return False

    # 自动从连
    def AutoConnect(self):
        eventlet.spawn_n(self.AutoConnectT)

    def AutoConnectT(self):
        while True:
            if self.closed:
                eventlet.sleep(60)
                if self.Connect():
                    break
                else:
                    eventlet.sleep(60)
            else:
                eventlet.sleep(60)



    # 心跳
    def Ping(self):
        while True:
            if self.closed:
                return True
            eventlet.sleep(10)
            if not CommWork.SockSend(self.sock, bytes([0b11000000, 0b00000000])):
                self.Close()
                return False



    # 断开连接
    def DisConnect(self):
        if self.sock:
            try:
                CommWork.SockSend(self.sock, bytes([0b11100000, 0b00000000]))
            except:
                pass
            if self.sock:
                try:
                    self.sock.shutdown(eventlet.green.socket.SHUT_RDWR)
                except:
                    pass
                try:
                    self.sock.close()
                except:
                    pass
            self.sock = None
            return True
        else:
            return False

    def Close(self):
        if not self.closed:
            self.DisConnect()
            self.closed = True

    def Get(self):
        if self.closed:
            return None
        else:
            return self.subMsgQ.get()

    def Recv(self):
        while True:
            if self.closed:
                return
            recvs = b''
            needRecv = 2
            while 0 != needRecv:
                try:
                    recv = self.sock.recv(needRecv)
                except:
                    recv = None
                if not recv:
                    self.Close()
                    return
                recvs += recv
                needRecv -= len(recv)
            # 过滤心跳
            if 0b11010000 == recvs[0] and 0b00000000 == recvs[1]:
                continue
            needRecv = 2
            while 0 != needRecv:
                try:
                    recv = self.sock.recv(needRecv)
                except:
                    recv = None
                if not recv:
                    self.Close()
                    return
                recvs += recv
                needRecv -= len(recv)
            lg = 0
            lgCount = 0
            for i in range(1, 4):
                if 127 < recvs[i]:
                    lg = lg | ((recvs[i] & 0x7F) << ((i-1) * 7)) 
                    lgCount += 1
                else:
                    lg = lg | ((recvs[i] & 0x7F) << ((i-1) * 7))
                    lgCount += 1
                    break
            needRecv = lg - (3-lgCount)
            while 0 != needRecv:
                try:
                    recv = self.sock.recv(needRecv)
                except:
                    recv = None
                if not recv:
                    self.Close()
                    return
                recvs += recv
                needRecv -= len(recv)
            if 0b00100000 == recvs[0] and 0b00000010 == recvs[1] and 0 == recvs[2] and 0 == recvs[3]:
                self.connectQr.put((recvs, lgCount))
            if recvs[0] in (0b01000000, 0b01010000, 0b01110000) and 0b00000010 == recvs[1]:
                self.pubQr.put((recvs, lgCount))
            if 0b10010000 == recvs[0] or 0b0011 == recvs[0] >> 4:
                self.subQr.put((recvs, lgCount))

    # 发布
    # sock 和broker连接的sock
    # qos  消息质量（0,1,2）
    # topic 路由
    # content 发布的内容
    # retain 保留标志
    def Pub(self, qos, topic, content, retain=False):
        if not self.sock or not topic or not content:
            return False
        if qos not in (0, 1, 2):
            return False
        if self.closed:
            return False
        # 可变报头
        topicL = len(topic)
        pkf = None
        try:
            pkf = self.pkFlag[0]
            self.pkFlag.pop(0)
            pkfs = [pkf >> 8, pkf & 0xFF]
            cgHead = bytes([topicL >> 8, topicL & 0xFF]) + topic.encode('utf-8')
            if 1 <= qos:
                cgHead += bytes(pkfs)
            # 有效载荷
            payLoad = content.encode('utf-8')
            # 固定报头
            ct1 = 0
            if retain:
                ct1 = (0b110 << 3) | (qos << 1) | 1
            else:
                ct1 = (0b110 << 3) | (qos << 1) | 0
            length = len(cgHead) + len(payLoad)
            ctHeadL = [ct1, ]
            while True:
                tlg = length & 0x7F
                length = length >> 7
                if 0 == length:
                    ctHeadL.append(tlg)
                    break
                else:
                    ctHeadL.append(tlg | 0b10000000)
            ctHead = bytes(ctHeadL)
            if not CommWork.SockSend(self.sock, ctHead + cgHead + payLoad):
                self.Close()
                return False
            if 0 == qos:
                return True
            try:
                (recvs, lgCount) = self.pubQr.get(timeout=self.qTimeout)
            except:
                return False
            if recvs:
                if 1 == qos:
                    if 0b01000000 == recvs[0] and 0b00000010 == recvs[1]:
                        if pkfs[0] == recvs[2] and pkfs[1] == recvs[3]:
                            return True
                        else:
                            return False
                    else:
                        return False
                else:
                    if 0b01010000 == recvs[0] and 0b00000010 == recvs[1]:
                        if pkfs[0] == recvs[2] and pkfs[1] == recvs[3]:
                            if not CommWork.SockSend(self.sock, bytes([0b01100010, 0b00000010] + pkfs)):
                                self.Close()
                                return False
                            try:
                                (recvs, lgCount) = self.pubQr.get(timeout=self.qTimeout)
                            except:
                                return False
                            if recvs:
                                if 0b01110000 == recvs[0] and 0b00000010 == recvs[1]:
                                    if pkfs[0] == recvs[2] and pkfs[1] == recvs[3]:
                                        return True
                                else:
                                    return False
                            else:
                                return False
                        else:
                            return False
                    else:
                        return False
            else:
                return False
        finally:
            if pkf:
                self.pkFlag.append(pkf)



    # 订阅---一直等待
    # sock 和broker连接的sock
    # qos  消息质量
    # topic 路由
    def Sub(self, qos, topic):
        self.evPool.spawn_n(self.SubT, qos, topic)


    # 订阅---一直等待
    # sock 和broker连接的sock
    # qos  消息质量
    # topic 路由
    def Sub2(self, qos, topic):
        self.evPool.spawn_n(self.SubT2, qos, topic)


    # 订阅---一直等待
    # sock 和broker连接的sock
    # qos  消息质量
    # topic 路由
    def SubT(self, qos, topic):
        if not self.sock or not topic:
            return
        if self.closed:
            return
        self.subQos = qos
        self.subTopic = topic
        # 可变报头
        pkf = self.pkFlag[0]
        self.pkFlag.pop(0)
        pkfs = [pkf >> 8, pkf & 0xFF]
        cgHead = bytes(pkfs)
        # 有效载荷
        topicL = len(topic)
        payLoad = bytes([topicL >> 8, topicL & 0xFF]) + topic.encode('utf-8') + bytes([qos, ])
        # 固定报头
        length = len(cgHead) + len(payLoad)
        ctHeadL = [0b10000010, ]
        while True:
            tlg = length & 0x7F
            length = length >> 7
            if 0 == length:
                ctHeadL.append(tlg)
                break
            else:
                ctHeadL.append(tlg | 0b10000000)
        ctHead = bytes(ctHeadL)
        if not CommWork.SockSend(self.sock, ctHead + cgHead + payLoad):
            self.pkFlag.append(pkf)
            self.Close()
            return
        while True:
            if self.closed:
                self.pkFlag.append(pkf)
                return
            recvs, lgCount = self.subQr.get()
            if recvs:
                if 0b10010000 == recvs[0]:
                    # 此处待定
                    continue
                    if pkfs[0] == recvs[lgCount + 1] and pkfs[1] == recvs[lgCount + 2]:
                        if 0 == qos:
                            if 0x00 == recvs[lgCount + 3]:
                                try:
                                    (recvs, lgCount) = self.subQr.get(timeout=self.qTimeout)
                                except:
                                    self.subMsgQ.put(None)
                                    continue
                                msg = self.GetSubMsg(recvs, lgCount, topic, qos)
                                self.pkFlag.append(pkf)
                                self.subMsgQ.put(msg)
                            else:
                                self.subMsgQ.put(None)
                        elif 1 == qos:
                            if 0x01 == recvs[lgCount + 3]:
                                try:
                                    (recvs, lgCount) = self.subQr.get(timeout=self.qTimeout)
                                except:
                                    self.subMsgQ.put(None)
                                    continue
                                msg = self.GetSubMsg(recvs, lgCount, topic, qos)
                                self.PubRes(recvs, qos)
                                self.pkFlag.append(pkf)
                                self.subMsgQ.put(msg)
                            else:
                                self.subMsgQ.put(None)
                        elif 2 == qos:
                            if 0x02 == recvs[lgCount + 3]:
                                try:
                                    (recvs, lgCount) = self.subQr.get(timeout=self.qTimeout)
                                except:
                                    self.subMsgQ.put(None)
                                msg = self.GetSubMsg(recvs, lgCount, topic, qos)
                                self.PubRes(recvs, qos)
                                self.pkFlag.append(pkf)
                                self.subMsgQ.put(msg)
                            else:
                                self.subMsgQ.put(None)
                        else:
                            self.subMsgQ.put(None)
                    else:
                        self.subMsgQ.put(None)
                elif 0b0011 == recvs[0] >> 4:
                    qos = recvs[0] & 0b00000110
                    self.pkFlag.append(pkf)
                    if qos not in (0, 1, 2):
                        self.subMsgQ.put(None)
                        continue
                    msg = self.GetSubMsg(recvs, lgCount, topic, qos)
                    if qos in (1, 2):
                        self.PubRes(recvs, qos)
                    self.subMsgQ.put(msg)
                else:
                    self.pkFlag.append(pkf)
                    self.subMsgQ.put(None)
            else:
                self.pkFlag.append(pkf)
                return


    # 订阅---一直等待
    # sock 和broker连接的sock
    # qos  消息质量
    # topic 路由
    def SubT2(self, qos, topic):
        if not self.sock or not topic:
            return
        if self.closed:
            return
        self.subQos = qos
        self.subTopic = topic
        # 可变报头
        pkf = self.pkFlag[0]
        self.pkFlag.pop(0)
        pkfs = [pkf >> 8, pkf & 0xFF]
        cgHead = bytes(pkfs)
        # 有效载荷
        topicL = len(topic)
        payLoad = bytes([topicL >> 8, topicL & 0xFF]) + topic.encode('utf-8') + bytes([qos, ])
        # 固定报头
        length = len(cgHead) + len(payLoad)
        ctHeadL = [0b10000010, ]
        while True:
            tlg = length & 0x7F
            length = length >> 7
            if 0 == length:
                ctHeadL.append(tlg)
                break
            else:
                ctHeadL.append(tlg | 0b10000000)
        ctHead = bytes(ctHeadL)
        if not CommWork.SockSend(self.sock, ctHead + cgHead + payLoad):
            self.pkFlag.append(pkf)
            self.Close()
            return
        while True:
            if self.closed:
                self.pkFlag.append(pkf)
                return
            recvs, lgCount = self.subQr.get()
            if recvs:
                if 0b10010000 == recvs[0]:
                    # 此处待定
                    continue
                    if pkfs[0] == recvs[lgCount + 1] and pkfs[1] == recvs[lgCount + 2]:
                        if 0 == qos:
                            if 0x00 == recvs[lgCount + 3]:
                                try:
                                    (recvs, lgCount) = self.subQr.get(timeout=self.qTimeout)
                                except:
                                    self.subMsgQ.put(None)
                                    continue
                                msg = self.GetSubMsg2(recvs, lgCount, topic, qos)
                                self.pkFlag.append(pkf)
                                self.subMsgQ.put(msg)
                            else:
                                self.subMsgQ.put(None)
                        elif 1 == qos:
                            if 0x01 == recvs[lgCount + 3]:
                                try:
                                    (recvs, lgCount) = self.subQr.get(timeout=self.qTimeout)
                                except:
                                    self.subMsgQ.put(None)
                                    continue
                                msg = self.GetSubMsg2(recvs, lgCount, topic, qos)
                                self.PubRes(recvs, qos)
                                self.pkFlag.append(pkf)
                                self.subMsgQ.put(msg)
                            else:
                                self.subMsgQ.put(None)
                        elif 2 == qos:
                            if 0x02 == recvs[lgCount + 3]:
                                try:
                                    (recvs, lgCount) = self.subQr.get(timeout=self.qTimeout)
                                except:
                                    self.subMsgQ.put(None)
                                msg = self.GetSubMsg2(recvs, lgCount, topic, qos)
                                self.PubRes(recvs, qos)
                                self.pkFlag.append(pkf)
                                self.subMsgQ.put(msg)
                            else:
                                self.subMsgQ.put(None)
                        else:
                            self.subMsgQ.put(None)
                    else:
                        self.subMsgQ.put(None)
                elif 0b0011 == recvs[0] >> 4:
                    qos = recvs[0] & 0b00000110
                    self.pkFlag.append(pkf)
                    if qos not in (0, 1, 2):
                        self.subMsgQ.put(None)
                        continue
                    msg = self.GetSubMsg2(recvs, lgCount, topic, qos)
                    if qos in (1, 2):
                        self.PubRes(recvs, qos)
                    self.subMsgQ.put(msg)
                else:
                    self.pkFlag.append(pkf)
                    self.subMsgQ.put(None)
            else:
                self.pkFlag.append(pkf)
                return



    # 解析sub到的数据
    def GetSubMsg(self, data, lgCount, topic, qos):
        if not data or not topic:
            return None
        topicL = (data[lgCount + 1] << 8) | data[lgCount + 2]
        if topic == data[lgCount + 3 : lgCount + topicL + 3].decode('utf-8')\
                or -1 != topic.find('+')\
                or -1 != topic.find('#'):
            if 1 == qos or 2 == qos:
                return data[lgCount + topicL + 5:].decode('utf-8')
            elif 0 == qos:
                return data[lgCount + topicL + 3:].decode('utf-8')
            else:
                return None
        else:
            return None

    # 解析sub到的数据
    def GetSubMsg2(self, data, lgCount, topic, qos):
        if not data or not topic:
            return None
        topicL = (data[lgCount + 1] << 8) | data[lgCount + 2]
        reTopic = data[lgCount + 3 : lgCount + topicL + 3].decode('utf-8')
        if topic == reTopic\
                or -1 != topic.find('+')\
                or -1 != topic.find('#'):
            if 1 == qos or 2 == qos:
                return (data[lgCount + topicL + 5:].decode('utf-8'), reTopic)
            elif 0 == qos:
                return (data[lgCount + topicL + 3:].decode('utf-8'), reTopic)
            else:
                return None
        else:
            return None



    # pub响应
    def PubRes(self, data, qos):
        if self.sock and data:
            dataL = len(data)
            if 1 == qos:
                sendData = bytes([
                    0b01000000,
                    0b00000010,
                    data[dataL - 2],
                    data[dataL - 1]
                    ])
                if not CommWork.SockSend(self.sock, sendData):
                    self.Close()
                    return False
            elif 2 == qos:
                sendData = bytes([
                    0b01010000,
                    0b00000010,
                    data[dataL - 2],
                    data[dataL - 1]
                    ])
                if not CommWork.SockSend(self.sock, sendData):
                    self.Close()
                    return False
                try:
                    (recvs, lgCount) = self.subQr.get(timeout=self.qTimeout)
                except:
                    pass
                #此处暂不做验证
                return True
            else:
                return False
        else:
            return False





'''
# 测试代码
#r = Connect('127.0.0.1', 1883)
#r1 = Connect('127.0.0.1', 1883)
r = Connect('114.215.131.216', 1883)
r1 = Connect('114.215.131.216', 1883)
evPool = eventlet.greenpool.GreenPool()
evPool.spawn_n(Sub, r, 2, '/test/tt')
eventlet.sleep(0.1)
evPool.spawn_n(Pub, r1, 2, '/test/tt', 'client_test')
evPool.waitall()
DisConnect(r)
DisConnect(r1)
'''
