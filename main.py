import time

from yahooquery import Ticker
from confluent_kafka import Producer

p = Producer({
     'bootstrap.servers' : ''
    ,'security.protocol' : 'SASL_SSL'
    
    ,'sasl.mechanisms'   : 'PLAIN'
    ,'sasl.username'     : ''
    ,'sasl.password'     : ''
})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

symbols = ["ITSA4.SA", "PETR4.SA", "WEGE3.SA", "TAEE11.SA"]
tickers = Ticker(symbols)

prices  = tickers.price

message = {"prices" : {}}

for i in range(5):
    print("iteração" + str(i))

    p.poll(0)

    for symbol in symbols:
        data = prices[symbol]
        price = data["regularMarketPrice"]
        
        print("O preço de {0} é: {1}".format(symbol, price))
        message["prices"][symbol] = price
        
    p.produce('TP_PRECOS', str(message).encode("utf-8"), callback=delivery_report)
    time.sleep(1.0)

p.flush()
