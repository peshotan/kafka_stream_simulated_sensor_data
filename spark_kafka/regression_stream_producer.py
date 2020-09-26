from kafka import KafkaProducer
import datetime, time, random, threading

rates = [1] # messages per second
sample_sigma = 500
fuzz_sigma = 10


def data_point_gauss(rand):
    """
    Return a random x,y data point near our data line.
    """
    x = rand.gauss(0, sample_sigma)
    y = rand.gauss(0, fuzz_sigma)
    
    # date = datetime.datetime.now()
    print("first point: ",x, "  Second Point: ", y)
    return x, y


def send_at(rate):
    rand = random.Random() # one per thread anyway
    producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093'])
    topic = 'xy-' + str(rate)
    interval = 1/rate
    while True:
        x, y = data_point_gauss(rand)
        msg = '%s %s' % (x, y)
        producer.send(topic, msg.encode('ascii'))
        time.sleep(interval)

        
if __name__ == "__main__":
    for rate in rates:
        server_thread = threading.Thread(target=send_at, args=(rate,))
        server_thread.setDaemon(True)
        server_thread.start()

    while 1:
        time.sleep(1)
