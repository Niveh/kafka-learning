import threading
import time
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient, KafkaClient
from kafka.admin import NewTopic

KAFKA_SERVER = "localhost:9092"


class _KafkaThread(threading.Thread):
    """Private Thread class with a stop() method. The thread itself has to check regularly for the stopped() condition.
    """

    def __init__(self):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()


class Producer(_KafkaThread):
    """Kafka Producer class. This class is used to send messages to a Kafka topic.
    """

    def __init__(self, topic):
        _KafkaThread.__init__(self)
        self._producer = None
        self._topic = topic

    def run(self):
        """Run the producer thread."""
        self._producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER)

        while not self.stop_event.is_set():
            self._producer.send(
                topic=self._topic, key=b"test_message", value=b"Hello World!")  # topic, message
            time.sleep(1)

        self._producer.close()


class Consumer(_KafkaThread):
    """Kafka Consumer class. This class is used to receive messages from a Kafka topic.
    """

    def __init__(self, topic):
        _KafkaThread.__init__(self)
        self._consumer = None
        self._topic = topic

    def run(self):
        """Run the consumer thread."""
        self._consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER,
                                       auto_offset_reset='earliest',
                                       consumer_timeout_ms=1000)
        self._consumer.subscribe([self._topic])  # subscribe to topic

        while not self.stop_event.is_set():
            for message in self._consumer:
                print(f"\nMessage Topic: {message.topic}")
                print(f"Message Partition: {message.partition}")
                print(f"Message Offset: {message.offset}")
                print(f"Message Key: {message.key.decode('utf-8')}")
                print(f"Message Value: {message.value.decode('utf-8')}\n")

                if self.stop_event.is_set():
                    break

        self._consumer.close()


class TaskHandler:
    """Task Handler class. This class is used to manage tasks. It can add tasks and start/stop them.
    """

    def __init__(self) -> None:
        self._tasks = []

    def add_task(self, task, run=False):
        self._tasks.append(task)

        if run:
            task.start()

    def run_tasks(self):
        for task in self._tasks:
            task.start()

    def stop_tasks(self):
        for task in self._tasks:
            task.stop()

        for task in self._tasks:
            task.join()


def __topic_exists(topic):
    """Check if a kafka topic exists.

    Args:
        topic (string): Kafka topic name

    Returns:
        bool: True if topic exists, False otherwise
    """
    try:
        client = KafkaClient(bootstrap_servers=KAFKA_SERVER)

        future = client.cluster.request_update()
        client.poll(future=future)

        return topic in client.cluster.topics()

    except Exception as ex:
        print(f"Topic check failed.\nException: {ex}")
        return False


def __create_topic(topic):
    """Create a kafka topic.

    Args:
        topic (string): Kafka topic name
    """
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
        topic = NewTopic(name=topic,
                         num_partitions=1,
                         replication_factor=1)
        admin.create(topic)
        print(f"Topic '{topic}' created.")

    except Exception as ex:
        print(f"Topic creation failed.\nException: {ex}")


def create_topic_if_not_exists(topic):
    """Create a kafka topic if it does not exist.

    Args:
        topic (string): Kafka topic name
    """
    if not __topic_exists(topic):
        print(f"Topic '{topic}' does not exist.\nCreating topic...")
        __create_topic(topic)

    else:
        print(f"Topic '{topic}' exists.")


def init_topics(topic_list):
    for topic in topic_list:
        create_topic_if_not_exists(topic)
