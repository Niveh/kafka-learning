import threading
import time
import json
from data import get_registered_user
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
        self._producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER,
                                       value_serializer=lambda m: json.dumps(m).encode('utf-8'))

        while not self.stop_event.is_set():
            self._producer.send(
                topic=self._topic, value=get_registered_user())
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
                print(
                    f"Message Key: {message.key.decode('utf-8') if message.key else None}")
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


def __topic_exists(topic_name):
    """Check if a kafka topic exists.

    Args:
        topic_name (string): Kafka topic name

    Returns:
        bool: True if topic exists, False otherwise
    """
    try:
        client = KafkaClient(bootstrap_servers=KAFKA_SERVER)

        future = client.cluster.request_update()
        client.poll(future=future)

        return topic_name in client.cluster.topics()

    except Exception as ex:
        print(f"Topic check failed.\nException: {ex}")
        return False


def init_topics(topic_list):
    """Create kafka topics if they do not exist.

    Args:
        topic_list (dict): List of Kafka topic info
    """
    topics_to_create = []

    for topic, info in topic_list.items():
        if not __topic_exists(topic):
            new_topic = NewTopic(
                name=topic, num_partitions=info["partitions"], replication_factor=info["replication_factor"])
            topics_to_create.append(new_topic)

    if len(topics_to_create) > 0:
        try:
            admin = KafkaAdminClient(bootstrap_servers=KAFKA_SERVER)
            admin.create_topics(new_topics=topics_to_create,
                                validate_only=False)

            print("Topics created.")

        except Exception as ex:
            print(f"Topic creation failed.\nException: {ex}")

    else:
        print("No topics to create.")
