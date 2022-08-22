import kafka_helper
import time

TEST_TOPIC = 'two_partitions_test'

topics = {
    TEST_TOPIC: {
        "name": TEST_TOPIC,
        "partitions": 2,
        "replication_factor": 1
    }
}


def main():
    topic_handler = kafka_helper.TopicHandler()
    topic_handler.init_topics(topics)

    task_handler = kafka_helper.TaskHandler()
    task_handler.add_task(kafka_helper.Producer(topic=TEST_TOPIC))
    task_handler.add_task(kafka_helper.Consumer(topic=TEST_TOPIC))

    task_handler.run_tasks()
    time.sleep(10)

    task_handler.stop_tasks()


if __name__ == "__main__":
    main()
