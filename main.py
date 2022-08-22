import kafka_helper
import time

TEST_TOPIC = 'test'

topics = [
    TEST_TOPIC
]


def main():
    kafka_helper.init_topics(topics)

    task_handler = kafka_helper.TaskHandler()

    task_handler.add_task(kafka_helper.Producer(topic=TEST_TOPIC))
    task_handler.add_task(kafka_helper.Consumer(topic=TEST_TOPIC))

    task_handler.run_tasks()
    time.sleep(10)

    task_handler.stop_tasks()


if __name__ == "__main__":
    main()
